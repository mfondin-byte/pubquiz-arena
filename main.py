"""
PubQuiz Arena — FastAPI backend with WebSocket game engine.
"""

import asyncio
import json
import os
import re
import uuid
from dataclasses import dataclass, field, asdict
from pathlib import Path
from typing import Optional

import qrcode
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, UploadFile, File, HTTPException
from fastapi.responses import HTMLResponse, FileResponse

# ── Paths ──────────────────────────────────────────────────────────────────
BASE_DIR = Path(__file__).parent
QUIZZES_DIR = BASE_DIR / "quizzes"
MEDIA_DIR = BASE_DIR / "media"
FRONTEND_FILE = BASE_DIR / "index.html"
QUIZZES_DIR.mkdir(exist_ok=True)
MEDIA_DIR.mkdir(exist_ok=True)

# ── Data models ────────────────────────────────────────────────────────────

@dataclass
class AnswerOption:
    id: str       # "a", "b", "c", "d"
    text: str

@dataclass
class Question:
    id: int
    question_type: str  # "text", "image", "video", "audio"
    question_text: str
    category: str
    timer_seconds: int
    media_key: Optional[str]
    options: list       # list of AnswerOption dicts
    answer: str

@dataclass
class Quiz:
    name: str
    max_teams: int = 4
    points_per_tick: float = 4.0
    questions: list = field(default_factory=list)   # list of Question dicts

    def save(self):
        QUIZZES_DIR.mkdir(parents=True, exist_ok=True)
        path = QUIZZES_DIR / f"{self.name}.json"
        with open(path, "w") as f:
            json.dump(asdict(self), f, indent=2)

    @classmethod
    def load(cls, name: str) -> Optional["Quiz"]:
        path = QUIZZES_DIR / f"{name}.json"
        if not path.exists():
            return None
        with open(path) as f:
            data = json.load(f)
        return cls(**data)

    @classmethod
    def list_all(cls) -> list[str]:
        files = QUIZZES_DIR.glob("*.json")
        return sorted(f.stem for f in files)


@dataclass
class TeamInfo:
    socket: WebSocket
    name: str
    answer: Optional[str] = None
    answer_time: float = 0.0   # relative to question start (seconds)
    points: int = 0
    connected_since: Optional[float] = None


# ── Game session ───────────────────────────────────────────────────────────

class GameSession:
    """Manages a single game session (one quiz, one round at a time)."""

    def __init__(self, quiz: Quiz):
        self.session_id = str(uuid.uuid4())[:8]
        self.quiz = quiz
        self.state: str = "idle"   # idle | connecting | question | result | final
        self.teams: dict[str, TeamInfo] = {}
        self.current_question_idx = 0
        self.question_start_time: float = 0
        self.timer_interval: asyncio.TimerHandle = None
        self.answers_collected: int = 0
        self.result_sent = False
        self.cumulative_scores: dict[str, int] = {}
        self._lock = asyncio.Lock()
        self._advance_event = asyncio.Event()  # Signal to go to next question
        self._stop_event = asyncio.Event()     # Signal to stop the game

    # ── Team management ──────────────────────────────────────────────────

    async def add_team(self, socket: WebSocket, name: str) -> bool:
        """Add a team. Returns True if accepted."""
        # Accept joins even during connecting/question/result/final states
        if self.state not in ("idle", "connecting", "question", "result", "final"):
            await socket.send_json({"type": "error", "message": "No active session"})
            return False
        if name in self.teams:
            await socket.send_json({"type": "error", "message": "Team name already taken"})
            return False
        if len(self.teams) >= self.quiz.max_teams:
            await socket.send_json({"type": "error", "message": "Max teams reached"})
            return False
        self.cumulative_scores[name] = 0
        team = TeamInfo(socket=socket, name=name, connected_since=asyncio.get_running_loop().time())
        self.teams[name] = team
        # Notify everyone of the updated team list
        await self.broadcast({
            "type": "teams_updated",
            "teams": list(self.teams.keys()),
            "count": len(self.teams),
            "max": self.quiz.max_teams
        })
        # Tell a joining player they're in
        await socket.send_json({"type": "joined", "team_name": name})
        return True

    # ── Game play ──────────────────────────────────────────────────────────

    async def start_game(self, host_socket: WebSocket) -> bool:
        if self.state != "idle" or not self.quiz.questions:
            return False
        if len(self.teams) == 0:
            return False
        self.state = "connecting"
        await self.broadcast({"type": "game_starting"})
        # After 2 seconds, auto-proceed so host can start first question
        await asyncio.sleep(2)
        # Go to question
        await self.broadcast({"type": "go_to_question"})
        return True

    def get_current_question(self) -> Optional[dict]:
        if self.current_question_idx >= len(self.quiz.questions):
            return None
        q = self.quiz.questions[self.current_question_idx]
        return q

    async def _collect_answers(self, timeout: int):
        """Wait for all teams to answer, or timeout.
        
        Timer is broadcast every 0.5s to reduce WebSocket flood.
        """
        start = asyncio.get_running_loop().time()
        remaining = timeout

        # Get real player teams (exclude host)
        player_teams = {k: v for k, v in self.teams.items() if v.name != "Host"}
        total_players = len(player_teams)

        while remaining > 0:
            await asyncio.sleep(0.5)  # Reduced from 0.1s to reduce flood
            elapsed = asyncio.get_running_loop().time() - self.question_start_time
            remaining = max(0, int(timeout - elapsed))
            await self.broadcast({"type": "timer", "seconds": remaining})

            # Check if all players answered
            answered = sum(1 for t in player_teams.values() if t.answer)
            if answered != self.answers_collected:
                self.answers_collected = answered
                await self.broadcast({
                    "type": "count_updated",
                    "count": self.answers_collected,
                    "total": total_players
                })

            # Exit early if all players answered and result was already sent
            if self.answers_collected >= total_players and self.result_sent:
                break

        # Send result if not already sent (from receive_answer or timeout)
        if not self.result_sent:
            await self._send_result()

    async def receive_answer(self, team_name: str, answer_id, elapsed: float):
        async with self._lock:
            # Ignore answers from host or non-existent teams
            if team_name not in self.teams:
                return
            if self.teams[team_name].name == "Host":
                return
            if self.teams[team_name].answer is not None:
                return  # already answered
            # Guard against None answer_id
            if answer_id is None:
                return
            self.teams[team_name].answer = str(answer_id)
            self.teams[team_name].answer_time = round(elapsed, 1)
            self.answers_collected += 1
            # Check if all players answered
            player_teams = {k: v for k, v in self.teams.items() if v.name != "Host"}
            answered_count = sum(1 for t in player_teams.values() if t.answer)
            if answered_count == len(player_teams):
                await self._send_result()

    async def round_loop(self, host_socket: WebSocket):
        """Main game loop: iterate through questions."""
        q_list = self.quiz.questions
        if not q_list:
            await self.broadcast({"type": "error", "message": "No questions in quiz"})
            self.state = "idle"
            return

        self.state = "question"
        self.current_question_idx = 0
        self.question_start_time = asyncio.get_running_loop().time()

        # Broadcast go_to_question to kick off the first question
        await self.broadcast({
            "type": "go_to_question",
            "question_index": self.current_question_idx,
            "question": q_list[self.current_question_idx],
            "total_questions": len(q_list),
            "timer_seconds": q_list[self.current_question_idx]["timer_seconds"]
        })

        try:
            while self.current_question_idx < len(q_list):
                self._advance_event.clear()
                self._stop_event.clear()
                self.result_sent = False
                self.answers_collected = 0
                # Reset answers for all teams
                for t in self.teams.values():
                    t.answer = None
                    t.answer_time = 0.0

                q = q_list[self.current_question_idx]
                self.question_start_time = asyncio.get_running_loop().time()

                # Wait for either timeout or all answered
                await self._collect_answers(q["timer_seconds"])

                # Advance to next question or final
                self.current_question_idx += 1

                if self.current_question_idx >= len(q_list):
                    # Final standings
                    await self._send_final()
                    break

                # Wait for host to click "Next"
                await self._advance_event.wait()

                # Broadcast next question
                next_q = q_list[self.current_question_idx]
                await self.broadcast({
                    "type": "go_to_question",
                    "question_index": self.current_question_idx,
                    "question": next_q,
                    "total_questions": len(q_list),
                    "timer_seconds": next_q["timer_seconds"]
                })

        except asyncio.CancelledError:
            pass
        except Exception:
            # Catch any unexpected errors to prevent silent loop termination
            import traceback
            traceback.print_exc()
        finally:
            self.state = "final"

    async def _count_answers(self):
        """Count how many teams have answered."""
        count = sum(1 for t in self.teams.values() if t.answer)
        await self.broadcast({
            "type": "count_updated",
            "count": count,
            "total": len(self.teams)
        })

    async def _send_result(self):
        if self.result_sent:
            return
        self.result_sent = True
        q = self.get_current_question()
        if not q:
            return

        # Calculate scores and update cumulative
        results = []
        q_index = self.current_question_idx
        player_teams = {k: v for k, v in self.teams.items() if v.name != "Host"}
        for name, team in player_teams.items():
            is_correct = False
            if team.answer is not None:
                qa = q["answer"]
                # Normalize: quiz answer may be int index or string option id
                if isinstance(qa, int):
                    # Convert string answer_id to index if possible
                    if isinstance(team.answer, str):
                        # Check if team.answer is a string index ("0", "1", etc.)
                        try:
                            str_idx = int(team.answer)
                            if str_idx == qa:
                                is_correct = True
                        except ValueError:
                            pass
                        # Also check if it's an option ID letter
                        if not is_correct:
                            for idx, opt in enumerate(q.get("options", [])):
                                if str(opt.get("id", "")) == team.answer:
                                    if idx == qa:
                                        is_correct = True
                                    break
                    else:
                        is_correct = int(team.answer) == qa
                else:
                    # qa is a string (option id)
                    is_correct = str(team.answer) == str(qa)
            points = 0
            if is_correct:
                elapsed = team.answer_time or 0
                remaining = max(0, q["timer_seconds"] - elapsed)
                points = int(self.quiz.points_per_tick * remaining)
            self.cumulative_scores[name] = self.cumulative_scores.get(name, 0) + points
            results.append({
                "team": name,
                "answer": team.answer,
                "is_correct": is_correct,
                "points": points,
                "time_used": round(team.answer_time, 1) if team.answer_time else None
            })

        totals = {name: self.cumulative_scores.get(name, 0) for name in player_teams}

        await self.broadcast({
            "type": "result",
            "results": results,
            "correct_answer": q["answer"],
            "options": q["options"]
        })

        # Show leaderboard
        leaderboard = sorted(results, key=lambda r: (-r["points"], r["team"]))
        await self.broadcast({
            "type": "leaderboard",
            "standings": leaderboard,
            "totals": totals,
            "question_index": q_index + 1,
            "total_questions": len(self.quiz.questions),
            "queue_seconds": 8
        })

    def _get_running_totals(self) -> dict:
        """Get cumulative scores across all questions played."""
        totals: dict[str, int] = {name: 0 for name in self.teams}
        # We'd need to track cumulative through the game. Let's use a dict.
        return totals

    async def _send_final(self):
        self.state = "final"
        await self.broadcast({"type": "final"})

        # Final standings
        totals = self._get_final_totals()
        standings = sorted(totals.items(), key=lambda x: -x[1])

        await self.broadcast({
            "type": "final_standings",
            "standings": standings,
            "session_id": self.session_id
        })

    def _get_final_totals(self) -> dict[str, int]:
        totals: dict[str, int] = {}
        for name, team in self.teams.items():
            if team.name == "Host":
                continue
            totals[name] = self.cumulative_scores.get(name, 0)
        return totals

    # ── Broadcasting ──────────────────────────────────────────────────────

    async def broadcast(self, message: dict):
        data = json.dumps(message)
        to_remove = []
        for name, team in self.teams.items():
            try:
                await team.socket.send_text(data)
            except Exception:
                to_remove.append(name)
        for name in to_remove:
            del self.teams[name]


# ── Quiz file manager ─────────────────────────────────────────────────────

class QuizManager:
    def create_quiz(self, name: str, max_teams: int = 4, points_per_tick: float = 4.0) -> Quiz:
        q = Quiz(name=name, max_teams=max_teams, points_per_tick=points_per_tick, questions=[])
        q.save()
        return q

    def list_quizzes(self) -> list[str]:
        return Quiz.list_all()

    def get_quiz(self, name: str) -> Optional[dict]:
        q = Quiz.load(name)
        if not q:
            return None
        return asdict(q)

    def update_quiz(self, name: str, data: dict) -> Quiz:
        quiz = Quiz.load(name)
        if not quiz:
            raise HTTPException(status_code=404, detail="Quiz not found")
        # Update fields
        if "max_teams" in data:
            quiz.max_teams = data["max_teams"]
        if "points_per_tick" in data:
            quiz.points_per_tick = data["points_per_tick"]
        quiz.save()
        return quiz

    def add_question(self, quiz_name: str, question_data: dict) -> Question:
        quiz = Quiz.load(quiz_name)
        if not quiz:
            raise HTTPException(status_code=404, detail="Quiz not found")
        # Ensure options are AnswerOption dicts (id + text)
        raw_options = question_data.get("options", [])
        options = []
        for i, opt in enumerate(raw_options):
            if isinstance(opt, dict):
                options.append({"id": opt.get("id", ["a","b","c","d"][i]), "text": opt.get("text", opt)})
            else:
                options.append({"id": ["a","b","c","d"][i], "text": opt})
        # Ensure answer is an int index
        raw_answer = question_data.get("answer", 0)
        if isinstance(raw_answer, str):
            raw_answer = int(raw_answer)
        elif isinstance(raw_answer, float):
            raw_answer = int(raw_answer)
        q = Question(
            id=len(quiz.questions) + 1,
            question_type=question_data["question_type"],
            question_text=question_data["question_text"],
            category=question_data.get("category", ""),
            timer_seconds=question_data.get("timer_seconds", 15),
            media_key=question_data.get("media_key"),
            options=options,
            answer=raw_answer
        )
        quiz.questions.append(asdict(q))
        quiz.save()
        return q

    def update_question(self, quiz_name: str, question_id: int, question_data: dict) -> Question:
        quiz = Quiz.load(quiz_name)
        if not quiz:
            raise HTTPException(status_code=404, detail="Quiz not found")
        # Find the question by id
        for i, q in enumerate(quiz.questions):
            if q.get("id") == question_id:
                # Ensure options are AnswerOption dicts
                raw_options = question_data.get("options", [])
                options = []
                for j, opt in enumerate(raw_options):
                    if isinstance(opt, dict):
                        options.append({"id": opt.get("id", ["a","b","c","d"][j]), "text": opt.get("text", opt)})
                    else:
                        options.append({"id": ["a","b","c","d"][j], "text": opt})
                raw_answer = question_data.get("answer", 0)
                if isinstance(raw_answer, str):
                    raw_answer = int(raw_answer)
                elif isinstance(raw_answer, float):
                    raw_answer = int(raw_answer)
                question_id = question_data.get("id", q.get("id", question_id))
                updated = Question(
                    id=question_id,
                    question_type=question_data.get("question_type", q.get("question_type", "text")),
                    question_text=question_data.get("question_text", q.get("question_text", "")),
                    category=question_data.get("category", q.get("category", "")),
                    timer_seconds=question_data.get("timer_seconds", q.get("timer_seconds", 15)),
                    media_key=question_data.get("media_key", q.get("media_key")),
                    options=options,
                    answer=raw_answer
                )
                quiz.questions[i] = asdict(updated)
                quiz.save()
                return updated
        raise HTTPException(status_code=404, detail="Question not found")

    def delete_question(self, quiz_name: str, question_id: int):
        quiz = Quiz.load(quiz_name)
        if not quiz:
            raise HTTPException(status_code=404, detail="Quiz not found")
        quiz.questions = [q for q in quiz.questions if q["id"] != question_id]
        # Re-index
        for i, q in enumerate(quiz.questions):
            q["id"] = i + 1
        quiz.save()

    def reorder_questions(self, quiz_name: str, order: list[int]):
        """Reorder questions by their IDs per the new order."""
        quiz = Quiz.load(quiz_name)
        if not quiz:
            raise HTTPException(status_code=404, detail="Quiz not found")
        id_to_q = {q["id"]: q for q in quiz.questions}
        new_order = [id_to_q[iid] for iid in order if iid in id_to_q]
        for i, q in enumerate(new_order):
            q["id"] = i + 1
        quiz.questions = new_order
        quiz.save()

    def delete_quiz(self, name: str):
        path = QUIZZES_DIR / f"{name}.json"
        if path.exists():
            path.unlink()

    def upload_media(self, quiz_name: str, file: UploadFile) -> str:
        quiz_media_dir = MEDIA_DIR / quiz_name
        quiz_media_dir.mkdir(parents=True, exist_ok=True)
        original = file.filename or "upload"
        # Sanitise
        safe_name = re.sub(r'[^a-zA-Z0-9_.\-]', '_', original)
        dest = quiz_media_dir / safe_name
        content = file.file.read()
        dest.write_bytes(content)
        return f"/api/media/{quiz_name}/{safe_name}"

    def generate_qr_code(self, url: str) -> bytes:
        qr = qrcode.make(url)
        import io
        buf = io.BytesIO()
        qr.save(buf, format="PNG")
        buf.seek(0)
        return buf.read()


# ── App setup ─────────────────────────────────────────────────────────────

app = FastAPI(title="PubQuiz Arena")
quiz_manager = QuizManager()
active_sessions: dict[str, GameSession] = {}


# Serve frontend
@app.get("/", response_class=HTMLResponse)
async def serve_index():
    if FRONTEND_FILE.exists():
        return FRONTEND_FILE.read_text()
    return "<h1>PubQuiz Arena</h1><p>index.html not found.</p>"


# Quiz CRUD routes
@app.get("/api/quizzes")
async def api_list_quizzes():
    names = quiz_manager.list_quizzes()
    result = []
    for name in names:
        q = quiz_manager.get_quiz(name)
        if q:
            result.append({
                "name": q["name"],
                "question_count": len(q["questions"]) if q["questions"] else 0,
                "max_teams": q["max_teams"],
                "points_per_tick": q["points_per_tick"]
            })
    return {"quizzes": result}


@app.get("/api/quizzes/{name}")
async def api_get_quiz(name: str):
    quiz = quiz_manager.get_quiz(name)
    if not quiz:
        raise HTTPException(status_code=404, detail="Quiz not found")
    return quiz


@app.post("/api/quizzes")
async def api_create_quiz(data: dict):
    name = data.get("name", "").strip()
    if not name:
        raise HTTPException(status_code=400, detail="Name required")
    max_teams = data.get("max_teams", 4)
    points_per_tick = data.get("points_per_tick", 4.0)
    try:
        q = quiz_manager.create_quiz(name, max_teams, points_per_tick)
        return {"quiz": asdict(q)}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.put("/api/quizzes/{name}")
async def api_update_quiz(name: str, data: dict):
    quiz = quiz_manager.update_quiz(name, data)
    return {"quiz": asdict(quiz)}


@app.delete("/api/quizzes/{name}")
async def api_delete_quiz(name: str):
    quiz_manager.delete_quiz(name)
    return {"ok": True}


@app.post("/api/quizzes/{name}/questions")
async def api_add_question(name: str, data: dict):
    q = quiz_manager.add_question(name, data)
    return {"question": asdict(q)}


@app.delete("/api/quizzes/{name}/questions/{question_id}")
async def api_delete_question(name: str, question_id: int):
    quiz_manager.delete_question(name, question_id)
    return {"ok": True}


@app.get("/api/quizzes/{name}/questions")
async def api_get_questions(name: str):
    quiz = quiz_manager.get_quiz(name)
    if not quiz:
        raise HTTPException(status_code=404, detail="Quiz not found")
    return quiz.get("questions", [])


@app.put("/api/quizzes/{name}/questions/{question_id}")
async def api_update_question(name: str, question_id: int, data: dict):
    q = quiz_manager.update_question(name, question_id, data)
    return {"question": asdict(q)}


@app.patch("/api/quizzes/{name}/reorder")
async def api_reorder_questions(name: str, data: dict):
    order = data.get("question_ids", [])
    if not order:
        order = data.get("order", [])
    quiz_manager.reorder_questions(name, order)
    return {"ok": True}


# Media upload
@app.post("/api/quizzes/{quiz_name}/media/upload")
async def api_upload_media(quiz_name: str, file: UploadFile = File(...)):
    path = quiz_manager.upload_media(quiz_name, file)
    original_name = file.filename or "upload"
    safe_name = re.sub(r'[^a-zA-Z0-9_.\-]', '_', original_name)
    return {"path": path, "media_key": safe_name}


# QR code
@app.get("/api/qr")
async def api_qr(url: str):
    data = quiz_manager.generate_qr_code(url)
    from fastapi.responses import Response
    return Response(content=data, media_type="image/png")


# Media serving (proxy / serve from disk)
@app.get("/api/media/{quiz_name}/{filename}")
async def api_media(quiz_name: str, filename: str):
    path = MEDIA_DIR / quiz_name / filename
    if not path.exists():
        raise HTTPException(status_code=404, detail="Media not found")
    return FileResponse(path)


# WebSocket game endpoint (must come before /api/ routes)
@app.websocket("/ws/{session_id}")
async def websocket_game(websocket: WebSocket, session_id: str):
    await websocket.accept()

    session = None
    quiz = None

    # Heartbeat task to keep WebSocket alive through Render's proxy layer
    # Render's proxy terminates idle WebSocket connections after ~60s
    async def heartbeat():
        while True:
            await asyncio.sleep(20)  # Ping every 20s
            try:
                await websocket.ping()
            except Exception:
                break

    heartbeat_task = asyncio.create_task(heartbeat())

    try:
        while True:
            data = await websocket.receive_text()
            msg = json.loads(data)

            # --- START command: first connection creates the session ---
            if msg.get("type") == "start":
                quiz_name = msg.get("quiz_name", "default")
                quiz = Quiz.load(quiz_name)
                if not quiz:
                    await websocket.send_json({"type": "error", "message": f"Quiz '{quiz_name}' not found"})
                    continue

                session = GameSession(quiz)
                session.teams[session_id] = TeamInfo(
                    socket=websocket, name="Host",
                    connected_since=asyncio.get_running_loop().time()
                )
                active_sessions[session_id] = session

                await websocket.send_json({
                    "type": "session_started",
                    "session_id": session.session_id,
                    "quiz_name": quiz.name,
                    "is_host": True
                })

                client = websocket.client
                host_host = client[0] if client else "localhost"
                host_port = client[1] if client else 8000
                qr_url = f"http://{host_host}:{host_port}/#/player?session={session.session_id}"
                await websocket.send_json({
                    "type": "qr_url",
                    "url": qr_url,
                    "teams": ["Host"],
                    "count": 1
                })
                # Do NOT auto-start round_loop. Host must click "Start Game" (send "start2" command).
                continue

            # --- Other commands (only valid if session exists) ---
            session = active_sessions.get(session_id)
            if session is None:
                await websocket.send_json({"type": "error", "message": "No active game session"})
                continue

            if msg.get("type") == "join":
                team_name = msg.get("team_name", "Player")
                await session.add_team(websocket, team_name)

            elif msg.get("type") == "answer":
                # Find team by socket, skip the host
                team_name = None
                for name, t in session.teams.items():
                    if t.socket == websocket and t.name != "Host":
                        team_name = name
                        break
                if team_name:
                    elapsed = msg.get("elapsed", 0)
                    try:
                        await session.receive_answer(team_name, msg.get("answer_id"), elapsed)
                    except Exception as e:
                        print(f"Answer error for {team_name}: {e}")
                        await websocket.send_json({"type": "error", "message": f"Failed to submit answer: {e}"})

            elif msg.get("type") == "next":
                session._advance_event.set()

            elif msg.get("type") == "start2":
                # Host clicks "Start Game" — begins the round loop
                if session.state != "idle" and session.state != "final":
                    continue  # Game already in progress
                asyncio.create_task(session.round_loop(websocket))

            elif msg.get("type") == "end":
                if not session.result_sent:
                    await session._send_result()
                session._stop_event.set()
                break

    except WebSocketDisconnect:
        # Cancel heartbeat task
        try:
            heartbeat_task.cancel()
        except Exception:
            pass
        if session:
            session.teams = {
                name: t for name, t in session.teams.items()
                if t.socket != websocket
            }
            try:
                await session.broadcast({
                    "type": "team_disconnected",
                    "count": len(session.teams)
                })
            except Exception:
                pass
    except Exception as e:
        # Cancel heartbeat task
        try:
            heartbeat_task.cancel()
        except Exception:
            pass
        import traceback
        print(f"WebSocket error: {e}")
        traceback.print_exc()


# ── Background session cleanup ──────────────────────────────────────────────

async def _session_cleanup_loop():
    """Remove empty sessions every 60s to free memory on Render."""
    while True:
        await asyncio.sleep(60)
        dead_sessions = []
        for sid, session in active_sessions.items():
            # Remove if no teams left (all players disconnected)
            player_teams = [n for n, t in session.teams.items() if t.name != "Host"]
            if not player_teams and len(session.teams) <= 1:
                dead_sessions.append(sid)
        for sid in dead_sessions:
            del active_sessions[sid]
            print(f"Cleaned up empty session: {sid}")


@app.on_event("startup")
async def start_session_cleanup():
    """Launch the cleanup loop as a background task so it doesn't block startup."""
    asyncio.create_task(_session_cleanup_loop())


@app.on_event("shutdown")
async def shutdown():
    """Close all WebSocket connections on shutdown."""
    for sid, session in active_sessions.items():
        for team in session.teams.values():
            try:
                await team.socket.close()
            except Exception:
                pass
