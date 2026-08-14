"""
PubQuiz Arena — FastAPI backend with WebSocket game engine.
"""

import asyncio
import time
import json
import random
import re
import sqlite3
import uuid
from dataclasses import dataclass, field, asdict
from pathlib import Path
from typing import Optional

import qrcode
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, UploadFile, File, HTTPException, Request
from fastapi.responses import HTMLResponse, FileResponse

# ── Paths ──────────────────────────────────────────────────────────────────
BASE_DIR = Path(__file__).parent
QUIZZES_DIR = BASE_DIR / "quizzes"
MEDIA_DIR = BASE_DIR / "media"
DB_PATH = BASE_DIR / "gamenight.db"
FRONTEND_FILE = BASE_DIR / "index.html"
QUIZZES_DIR.mkdir(exist_ok=True)
MEDIA_DIR.mkdir(exist_ok=True)

# ── Database setup ─────────────────────────────────────────────────────────

def get_db():
    """Get a SQLite connection with foreign keys enabled."""
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def init_db():
    """Create the SQLite schema if it doesn't exist."""
    conn = get_db()
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS quizzes (
            name TEXT PRIMARY KEY,
            max_teams INTEGER NOT NULL DEFAULT 4,
            points_per_tick REAL NOT NULL DEFAULT 4.0,
            questions_json TEXT NOT NULL DEFAULT '[]'
        );
    """)
    conn.commit()
    conn.close()


init_db()


def _load_quiz_from_db(name: str) -> Optional[dict]:
    """Load a quiz dict from the SQLite database."""
    conn = get_db()
    cur = conn.execute("SELECT name, max_teams, points_per_tick, questions_json FROM quizzes WHERE name = ?", (name,))
    row = cur.fetchone()
    conn.close()
    if not row:
        return None
    return {"name": row[0], "max_teams": row[1], "points_per_tick": row[2], "questions": json.loads(row[3] or "[]")}


def _save_quiz_to_db(quiz: "Quiz") -> None:
    """Save a quiz dict to the SQLite database."""
    conn = get_db()
    conn.execute(
        "INSERT OR REPLACE INTO quizzes (name, max_teams, points_per_tick, questions_json) VALUES (?, ?, ?, ?)",
        (quiz.name, quiz.max_teams, quiz.points_per_tick, json.dumps(quiz.questions or [])),
    )
    conn.commit()
    conn.close()


def _delete_quiz_from_db(name: str) -> None:
    conn = get_db()
    conn.execute("DELETE FROM quizzes WHERE name = ?", (name,))
    conn.commit()
    conn.close()


def _list_quizzes_from_db() -> list[str]:
    conn = get_db()
    cur = conn.execute("SELECT name FROM quizzes ORDER BY name")
    names = [row[0] for row in cur.fetchall()]
    conn.close()
    return names


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
    max_teams: int = 4  # Kept for DB compatibility but unused
    points_per_tick: float = 4.0
    questions: list = field(default_factory=list)   # list of Question dicts

    def save(self):
        _save_quiz_to_db(self)

    @classmethod
    def load(cls, name: str) -> Optional["Quiz"]:
        data = _load_quiz_from_db(name)
        if not data:
            return None
        return cls(**data)

    @classmethod
    def list_all(cls) -> list[str]:
        return _list_quizzes_from_db()


@dataclass
class TeamInfo:
    socket: WebSocket
    name: str
    answer: Optional[str] = None
    answer_time: float = 0.0   # relative to question start (seconds)
    points: int = 0
    connected_since: Optional[float] = None
    # For reconnection support
    connection_key: Optional[str] = None
    # Bot support
    is_bot: bool = False
    bot_answer_delay: float = 0.0   # seconds to wait before answering
    bot_answer_id: Optional[str] = None
    bot_task: Optional[asyncio.Task] = None  # Running bot task handle


# ── Game session ───────────────────────────────────────────────────────────

class GameSession:
    """Manages a single game session (one quiz, one round at a time)."""

    def __init__(self, quiz: Quiz):
        # Generate 3-digit numeric session code (000-999)
        self.session_id = str(random.randint(0, 999)).zfill(3)
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
        self._timer_started = False            # Track if timer has started
        # Leaderboard history: list of per-question standings snapshots
        self.leaderboard_history: list[dict] = []
        # Track disconnected players for reconnection
        self.disconnected_keys: dict[str, TeamInfo] = {}

    # ── Team management ──────────────────────────────────────────────────

    async def add_team(self, socket: WebSocket, name: str, is_bot: bool = False,
                       bot_delay: float = 0.0, bot_answer: str = None) -> bool:
        """Add a team. Returns True if accepted."""
        # Accept joins even during connecting/question/result/final states
        if self.state not in ("idle", "connecting", "question", "result", "final"):
            await socket.send_json({"type": "error", "message": "No active session"})
            return False
        if name in self.teams:
            await socket.send_json({"type": "error", "message": "Team name already taken"})
            return False
        self.cumulative_scores[name] = 0
        team = TeamInfo(socket=socket, name=name, connected_since=asyncio.get_running_loop().time())
        self.teams[name] = team
        # Notify everyone of the updated team list
        await self.broadcast({
            "type": "teams_updated",
            "teams": [{"name": t.name, "socket_key": k} for k, t in self.teams.items()],
            "count": len(self.teams),
        })
        # Tell a joining player they're in
        await socket.send_json({"type": "joined", "team_name": name, "quiz_name": self.quiz.name})
        return True

    async def add_bot_team(self, name: str, bot_delay: float = None, bot_answer: str = None) -> bool:
        """Add a bot team for testing. Bots answer automatically after a delay."""
        if name in self.teams:
            return False
        self.cumulative_scores[name] = 0
        # Bots don't need a real WebSocket, but we use None and a special handler
        team = TeamInfo(
            socket=None, name=name, connected_since=asyncio.get_running_loop().time(),
            is_bot=True,
            bot_answer_delay=bot_delay or random.uniform(2.0, 8.0),
            bot_answer_id=bot_answer,
        )
        self.teams[name] = team
        await self.broadcast({
            "type": "teams_updated",
            "teams": [{"name": t.name, "socket_key": k} for k, t in self.teams.items()],
            "count": len(self.teams),
            "is_bot": name
        })
        return True

    async def remove_bot_team(self, name: str) -> bool:
        """Remove a bot team."""
        if name not in self.teams:
            return False
        if not self.teams[name].is_bot:
            return False
        if self.teams[name].bot_task:
            self.teams[name].bot_task.cancel()
        del self.teams[name]
        del self.cumulative_scores[name]
        await self.broadcast({
            "type": "teams_updated",
            "teams": [{"name": t.name, "socket_key": k} for k, t in self.teams.items()],
            "count": len(self.teams),
        })
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
        """Wait for all teams to answer, or timeout, or host clicks next."""
        # Get real player teams (exclude host and bots)
        player_teams = {k: v for k, v in self.teams.items() if v.name != "Host" and not v.is_bot}
        total_players = len(player_teams)

        next_click_event = asyncio.Event()

        async def listen_for_next():
            await self._advance_event.wait()
            self._advance_event.clear()
            next_click_event.set()

        next_listener = asyncio.create_task(listen_for_next())

        finish_event = asyncio.Event()

        async def on_timeout():
            await asyncio.sleep(timeout)
            finish_event.set()

        async def on_next():
            await next_click_event.wait()
            finish_event.set()

        timeout_task = asyncio.create_task(on_timeout())
        next_task = asyncio.create_task(on_next())

        # Start bot answer timers for current question
        bot_tasks = []
        for t in self.teams.values():
            if t.is_bot and t.answer is None:
                bot_task = asyncio.create_task(self._bot_answer(t))
                bot_tasks.append(bot_task)
                t.bot_task = bot_task

        # Timer tick task (Improvement 1 & 6)
        async def timer_ticks():
            elapsed = 0
            while not finish_event.is_set() and not self._stop_event.is_set():
                await asyncio.sleep(1)
                elapsed += 1
                remaining = max(0, timeout - elapsed)
                server_time_now = time.time()
                await self.broadcast({
                    "type": "timer_tick",
                    "seconds_remaining": remaining,
                    "total_seconds": timeout,
                    "server_time": server_time_now,
                    "elapsed": round(elapsed, 1)
                })
                if remaining <= 0:
                    break

        timer_task = asyncio.create_task(timer_ticks())

        while not finish_event.is_set() and not self._stop_event.is_set():
            answered = sum(1 for t in player_teams.values() if t.answer)
            if answered != self.answers_collected:
                self.answers_collected = answered
                await self.broadcast({
                    "type": "count_updated",
                    "count": self.answers_collected,
                    "total": total_players
                })
            await asyncio.sleep(0.2)

        timeout_task.cancel()
        next_task.cancel()
        next_listener.cancel()
        timer_task.cancel()
        for bt in bot_tasks:
            bt.cancel()

        self._advance_event.clear()

        if not self.result_sent:
            await self._send_result()

    async def _bot_answer(self, team: TeamInfo):
        try:
            delay = team.bot_answer_delay
            await asyncio.sleep(delay)
            if team.answer is not None:
                return
            if team.bot_answer_id:
                answer = team.bot_answer_id
            else:
                current_q = self.get_current_question()
                if current_q and current_q.get("options"):
                    options = current_q["options"]
                    idx = random.randint(0, len(options) - 1)
                    answer = str(options[idx].get("id", idx)) if isinstance(options[idx], dict) else str(idx)
                else:
                    answer = "a"
            await self.receive_answer(team.name, answer, round(delay, 1))
        except asyncio.CancelledError:
            pass
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

        try:
            while self.current_question_idx < len(q_list):
                self._stop_event.clear()
                self.result_sent = False
                self.answers_collected = 0
                self._timer_started = False
                # Reset answers for all teams
                for t in self.teams.values():
                    t.answer = None
                    t.answer_time = 0.0

                q = q_list[self.current_question_idx]

                # Set wall-clock start time BEFORE sending messages
                self._timer_started = True
                self.question_start_time = time.time()

                # Broadcast question + timer start together
                await self.broadcast({
                    "type": "go_to_question",
                    "question_index": self.current_question_idx,
                    "question": q,
                    "total_questions": len(q_list),
                    "timer_seconds": q["timer_seconds"],
                    "server_time": self.question_start_time
                })
                await self.broadcast({
                    "type": "timer_start",
                    "question_index": self.current_question_idx,
                    "timer_seconds": q["timer_seconds"],
                    "server_time": self.question_start_time
                })

                # Wait for either timeout or all answered
                await self._collect_answers(q["timer_seconds"])

                # Advance to next question or final
                self.current_question_idx += 1

                if self.current_question_idx >= len(q_list):
                    # Final standings
                    await self._send_final()
                    break

                # Wait for host to click "Next" before showing next question
                # (No automatic advance - host must manually click Next)
                await self._advance_event.wait()
                self._advance_event.clear()
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
        count = sum(1 for t in self.teams.values() if t.name != "Host" and t.answer)
        total_players = sum(1 for t in self.teams.values() if t.name != "Host")
        await self.broadcast({
            "type": "count_updated",
            "count": count,
            "total": total_players
        })

    @staticmethod
    def _calc_score(elapsed: float, q_timer: int) -> int:
        """Progressive (curved) 200→50 in first half, then linear 50→0 in second half.
        At 0s: 200, at 50% timer: 50, at 100% timer: 0.
        Answers just before timeout still get 1 point (not 0)."""
        half = max(q_timer * 0.5, 1)
        if elapsed <= half:
            return int(50 + 150 * max(0, 1 - elapsed / half) ** 2)
        else:
            return int(50 * max(0, 1 - (elapsed - half) / half))

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
            try:
                is_correct = False
                if team.answer is not None:
                    qa = q["answer"]
                    # Normalize: quiz answer may be int index or string option id
                    if isinstance(qa, int):
                        # Convert string answer_id to index if possible
                        if isinstance(team.answer, str):
                            try:
                                str_idx = int(team.answer)
                                if str_idx == qa:
                                    is_correct = True
                            except ValueError:
                                pass
                            if not is_correct:
                                for idx, opt in enumerate(q.get("options", [])):
                                    opt_id = opt if isinstance(opt, str) else (opt.get("id", "") if isinstance(opt, dict) else "")
                                    if str(opt_id) == team.answer:
                                        if idx == qa:
                                            is_correct = True
                                        break
                        else:
                            is_correct = int(team.answer) == qa
                    else:
                        is_correct = str(team.answer) == str(qa)
                points = 0
                if is_correct:
                    elapsed = team.answer_time or 0
                    q_timer = max(q.get("timer_seconds", 10), 1)
                    points = GameSession._calc_score(elapsed, q_timer)
                print(f"[SCORE] {name}: answer={team.answer!r} qa={qa!r} correct={is_correct} points={points}", flush=True)
                self.cumulative_scores[name] = self.cumulative_scores.get(name, 0) + points
                results.append({
                    "team": name,
                    "answer": team.answer,
                    "is_correct": is_correct,
                    "points": points,
                    "time_used": round(team.answer_time, 1) if team.answer_time else None
                })
            except Exception as e:
                import traceback
                traceback.print_exc()

        totals = {name: self.cumulative_scores.get(name, 0) for name in player_teams}

        await self.broadcast({
            "type": "result",
            "results": results,
            "correct_answer": q["answer"],
            "options": q["options"]
        })

        # Show leaderboard using cumulative scores
        leaderboard = sorted(totals.items(), key=lambda x: (-x[1], x[0]))
        # Add last question points to each team
        standings_with_points = []
        for name, score in leaderboard:
            last_pts = 0
            for r in results:
                if r['team'] == name:
                    last_pts = r['points']
                    break
            standings_with_points.append({
                "team": name,
                "points": score,
                "last_points": last_pts
            })
        await self.broadcast({
            "type": "leaderboard",
            "standings": standings_with_points,
            "totals": totals,
            "question_index": q_index + 1,
            "total_questions": len(self.quiz.questions),
            "queue_seconds": 8
        })
        # Record leaderboard history (Improvement 8)
        if not self.result_sent:
            self.leaderboard_history.append({
                'question_index': q_index + 1,
                'standings': standings_with_points
            })

    def _get_running_totals(self) -> dict:
        """Get cumulative scores across all questions played."""
        totals: dict[str, int] = {name: 0 for name in self.teams}
        # We'd need to track cumulative through the game. Let's use a dict.
        return totals

    async def _send_final(self):
        self.state = "final"
        await self.broadcast({"type": "final"})

        # Final standings - convert to proper format
        totals = self._get_final_totals()
        standings = sorted(totals.items(), key=lambda x: -x[1])
        standings_formatted = [{"team": name, "points": score} for name, score in standings]

        await self.broadcast({
            "type": "final_standings",
            "standings": standings_formatted,
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
        """Broadcast a message to all unique sockets."""
        data = json.dumps(message)
        seen_sockets = set()
        to_remove = []
        for name, team in self.teams.items():
            if team.socket in seen_sockets:
                continue  # Skip duplicate sockets
            seen_sockets.add(team.socket)
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


@app.get("/favicon.ico")
async def favicon():
    # Return empty to suppress 404 errors
    from fastapi.responses import Response
    return Response(content=b"", media_type="image/x-icon")


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


# LocalTunnel / reverse proxy detection
# Returns the public-facing URL that the browser connected through.
# LocalTunnel injects X-Forwarded-Host header with the public URL.
@app.get("/api/public-url")
async def api_public_url(request: Request):
    forwarded_host = request.headers.get("x-forwarded-host", "")
    if forwarded_host:
        # LocalTunnel sends "host" or "host:port" — keep it as-is
        return {"url": forwarded_host, "detected": True}
    # Fallback: use the standard Host header
    host = request.headers.get("host", "localhost")
    return {"url": host, "detected": False}


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
                # Add host team using "Host" as the key
                session.teams["Host"] = TeamInfo(
                    socket=websocket, name="Host",
                    connected_since=asyncio.get_running_loop().time()
                )
                # Store session using BOTH the URL path (for host) and 3-digit code (for players)
                active_sessions[session_id] = session
                active_sessions[session.session_id] = session

                # Use server_ip from the message (sent by admin UI) for QR generation,
                # falling back to WebSocket client address.
                # This ensures the QR encodes the real local IP even if the admin
                # opened the page via a proxy or unusual network route.
                client = websocket.client
                msg_ip = msg.get("server_ip", "")
                if msg_ip:
                    host_host = msg_ip
                elif client:
                    host_host = client[0]
                else:
                    host_host = "localhost"
                host_port = client[1] if client else 8000

                await websocket.send_json({
                    "type": "session_started",
                    "session_id": session.session_id,
                    "quiz_name": quiz.name,
                    "is_host": True,
                    "server_ip": host_host,
                    "server_ip_warning": host_host in ("localhost", "127.0.0.1", "::1")
                })

                # Use public_url (LocalTunnel) if provided, otherwise use local IP
                public_url = msg.get("public_url", "")
                if public_url:
                    qr_url = f"https://{public_url}/#/player?session={session.session_id}"
                else:
                    qr_url = f"http://{host_host}:{host_port}/#/player?session={session.session_id}"
                is_warning = (not public_url) and (host_host in ("localhost", "127.0.0.1", "::1"))
                await websocket.send_json({
                    "type": "qr_url",
                    "url": qr_url,
                    "server_ip_warning": is_warning,
                    "server_ip": host_host,
                    "public_url": public_url if public_url else None,
                    "teams": [{"name": "Host", "socket_key": "Host"}],
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
                # Generate connection key for reconnection
                conn_key = str(uuid.uuid4())[:12]
                team = session.teams.get(team_name)
                if team:
                    team.connection_key = conn_key
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
                    except Exception:
                        await websocket.send_json({"type": "error", "message": "Failed to submit answer"})

            elif msg.get("type") == "start_timer":
                # Host clicks "Start Timer" — begin countdown for current question
                if session._timer_started:
                    continue  # Timer already started
                session._advance_event.set()

            elif msg.get("type") == "next":
                session._advance_event.set()

            elif msg.get("type") == "leaderboard":
                # Host clicks "Leaderboard" — show current standings using same scoring as _send_result
                q = session.get_current_question()
                if q:
                    q_index = session.current_question_idx
                    results = []
                    player_teams = {k: v for k, v in session.teams.items() if v.name != "Host"}
                    for name, team in player_teams.items():
                        is_correct = False
                        if team.answer is not None:
                            qa = q["answer"]
                            if isinstance(qa, int):
                                if isinstance(team.answer, str):
                                    try:
                                        str_idx = int(team.answer)
                                        if str_idx == qa:
                                            is_correct = True
                                    except ValueError:
                                        pass
                                    if not is_correct:
                                        for idx, opt in enumerate(q.get("options", [])):
                                            if str(opt.get("id", "")) == team.answer:
                                                is_correct = True
                                else:
                                    is_correct = team.answer == qa
                        # Use same normalized scoring as _send_result
                        points = 0
                        if is_correct:
                            elapsed = team.answer_time or 0
                            q_timer = max(q.get("timer_seconds", 20), 1)
                            points = GameSession._calc_score(elapsed, q_timer)
                        results.append({"team": name, "points": points, "is_correct": is_correct, "time_used": round(team.answer_time, 1) if team.answer_time else None})
                    # Sort by points descending
                    results.sort(key=lambda r: (-r["points"], r["team"]))
                    # Add cumulative scores
                    totals = {name: session.cumulative_scores.get(name, 0) for name in player_teams}
                    # Create standings with cumulative scores
                    leaderboard = sorted(totals.items(), key=lambda x: (-x[1], x[0]))
                    standings_with_points = []
                    for name, score in leaderboard:
                        last_pts = 0
                        for r in results:
                            if r['team'] == name:
                                last_pts = r['points']
                                break
                        standings_with_points.append({
                            "team": name,
                            "points": score,
                            "last_points": last_pts
                        })
                    await session.broadcast({
                        "type": "leaderboard",
                        "standings": standings_with_points,
                        "totals": totals,
                        "question_index": q_index + 1,
                        "total_questions": len(session.quiz.questions)
                    })

            elif msg.get("type") == "start2":
                # Host clicks "Start Game" — begins the round loop
                if session.state != "idle" and session.state != "final":
                    continue  # Game already in progress
                asyncio.create_task(session.round_loop(websocket))

            elif msg.get("type") == "end":
                # Immediately go to final standings (winners vs losers screen)
                await session._send_final()
                session._stop_event.set()
                break

            # ── Reconnection (Improvement 7) ──
            elif msg.get("type") == "reconnect":
                conn_key = msg.get("connection_key")
                if not conn_key:
                    await websocket.send_json({"type": "error", "message": "No connection_key"})
                    continue
                team = None
                team_key = None
                if conn_key in session.disconnected_keys:
                    team = session.disconnected_keys[conn_key]
                    team.socket = websocket
                    team.connected_since = asyncio.get_running_loop().time()
                    del session.disconnected_keys[conn_key]
                    for k, t in session.teams.items():
                        if t is team:
                            team_key = k
                            break
                else:
                    for k, t in session.teams.items():
                        if t.connection_key == conn_key and t.name != "Host":
                            team = t
                            team.socket = websocket
                            team.connected_since = asyncio.get_running_loop().time()
                            team_key = k
                            break
                if not team:
                    await websocket.send_json({"type": "error", "message": "Team not found for reconnection"})
                    continue
                await websocket.send_json({"type": "reconnected", "team_name": team.name})
                await websocket.send_json({
                    "type": "session_snapshot",
                    "session_id": session.session_id,
                    "quiz_name": session.quiz.name,
                    "state": session.state,
                    "current_question_index": session.current_question_idx,
                    "teams": [{"name": t.name, "socket_key": k, "is_bot": t.is_bot} for k, t in session.teams.items()],
                    "cumulative_scores": session.cumulative_scores,
                    "leaderboard_history": session.leaderboard_history,
                    "is_host": team.name == "Host"
                })
                if session.state == "question" and session._timer_started:
                    current_q = session.get_current_question()
                    if current_q:
                        elapsed_since_start = time.time() - session.question_start_time
                        remaining = max(0, current_q["timer_seconds"] - elapsed_since_start)
                        await websocket.send_json({
                            "type": "timer_start",
                            "question_index": session.current_question_idx,
                            "timer_seconds": current_q["timer_seconds"],
                            "server_time": session.question_start_time
                        })
                        await websocket.send_json({
                            "type": "timer_tick",
                            "seconds_remaining": int(remaining),
                            "total_seconds": current_q["timer_seconds"],
                            "server_time": session.question_start_time,
                            "elapsed": round(elapsed_since_start, 1)
                        })
                elif session.state in ("result", "final"):
                    await websocket.send_json({"type": "state_sync", "state": session.state})
                session.teams[team_key or team.name].socket = websocket
                continue

            # ── Bot management (Improvement 4) ──
            elif msg.get("type") == "add_bot":
                team_name = msg.get("bot_name", "Bot " + str(len([t for t in session.teams.values() if t.is_bot]) + 1))
                bot_delay = msg.get("bot_delay", random.uniform(2.0, 8.0))
                bot_answer = msg.get("bot_answer")
                bot_correct = msg.get("bot_correct", False)
                if bot_correct:
                    current_q = session.get_current_question()
                    if current_q:
                        bot_answer = str(current_q.get("answer", "a"))
                success = await session.add_bot_team(team_name, bot_delay=bot_delay, bot_answer=bot_answer)
                await websocket.send_json({"type": "bot_added", "success": success, "team_name": team_name})

            elif msg.get("type") == "remove_bot":
                team_name = msg.get("bot_name", "")
                success = await session.remove_bot_team(team_name)
                await websocket.send_json({"type": "bot_removed", "success": success, "team_name": team_name})

            elif msg.get("type") == "get_bot_info":
                bots = [{"name": t.name, "delay": t.bot_answer_delay, "answer": t.bot_answer_id}
                        for t in session.teams.values() if t.is_bot]
                await websocket.send_json({"type": "bot_info", "bots": bots})

            elif msg.get("type") == "get_leaderboard_history":
                await websocket.send_json({
                    "type": "leaderboard_history",
                    "history": session.leaderboard_history
                })

    except WebSocketDisconnect:
        # Cancel heartbeat task
        try:
            heartbeat_task.cancel()
        except Exception:
            pass
        if session:
            # Find the disconnecting team for reconnection
            disconn_team = None
            for name, t in session.teams.items():
                if t.socket == websocket:
                    disconn_team = (name, t)
                    break
            if disconn_team and not disconn_team[1].is_bot:
                name, t = disconn_team
                if t.connection_key:
                    session.disconnected_keys[t.connection_key] = t
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
        traceback.print_exc()


# ── Background session cleanup ──────────────────────────────────────────────

async def _session_cleanup_loop():
    """Remove empty sessions every 5 minutes to free memory on Render."""
    while True:
        await asyncio.sleep(300)  # 5 minutes
        dead_sessions = []
        for sid, session in active_sessions.items():
            # Remove if no teams left (all players disconnected)
            player_teams = [n for n, t in session.teams.items() if t.name != "Host"]
            if not player_teams and len(session.teams) <= 1:
                dead_sessions.append(sid)
        for sid in dead_sessions:
            del active_sessions[sid]


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
