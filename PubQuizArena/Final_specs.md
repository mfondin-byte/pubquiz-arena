🏆 FINAL SPEC — PubQuiz Arena
1. Concept
Kahoot-style live pub quiz for N teams (configurable per quiz) of 1 representative each. Host projects questions; representatives answer on phones via a shared local URL. After every question a leaderboard updates. The final screen bumps all three with a gold/silver/bronze podium. Massive confetti.

No AI during gameplay. AI only helps build the app.

2. Architecture & Tech Stack
Layer    Tech
Backend    Python 3.12+, FastAPI, WebSockets
Frontend    Vanilla HTML/CSS/JS (one index.html, no build tools)
Real-time    WebSockets for live answer submission, wall-clock countdown broadcast, state sync
Persistence    JSON quiz files on disk, media in ./media/<quiz-slug>/
Confetti    Canvas-based particle confetti (end-screen)
Access    http://<mac-ip>:8000 — same local WiFi
Three sub-UIs in one page, routed by hash:

/#admin — Quiz builder & management
/#host — Projected host screen
/#player — Mobile player interface
3. Admin UI — Full Quiz Builder
3.1 Quiz Management
Create a new quiz → give it a name, set:
max_teams (how many teams can join, e.g. 3–6)
points_per_tick (default 4)
Save → writes to ./quizzes/<name>.json
Load a saved quiz → loads existing quiz
Delete a quiz → removes file
Share QR: generates a QR code image of the join URL → displays on screen; players scan
3.2 Question Builder (per question)
Drag-and-drop reorder + up/down buttons
Fields:
Question text (required)
Category (free text, e.g. "Music", "Sport")
Timer (seconds, default 15, range 5–60)
Question type: text / image / video / audio
Media file → drag-drop or file picker; saved to ./media/<quiz-slug>/<file>
4 answer options with radio for correct answer
3.3 Preview & Start
Preview mode: replays a question as it would appear on host + player screens
"Start Game" button → opens a new /#host session with live WebSocket per session.
Once started, questions are frozen (no edits).
3.4 Host Screen (Big Projected Display)
[QUESTION SCREEN]
   Category label | Media (image/video/audio) | Question text
   ───────────────────────────────────────────────
   A. Answer 1     B. Answer 2
   C. Answer 3     D. Answer 4
   [Animated countdown bar: 15s → 0s]
   [Counter: 1/3 teams connected → 2/3 → 3/3]

[RESULT SCREEN]
   ✓ Team Alpha: 36 pts (answered at 9.0s)
   ✗ Team Beta: 0 pts (wrong)
   ⏰ Team Gamma: 0 pts (timeout)
   [Leaderboard / ranking]
   [NEXT QUESTION button]
Full state machine:

Idle → "Select quiz → Start Game"
Connecting → Waiting for N teams to connect
Question → concurrent answer collection + live timer
Reveal → all answers + scores shown
Leaderboard → ranking update
Final → podium + confetti
Host advances manually ("Next Question" or "Play Again").

4. Player UI (Mobile)
States:

Join → enter team name → join screen (e.g. "Team Shark connected! Waiting for host...")
Waiting → host starts
Question → media plays at top, options below, animated timer bar
Result → ✓/✗, points, time remaining
Final → "The game is over!"
Join works by scanning QR → opens http://<mac-ip>:8000/#player?session=abc

5. Scoring & Timing
Formula: points = points_per_tick × seconds_remaining at submission time
Wrong answer = 0 points
Timeout = 0 points
Only correct answers score points
Callendar: broadcasted from host (wall-clock), not client-side
Timer per question: configurable (5–60 s)
6. Game Flow
Available quiz at host: 20–30 questions
End of last question → Final podium screen
Podium: Gold, Silver, Bronze top 3
Confetti: canvas-based particle system, multi-color, full-screen, ~6 s loop
"Play Again" → replay same quiz
"New Quiz" → back to admin
7. Data Model
Quiz file (quizzes/<name>.json):

{
  "name": "Triv Thurs",
  "max_teams": 3,
  "points_per_tick": 4,
  "questions": [
    {
      "id": 1,
      "type": "audio",
      "text": "Who sang this song?",
      "category": "Music",
      "timer_seconds": 15,
      "media_file": "media/triv-thurs/coldplay.mp3",
      "options": [
        {"id": "a", "text": "Coldplay"},
        {"id": "b", "text": "U2"},
        {"id": "c", "text": "Radiohead"},
        {"id": "d", "text": "Oasis"}
      ],
      "correct_answer": "a"
    }
  ]
}
