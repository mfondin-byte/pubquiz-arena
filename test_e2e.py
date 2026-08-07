#!/usr/bin/env python3
"""Comprehensive e2e test: quiz CRUD + full game flow with separate WS per role."""
import asyncio, json, http.client, subprocess, sys, os, websockets

HOST = "127.0.0.1"
PORT = 10100

def _api(method, path, body=None):
    conn = http.client.HTTPConnection(HOST, PORT)
    hdr = {'Content-Type':'application/json'} if body else {}
    conn.request(method, path, json.dumps(body).encode() if body else None, hdr)
    r = conn.getresponse(); d = r.read().decode(); conn.close()
    return r.status, json.loads(d) if d else {}

def _print(title, ok, detail=""):
    status = "PASS" if ok else "FAIL"
    print(f"  {status}: {title}" + (f" \u2014 {detail}" if detail else ""))

async def run():
    proc = subprocess.Popen(
        [sys.executable, '-m', 'uvicorn', 'main:app', '--host', HOST, '--port', str(PORT)],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    await asyncio.sleep(2.5)

    try:
        # Quiz CRUD
        print("\n\u2500\u2500 Quiz CRUD \u2500\u2500")
        s, d = _api('POST', '/api/quizzes', {'name': 'E2E-Quiz', 'max_teams': 3, 'points_per_tick': 5})
        _print("Create quiz", s == 200 and d['quiz']['name'] == 'E2E-Quiz')

        s, d = _api('POST', '/api/quizzes/E2E-Quiz/questions', {
            'question_type': 'text', 'question_text': 'Q1?', 'category': 'Gen',
            'timer_seconds': 10, 'options': ['A1', 'B1', 'C1', 'D1'], 'answer': 0})
        _print("Add Q1", s == 200 and d['question']['question_text'] == 'Q1?')

        s, d = _api('POST', '/api/quizzes/E2E-Quiz/questions', {
            'question_type': 'text', 'question_text': 'Q2?', 'category': 'Gen',
            'timer_seconds': 8, 'options': ['A2', 'B2', 'C2', 'D2'], 'answer': 1})
        _print("Add Q2", s == 200)

        s, d = _api('GET', '/api/quizzes/E2E-Quiz')
        _print("Get quiz", s == 200 and len(d['questions']) == 2, f"{len(d['questions'])} questions")

        s, d = _api('GET', '/api/quizzes/E2E-Quiz/questions')
        _print("Get questions", s == 200 and len(d) == 2, f"{len(d)} questions returned")

        # Full game flow (separate WS per role)
        print("\n\u2500\u2500 Full Game Flow \u2500\u2500")

        async with (
            websockets.connect(f"ws://{HOST}:{PORT}/ws/e2e_session") as ws_host,
            websockets.connect(f"ws://{HOST}:{PORT}/ws/e2e_session") as ws_alpha,
            websockets.connect(f"ws://{HOST}:{PORT}/ws/e2e_session") as ws_beta,
        ):
            # 1. Host sends start
            await ws_host.send(json.dumps({"type": "start", "quiz_name": "E2E-Quiz", "host_name": "HostMC"}))
            msgs = []
            for _i in range(5):
                try:
                    m = json.loads(await asyncio.wait_for(ws_host.recv(), timeout=3))
                    msgs.append(m)
                except asyncio.TimeoutError:
                    break
            types = [m['type'] for m in msgs]
            _print("session_started seen", 'session_started' in types, f"msg types: {types}")

            # 2. Alpha joins (separate socket)
            await ws_alpha.send(json.dumps({"type": "join", "team_name": "Alpha"}))
            joined_alpha = False
            alpha_count = None
            for _i in range(3):
                try:
                    m = json.loads(await asyncio.wait_for(ws_alpha.recv(), timeout=3))
                    if m['type'] == 'joined':
                        joined_alpha = True
                    elif m['type'] == 'teams_updated':
                        alpha_count = f"{m['count']}/{m['max']}"
                except asyncio.TimeoutError:
                    break
            _print("Team Alpha joined", joined_alpha, f"count={alpha_count or '?'}")

            # 3. Beta joins (separate socket)
            await ws_beta.send(json.dumps({"type": "join", "team_name": "Beta"}))
            joined_beta = False
            beta_count = None
            for _i in range(3):
                try:
                    m = json.loads(await asyncio.wait_for(ws_beta.recv(), timeout=3))
                    if m['type'] == 'joined':
                        joined_beta = True
                    elif m['type'] == 'teams_updated':
                        beta_count = f"{m['count']}/{m['max']}"
                except asyncio.TimeoutError:
                    break
            _print("Team Beta joined", joined_beta, f"count={beta_count or '?'}")

            # 4. Drain teams_updated from all sockets
            await asyncio.sleep(0.2)
            for sock in (ws_host, ws_alpha, ws_beta):
                while True:
                    try:
                        m = json.loads(await asyncio.wait_for(sock.recv(), timeout=0.5))
                        if m['type'] not in ('teams_updated',):
                            break
                    except asyncio.TimeoutError:
                        break

            # 5. Host starts round
            await ws_host.send(json.dumps({"type": "start2"}))

            # Collect go_to_question from all sockets
            got_q1 = False
            q1_text = None
            for i in range(3):
                try:
                    sock = [ws_host, ws_alpha, ws_beta][i]
                    m = json.loads(await asyncio.wait_for(sock.recv(), timeout=10))
                    if m['type'] == 'go_to_question':
                        got_q1 = True
                        q1_text = m['question']['question_text']
                except asyncio.TimeoutError:
                    pass
            _print("First question received", got_q1, f"text={q1_text}")

            # 6. Alpha answers (from Alpha's socket)
            await ws_alpha.send(json.dumps({"type": "answer", "answer_id": 0, "elapsed": 3.0}))
            got_count1 = False
            for i in range(3):
                try:
                    sock = [ws_host, ws_alpha, ws_beta][i]
                    m = json.loads(await asyncio.wait_for(sock.recv(), timeout=10))
                    if m['type'] == 'count_updated':
                        got_count1 = True
                        _print("Alpha answered", True, f"count={m['count']}/{m['total']}")
                        break
                except asyncio.TimeoutError:
                    pass

            # 7. Beta answers (from Beta's socket)
            await ws_beta.send(json.dumps({"type": "answer", "answer_id": 1, "elapsed": 6.0}))
            got_result1 = False
            r_results = []
            for i in range(20):
                try:
                    m = json.loads(await asyncio.wait_for(ws_host.recv(), timeout=10))
                    if m['type'] in ('result', 'leaderboard'):
                        got_result1 = True
                        r_results = [(r['team'], r['points']) for r in m.get('results', [])]
                        _print(f"First result ({m['type']})", True, f"results: {r_results}")
                        break
                    if m['type'] == 'timer':
                        pass
                except asyncio.TimeoutError:
                    break
            if not got_result1:
                _print("Result received", False)

            # Drain stale messages from all sockets before advancing
            for sock in (ws_host, ws_alpha, ws_beta):
                while True:
                    try:
                        m = json.loads(await asyncio.wait_for(sock.recv(), timeout=0.5))
                        if m['type'] not in ('timer', 'teams_updated', 'count_updated'):
                            break
                    except asyncio.TimeoutError:
                        break

            # 8. Host advances to Q2
            await ws_host.send(json.dumps({"type": "next"}))
            got_q2 = False
            q2_text = None
            # Drain first then read go_to_question from any socket
            for _ in range(3):
                try:
                    m = json.loads(await asyncio.wait_for(ws_host.recv(), timeout=10))
                    if m['type'] in ('timer', 'teams_updated', 'count_updated'):
                        continue
                    elif m['type'] == 'go_to_question':
                        got_q2 = True
                        q2_text = m['question']['question_text']
                        break
                except asyncio.TimeoutError:
                    pass
            if not got_q2:
                for sock in (ws_alpha, ws_beta):
                    try:
                        m = json.loads(await asyncio.wait_for(sock.recv(), timeout=10))
                        if m['type'] == 'go_to_question':
                            got_q2 = True
                            q2_text = m['question']['question_text']
                            break
                    except asyncio.TimeoutError:
                        pass
            _print("Second question received", got_q2, f"text={q2_text}")

            # 9. Alpha answers Q2 (correct = index 1)
            await ws_alpha.send(json.dumps({"type": "answer", "answer_id": 1, "elapsed": 3.0}))
            got_count2 = False
            for i in range(3):
                try:
                    sock = [ws_host, ws_alpha, ws_beta][i]
                    m = json.loads(await asyncio.wait_for(sock.recv(), timeout=15))
                    if m['type'] == 'count_updated':
                        got_count2 = True
                        _print("Second question: Alpha answered", True)
                        break
                except asyncio.TimeoutError:
                    pass

            # 10. Beta answers Q2 (correct = 1)
            await ws_beta.send(json.dumps({"type": "answer", "answer_id": 1, "elapsed": 6.0}))
            got_result2 = False
            r2_results = []
            for i in range(10):
                try:
                    m = json.loads(await asyncio.wait_for(ws_host.recv(), timeout=15))
                    if m['type'] in ('result', 'leaderboard'):
                        got_result2 = True
                        r2_results = [(r['team'], r['points']) for r in m.get('results', [])]
                        _print(f"Second result ({m['type']})", True, f"results: {r2_results}")
                        break
                    if m['type'] == 'timer':
                        pass
                except asyncio.TimeoutError:
                    break
            if not got_result2:
                _print("Second result received", False)

            # 11. Host clicks Next one last time -> final standings
            await ws_host.send(json.dumps({"type": "next"}))
            got_final = False
            standings = []
            for i in range(5):
                try:
                    m = json.loads(await asyncio.wait_for(ws_host.recv(), timeout=15))
                    if m['type'] == 'final_standings':
                        got_final = True
                        raw = m.get('standings', [])
                        # standings is list of tuples (team, total)
                        standings = [(s[0], s[1]) if isinstance(s, (list, tuple)) else (s.get('team','?'), s.get('total',0)) for s in raw]
                        _print("Final standings", True, f"standings: {standings}")
                        break
                    if m['type'] == 'timer':
                        pass
                except asyncio.TimeoutError:
                    break
            if not got_final:
                _print("Final standings", False)

    except Exception as e:
        print(f"\nError: {e}")
        import traceback; traceback.print_exc()
    finally:
        proc.terminate()
        proc.wait()

def main():
    asyncio.run(run())

if __name__ == '__main__':
    main()
