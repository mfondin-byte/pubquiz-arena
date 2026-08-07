#!/usr/bin/env python3
"""Comprehensive game flow test."""
import asyncio, json, http.client, subprocess, time, sys, os
os.environ['PYTHONASYNCIODEBUG'] = '1'

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
    print(f"  {status}: {title}" + (f" — {detail}" if detail else ""))

async def run():
    proc = subprocess.Popen(
        [sys.executable,'-m','uvicorn','main:app','--host',HOST,'--port',str(PORT)],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    await asyncio.sleep(2.5)

    try:
        # ── Test 1: Quiz CRUD ──
        print("\n── Quiz CRUD ──")
        s, d = _api('POST','/api/quizzes', {'name':'E2E-Quiz','max_teams':3,'points_per_tick':5})
        _print("Create quiz", s==200 and d['quiz']['name']=='E2E-Quiz')

        s, d = _api('POST','/api/quizzes/E2E-Quiz/questions', {
            'question_type':'text','question_text':'Q1?','category':'Gen','timer_seconds':10,
            'options':['A1','B1','C1','D1'],
            'answer':0})
        _print("Add Q1", s==200 and d['question']['question_text']=='Q1?')

        s, d = _api('POST','/api/quizzes/E2E-Quiz/questions', {
            'question_type':'text','question_text':'Q2?','category':'Gen','timer_seconds':8,
            'options':['A2','B2','C2','D2'],
            'answer':1})
        _print("Add Q2", s==200)

        s, d = _api('GET','/api/quizzes/E2E-Quiz')
        _print("Get quiz", s==200 and len(d['questions'])==2, f"{len(d['questions'])} questions")

        s, d = _api('GET','/api/quizzes/E2E-Quiz/questions')
        _print("Get questions", s==200 and len(d)==2, f"{len(d)} questions returned")

        # ── Test 2: Full Game Flow ──
        print("\n── Full Game Flow ──")

        async with websockets.connect(f"ws://{HOST}:{PORT}/ws/e2e_session") as ws:
            # Host sends start
            await ws.send(json.dumps({"type":"start","quiz_name":"E2E-Quiz","host_name":"HostMC"}))
            msgs = []
            for i in range(5):
                try:
                    m = json.loads(await asyncio.wait_for(ws.recv(), timeout=3))
                    msgs.append(m)
                except asyncio.TimeoutError:
                    break

            types = [m['type'] for m in msgs]
            _print("session_started seen", 'session_started' in types, f"msg types: {types}")

            # Join as Team Alpha
            await ws.send(json.dumps({"type":"join","team_name":"Alpha"}))
            for i in range(2):
                try:
                    m = json.loads(await asyncio.wait_for(ws.recv(), timeout=3))
                    if m['type']=='teams_updated':
                        _print("Team Alpha joined", True, f"count={m['count']}/{m['max']}")
                        break
                except asyncio.TimeoutError:
                    break
            else:
                _print("Team Alpha joined", False, "no teams_updated message")

            # Join as Team Beta
            await ws.send(json.dumps({"type":"join","team_name":"Beta"}))
            for i in range(2):
                try:
                    m = json.loads(await asyncio.wait_for(ws.recv(), timeout=3))
                    if m['type']=='teams_updated':
                        _print("Team Beta joined", True, f"count={m['count']}/{m['max']}")
                        break
                except asyncio.TimeoutError:
                    break
            else:
                _print("Team Beta joined", False)

            # Start the round
            await ws.send(json.dumps({"type":"start2"}))
            for i in range(5):
                try:
                    m = json.loads(await asyncio.wait_for(ws.recv(), timeout=3))
                    if m['type'] in ('game_starting','question'):
                        _print(f"Round started ({m['type']})", True)
                        break
                except asyncio.TimeoutError:
                    break
            else:
                _print("Round started", False)

            # Answer for Alpha (correct, 5s elapsed)
            await ws.send(json.dumps({"type":"answer","answer_id":"0","elapsed":5.0}))
            for i in range(2):
                try:
                    m = json.loads(await asyncio.wait_for(ws.recv(), timeout=3))
                    if m['type']=='count_updated':
                        _print("Alpha answered", True, f"count={m['count']}/{m['total']}")
                        break
                except asyncio.TimeoutError:
                    break

            # Answer for Beta (wrong, 7s elapsed) -- this should trigger result
            await ws.send(json.dumps({"type":"answer","answer_id":"1","elapsed":7.0}))
            result_wait = False
            for i in range(5):
                try:
                    m = json.loads(await asyncio.wait_for(ws.recv(), timeout=10))
                    if m['type'] in ('result','leaderboard'):
                        result_wait = True
                        _print(f"{m['type']} received", True, f"results: {[(r['team'],r['points']) for r in m.get('results',[])]}")
                    if m['type']=='timer':
                        pass  # ignore timer ticks
                except asyncio.TimeoutError:
                    break

            if not result_wait:
                _print("Result received", False)

            # Host clicks Next
            await ws.send(json.dumps({"type":"next"}))
            next_received = False
            for i in range(5):
                try:
                    m = json.loads(await asyncio.wait_for(ws.recv(), timeout=30))
                    if m['type']=='question':
                        next_received = True
                        _print("Second question received", True, f"text={m['question']['question_text']}")
                        break
                    elif m['type']=='timer':
                        pass
                except asyncio.TimeoutError:
                    break
            if not next_received:
                _print("Second question received", False)

            # Answer for Alpha (correct, 3s)
            await ws.send(json.dumps({"type":"answer","answer_id":"1","elapsed":3.0}))
            answered = False
            for i in range(5):
                try:
                    m = json.loads(await asyncio.wait_for(ws.recv(), timeout=30))
                    if m['type']=='count_updated':
                        answered = True
                        _print("Second question: Alpha answered", True)
                        break
                except asyncio.TimeoutError:
                    break
            if not answered:
                _print("Second question: Alpha answered", False)

            # Answer for Beta (correct, 6s)
            await ws.send(json.dumps({"type":"answer","answer_id":"1","elapsed":6.0}))
            got_result2 = False
            for i in range(5):
                try:
                    m = json.loads(await asyncio.wait_for(ws.recv(), timeout=30))
                    if m['type'] in ('result','leaderboard'):
                        got_result2 = True
                        _print(f"Second result ({m['type']})", True)
                    if m['type']=='timer':
                        pass
                except asyncio.TimeoutError:
                    break
            if not got_result2:
                _print("Second result received", False)

            # Host clicks Next one last time
            await ws.send(json.dumps({"type":"next"}))
            got_final = False
            for i in range(5):
                try:
                    m = json.loads(await asyncio.wait_for(ws.recv(), timeout=30))
                    if m['type']=='final_standings':
                        got_final = True
                        _print("Final standings", True, f"standings: {m.get('standings',[])}")
                        break
                    if m['type']=='timer':
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

async def run():
    import websockets
    proc = subprocess.Popen(
        [sys.executable,'-m','uvicorn','main:app','--host',HOST,'--port',str(PORT)],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    await asyncio.sleep(2.5)

    try:
        _run_tests()
    finally:
        proc.terminate()
        proc.wait()

def _run_tests():
    print("\n── Quiz CRUD ──")
    s, d = _api('POST','/api/quizzes', {'name':'E2E-Quiz','max_teams':3,'points_per_tick':5})
    _print("Create quiz", s==200 and d['quiz']['name']=='E2E-Quiz')

    s, d = _api('POST','/api/quizzes/E2E-Quiz/questions', {
        'question_type':'text','question_text':'Q1?','category':'Gen','timer_seconds':10,
        'options':['A1','B1','C1','D1'],
        'answer':0})
    _print("Add Q1", s==200 and d['question']['question_text']=='Q1?')

    s, d = _api('POST','/api/quizzes/E2E-Quiz/questions', {
        'question_type':'text','question_text':'Q2?','category':'Gen','timer_seconds':8,
        'options':['A2','B2','C2','D2'],
        'answer':1})
    _print("Add Q2", s==200)

    s, d = _api('GET','/api/quizzes/E2E-Quiz')
    _print("Get quiz", s==200 and len(d['questions'])==2, f"{len(d['questions'])} questions")

    s, d = _api('GET','/api/quizzes/E2E-Quiz/questions')
    _print("Get questions", s==200 and len(d)==2, f"{len(d)} questions returned")

def main():
    asyncio.run(run())

if __name__ == '__main__':
    main()
