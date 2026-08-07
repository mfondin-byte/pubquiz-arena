#!/usr/bin/env python3
"""WebSocket game flow test - question_index based synchronization."""
import asyncio, json, subprocess, sys
import websockets

HOST = "127.0.0.1"
PORT = 8070

async def run():
    proc = subprocess.Popen(
        [sys.executable, "-m", "uvicorn", "main:app", "--host", HOST, "--port", str(PORT)],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    await asyncio.sleep(4)

    try:
        print()
        print("-- Game Flow Test --")
        async with websockets.connect(f"ws://{HOST}:{PORT}/ws/test_wsgame") as ws:
            async def recv_all(timeout=2.0):
                msgs = []
                try:
                    while True:
                        msg = json.loads(await asyncio.wait_for(ws.recv(), timeout=timeout))
                        msgs.append(msg)
                except asyncio.TimeoutError:
                    pass
                return msgs

            def find_msgs(msgs, msg_type):
                return [m for m in msgs if m.get("type") == msg_type]

            def print_test(label, ok, detail=""):
                status = "PASS" if ok else "FAIL"
                print(f"  {status}: {label}" + (f" -- {detail}" if detail else ""))

            # Start game
            await ws.send(json.dumps({"type":"start","quiz_name":"Fun Night Quiz","host_name":"HostMC"}))
            all_msgs = await recv_all(2.0)
            session_msgs = find_msgs(all_msgs, "session_started")
            print_test("session_started", len(session_msgs) > 0)

            await ws.send(json.dumps({"type":"join","team_name":"Alpha"}))
            await asyncio.sleep(0.5)
            await ws.send(json.dumps({"type":"start2"}))
            
            # Wait for go_to_question for Q1
            found_go = False
            while not found_go:
                try:
                    m = json.loads(await asyncio.wait_for(ws.recv(), timeout=3.0))
                    if m.get("type") == "go_to_question":
                        found_go = True
                except asyncio.TimeoutError:
                    break
            
            # Answer Q1 (correct answer is 'b')
            await ws.send(json.dumps({"type":"answer","answer_id":"b","elapsed":3.0}))
            
            # Collect result + leaderboard for Q1
            q1_result, q1_lb = [], []
            q1_start = asyncio.get_event_loop().time()
            while (asyncio.get_event_loop().time() - q1_start) < 5.0:
                try:
                    m = json.loads(await asyncio.wait_for(ws.recv(), timeout=1.0))
                    if m.get("type") == "result":
                        q1_result.append(m)
                    elif m.get("type") == "leaderboard":
                        q1_lb.append(m)
                    if q1_result and q1_lb:
                        break
                except asyncio.TimeoutError:
                    pass
            print_test("Q1 result", len(q1_result) > 0)
            print_test("Q1 leaderboard", len(q1_lb) > 0)
            if q1_lb:
                alpha_pts = next((s["points"] for s in q1_lb[0].get("standings",[]) if s["team"]=="Alpha"), 0)
                print_test("Alpha scored", alpha_pts > 0, f"Alpha={alpha_pts}pts")

            # Q2-Q5 using go_to_question as sync point
            for qnum in range(2, 6):
                # Send next to advance to this question
                await ws.send(json.dumps({"type":"next"}))
                found_go = False
                while not found_go:
                    try:
                        m = json.loads(await asyncio.wait_for(ws.recv(), timeout=3.0))
                        if m.get("type") == "go_to_question":
                            found_go = True
                            break
                    except asyncio.TimeoutError:
                        break

                await asyncio.sleep(5.5)

                ans = "c" if qnum == 2 else "b"
                await ws.send(json.dumps({"type":"answer","answer_id":ans,"elapsed":5.0}))

                result_msgs, lb_msgs = [], []
                start = asyncio.get_event_loop().time()
                while (asyncio.get_event_loop().time() - start) < 5.0:
                    try:
                        m = json.loads(await asyncio.wait_for(ws.recv(), timeout=1.0))
                        if m.get("type") == "result":
                            result_msgs.append(m)
                        elif m.get("type") == "leaderboard":
                            lb_msgs.append(m)
                        if result_msgs and lb_msgs:
                            break
                    except asyncio.TimeoutError:
                        pass

                print_test(f"Q{qnum} result", len(result_msgs) > 0)
                print_test(f"Q{qnum} leaderboard", len(lb_msgs) > 0)

            # Check final
            stands_msgs = []
            start = asyncio.get_event_loop().time()
            while (asyncio.get_event_loop().time() - start) < 3.0:
                try:
                    m = json.loads(await asyncio.wait_for(ws.recv(), timeout=1.0))
                    if m.get("type") == "final_standings":
                        stands_msgs.append(m)
                except asyncio.TimeoutError:
                    pass

            print_test("final_standings", len(stands_msgs) > 0)
            if stands_msgs:
                alpha = next((s[1] for s in stands_msgs[0].get("standings",[]) if s[0]=="Alpha"), 0)
                print_test("Alpha total", alpha > 0, f"Total={alpha}pts")

        print()
        print("-- Done --")
    except Exception as e:
        print(f"Error: {e}")
        import traceback; traceback.print_exc()
    finally:
        proc.terminate()
        proc.wait()

asyncio.run(run())
