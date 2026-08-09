#!/usr/bin/env python3
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
        async with websockets.connect(f"ws://{HOST}:{PORT}/ws/test_wsgame") as host_ws:
            async def recv_all(ws, timeout=2.0):
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

            await host_ws.send(json.dumps({"type":"start","quiz_name":"General Knowledge","host_name":"HostMC"}))
            all_msgs = await recv_all(host_ws, 2.0)
            print_test("session_started", len(find_msgs(all_msgs, "session_started")) > 0)
            await host_ws.send(json.dumps({"type":"start2"}))
            found_go = False
            while not found_go:
                try:
                    m = json.loads(await asyncio.wait_for(host_ws.recv(), timeout=3.0))
                    if m.get("type") == "go_to_question":
                        found_go = True
                except asyncio.TimeoutError:
                    break
            collected = []
            start = asyncio.get_event_loop().time()
            while (asyncio.get_event_loop().time() - start) < 25.0:
                try:
                    m = json.loads(await asyncio.wait_for(host_ws.recv(), timeout=1.0))
                    collected.append(m)
                    if m.get("type") in ("result", "leaderboard"):
                        break
                except asyncio.TimeoutError:
                    pass
            print_test("Q1 result/leaderboard", any(m.get("type") in ("result","leaderboard") for m in collected))

            async with websockets.connect(f"ws://{HOST}:{PORT}/ws/test_wsgame") as player_ws:
                await player_ws.send(json.dumps({"type":"join","team_name":"Alpha"}))
                p_msgs = await recv_all(player_ws, 1.0)
                print_test("Player joined", len(find_msgs(p_msgs, "joined")) > 0)
                host_upd = await recv_all(host_ws, 1.0)
                print_test("Host sees teams_update", len(find_msgs(host_upd, "teams_updated")) > 0)
                await player_ws.send(json.dumps({"type":"answer","answer_id":"b","elapsed":5.0}))
                r1, lb1 = [], []
                start = asyncio.get_event_loop().time()
                while (asyncio.get_event_loop().time() - start) < 5.0:
                    try:
                        m = json.loads(await asyncio.wait_for(host_ws.recv(), timeout=1.0))
                        if m.get("type") == "result": r1.append(m)
                        elif m.get("type") == "leaderboard": lb1.append(m)
                        if r1 and lb1: break
                    except asyncio.TimeoutError:
                        pass
                print_test("Q1 result", len(r1) > 0)
                print_test("Q1 leaderboard", len(lb1) > 0)
                if lb1:
                    pts = next((s.get("points",0) for s in lb1[0].get("standings",[]) if s.get("team")=="Alpha"), 0)
                    print_test("Alpha scored", pts > 0, f"Alpha={pts}pts")
                await host_ws.send(json.dumps({"type":"next"}))
                found_go = False
                while not found_go:
                    try:
                        m = json.loads(await asyncio.wait_for(host_ws.recv(), timeout=3.0))
                        if m.get("type") == "go_to_question":
                            found_go = True
                    except asyncio.TimeoutError:
                        break
                await asyncio.sleep(5.5)
                await player_ws.send(json.dumps({"type":"answer","answer_id":"b","elapsed":5.0}))
                r2, lb2 = [], []
                start = asyncio.get_event_loop().time()
                while (asyncio.get_event_loop().time() - start) < 5.0:
                    try:
                        m = json.loads(await asyncio.wait_for(host_ws.recv(), timeout=1.0))
                        if m.get("type") == "result": r2.append(m)
                        elif m.get("type") == "leaderboard": lb2.append(m)
                        if r2 and lb2: break
                    except asyncio.TimeoutError:
                        pass
                print_test("Q2 result", len(r2) > 0)
                print_test("Q2 leaderboard", len(lb2) > 0)
                stands = []
                for _ in range(8):
                    await host_ws.send(json.dumps({"type":"next"}))
                    while True:
                        try:
                            m = json.loads(await asyncio.wait_for(host_ws.recv(), timeout=2.0))
                            if m.get("type") == "final_standings":
                                stands.append(m)
                                break
                            if m.get("type") == "go_to_question":
                                break
                        except asyncio.TimeoutError:
                            break
                    if stands:
                        break
                print_test("final_standings", len(stands) > 0)
                if stands:
                    alpha = next((s[1] for s in stands[0].get("standings",[]) if s[0]=="Alpha"), 0)
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
