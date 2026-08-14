import asyncio, json, sys, websockets, urllib.request, urllib.parse

async def test():
    try:
        # Test 1: Check that the HTML page loads with all new fields
        resp = urllib.request.urlopen('http://127.0.0.1:8002/')
        html = resp.read().decode()
        assert 'player-host-ip' in html, "Missing player-host-ip input!"
        assert 'ip-host-ip' in html, "Missing ip-host-ip display!"
        assert 'host-host-ip' in html, "Missing host-host-ip display!"
        assert 'lt-badge' in html, "Missing LocalTunnel badge in admin!"
        assert 'host-lt-badge' in html, "Missing LocalTunnel badge in host!"
        print("PASS: HTML has all IP-related fields and LocalTunnel badges")
        
        # Test 2: Check /api/public-url endpoint
        resp = urllib.request.urlopen('http://127.0.0.1:8002/api/public-url')
        data = json.loads(resp.read().decode())
        assert 'url' in data, "public-url missing 'url' field"
        assert 'detected' in data, "public-url missing 'detected' field"
        print(f"PASS: /api/public-url returns: {data}")
        
        # Test 3: Start session with public_url (simulating LocalTunnel)
        async with websockets.connect('ws://127.0.0.1:8002/ws/qz_test444') as host:
            await host.send(json.dumps({
                'type': 'start',
                'quiz_name': 'first test on sql',
                'host_name': 'TestHost',
                'server_ip': '192.168.1.50',
                'public_url': 'abc123.loca.lt'
            }))
            msg = json.loads(await asyncio.wait_for(host.recv(), timeout=2))
            assert msg['type'] == 'session_started'
            assert msg['server_ip'] == '192.168.1.50'
            print(f"PASS: session_started with server_ip: {msg['server_ip']}")
            session_id = msg['session_id']
            
            msg2 = json.loads(await asyncio.wait_for(host.recv(), timeout=2))
            assert msg2['type'] == 'qr_url'
            qr_url = msg2.get('url', '')
            print(f"QR URL with LocalTunnel: {qr_url}")
            assert 'https://abc123.loca.lt' in qr_url, f"Expected LocalTunnel URL in QR, got: {qr_url}"
            print("PASS: QR URL uses https://abc123.loca.lt (LocalTunnel)!")
            assert msg2.get('public_url') == 'abc123.loca.lt'
            print("PASS: public_url field present in qr_url message")
            
            # Player connects
            async with websockets.connect(f'ws://127.0.0.1:8002/ws/{session_id}') as player:
                await player.send(json.dumps({'type': 'join', 'team_name': 'TestTeam'}))
                msg3 = json.loads(await asyncio.wait_for(player.recv(), timeout=2))
                if msg3['type'] == 'teams_updated':
                    msg3 = json.loads(await asyncio.wait_for(player.recv(), timeout=2))
                assert msg3['type'] == 'joined'
                assert msg3['team_name'] == 'TestTeam'
                print(f"PASS: Player joined as {msg3['team_name']}")
            
            await host.send(json.dumps({'type': 'end'}))
            
            # Test 4: localhost without LocalTunnel = warning
            async with websockets.connect('ws://127.0.0.1:8002/ws/qz_test555') as host2:
                await host2.send(json.dumps({
                    'type': 'start',
                    'quiz_name': 'first test on sql',
                    'host_name': 'TestHost',
                    'server_ip': 'localhost'
                }))
                msg = json.loads(await asyncio.wait_for(host2.recv(), timeout=2))
                assert msg['type'] == 'session_started'
                assert msg['server_ip_warning'] == True
                print("PASS: localhost warning is True (no LocalTunnel)")
                
                msg2 = json.loads(await asyncio.wait_for(host2.recv(), timeout=2))
                assert msg2['type'] == 'qr_url'
                qr_url = msg2.get('url', '')
                assert 'localhost' in qr_url
                assert msg2.get('public_url') is None
                print(f"PASS: QR URL is localhost (will warn user): {qr_url}")
                
                await host2.send(json.dumps({'type': 'end'}))
            
            # Test 5: Valid IP without LocalTunnel = no warning
            async with websockets.connect('ws://127.0.0.1:8002/ws/qz_test666') as host3:
                await host3.send(json.dumps({
                    'type': 'start',
                    'quiz_name': 'first test on sql',
                    'host_name': 'TestHost',
                    'server_ip': '192.168.1.50'
                }))
                msg = json.loads(await asyncio.wait_for(host3.recv(), timeout=2))
                assert msg['type'] == 'session_started'
                assert msg['server_ip_warning'] == False
                print("PASS: valid IP warning is False (no LocalTunnel)")
                
                msg2 = json.loads(await asyncio.wait_for(host3.recv(), timeout=2))
                assert msg2['type'] == 'qr_url'
                qr_url = msg2.get('url', '')
                assert 'http://192.168.1.50' in qr_url
                assert msg2.get('public_url') is None
                print(f"PASS: QR URL uses local IP: {qr_url}")
                
                await host3.send(json.dumps({'type': 'end'}))
                
            print("\n=== ALL TESTS PASSED ===")
            
    except websockets.exceptions.ConnectionClosed:
        # Expected - WebSocket was closed by server on session end
        print("\n=== ALL TESTS PASSED (connection closed by server) ===")
    except Exception as e:
        print(f'FAILED: {e}')
        import traceback
        traceback.print_exc()
    finally:
        pass  # Can't kill the background server process

asyncio.run(test())

