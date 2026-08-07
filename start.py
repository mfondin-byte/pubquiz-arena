#!/usr/bin/env python3
"""PubQuiz Arena — Startup script."""

import os
import subprocess
import sys

PORT = int(os.environ.get("PUBQUIZ_PORT", "8000"))

# If port is already taken, try next ones
for port in [int(os.environ.get("PUBQUIZ_PORT", "8000")), 
             int(os.environ.get("PUBQUIZ_PORT", "8000")) + 1,
             int(os.environ.get("PUBQUIZ_PORT", "8000")) + 2]:
    PORT = port
    try:
        import socket
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.bind(("0.0.0.0", PORT))
        s.close()
        break
    except OSError:
        continue
else:
    print("ERROR: Could not find an open port")
    sys.exit(1)

# Get local IP for QR code display
try:
    import socket as sock
    s = sock.socket(sock.AF_INET, sock.AF_DGRAM)
    s.connect(("8.8.8.8", 80))
    local_ip = s.getsockname()[0]
    s.close()
except Exception:
    local_ip = "localhost"

print("=" * 60)
print("  PubQuiz Arena")
print("=" * 60)
print(f"  Admin/Host:  http://{local_ip}:{PORT}/#admin")
print(f"  Instructor:  http://{local_ip}:{PORT}/#host")
print(f"  Player:      http://{local_ip}:{PORT}/#player")
print("=" * 60)
print(f"  Press Ctrl+C to stop")
print("=" * 60)

subprocess.run([
    sys.executable, "-m", "uvicorn", "main:app",
    "--host", "0.0.0.0",
    "--port", str(PORT),
    "--log-level", "warning"
], env={**os.environ, "PUBQUIZ_PORT": str(PORT)})
