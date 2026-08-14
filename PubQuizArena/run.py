#!/usr/bin/env python3
"""PubQuiz Arena - Launcher with auto-port detection."""
import socket, subprocess, sys, os

def find_open_port(start=8002, max_attempts=20):
    for port in range(start, start + max_attempts):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            s.bind(('0.0.0.0', port))
            s.close()
            return port
        except OSError:
            s.close()
    return None

def get_local_ip():
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(('8.8.8.8', 80))
        ip = s.getsockname()[0]
        s.close()
        return ip
    except:
        return 'localhost'

def main():
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    os.chdir(BASE_DIR)
    port = find_open_port()
    if port is None:
        print("ERROR: No open port found")
        sys.exit(1)
    local_ip = get_local_ip()
    print("\n" + "="*60)
    print("  PubQuiz Arena")
    print("="*60)
    print(f"  Admin:      http://{local_ip}:{port}/#admin")
    print(f"  Host/:      http://{local_ip}:{port}/#host")
    print(f"  Player/:    http://{local_ip}:{port}/#player")
    print("="*60)
    print(f"\nOpen http://{local_ip}:{port}/#admin in your browser.")
    print(f"Teams join from the same WiFi — share the QR code once host mode starts.")
    print(f"\nPress Ctrl+C to stop.\n")
    print("="*60 + "\n")
    subprocess.run([
        sys.executable, '-m', 'uvicorn', 'main:app',
        '--host', '0.0.0.0',
        '--port', str(port),
        '--log-level', 'warning'
    ])

if __name__ == '__main__':
    main()
