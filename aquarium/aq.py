#!/usr/bin/env python3
"""aq.py — одна команда в cdp-bridge (127.0.0.1:8778).

    python3 aquarium/aq.py '{"cmd":"status"}'
    python3 aquarium/aq.py '{"cmd":"tabs"}'
    python3 aquarium/aq.py '{"cmd":"evaluate","expression":"document.title","context":"<tid>"}'
"""
import json
import os
import socket
import sys


def main():
    if len(sys.argv) < 2:
        print(__doc__, file=sys.stderr)
        return 1
    try:
        cmd = json.loads(sys.argv[1])
    except json.JSONDecodeError as e:
        print(f'ERROR: invalid JSON: {e}', file=sys.stderr)
        return 1
    host, _, port = os.getenv("AI_BROWSER_AQ", "127.0.0.1:8778").rpartition(":")
    try:
        with socket.create_connection((host or "127.0.0.1", int(port)),
                                      timeout=180) as s:
            s.sendall((json.dumps(cmd) + "\n").encode())
            buf = b""
            while not buf.endswith(b"\n"):
                chunk = s.recv(1 << 22)
                if not chunk:
                    break
                buf += chunk
        print(buf.decode().strip())
        return 0
    except OSError as e:
        print(f'ERROR: aquarium bridge not reachable: {e}', file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
