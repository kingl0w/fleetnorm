#!/usr/bin/env python3
"""slow webhook endpoint: 200 OK after a fixed delay. usage: slow.py [delay_seconds] [port]"""
import signal
import sys
import time
from http.server import BaseHTTPRequestHandler, HTTPServer

delay = float(sys.argv[1]) if len(sys.argv) > 1 else 0.2
port = int(sys.argv[2]) if len(sys.argv) > 2 else 9009
received = 0


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def do_POST(self):
        global received
        self.rfile.read(int(self.headers.get("Content-Length", 0)))
        time.sleep(delay)
        self.send_response(200)
        self.send_header("Content-Length", "0")
        self.end_headers()
        received += 1
        if received % 100 == 0:
            print(f"{received} received", flush=True)

    def log_message(self, *args):
        pass


signal.signal(signal.SIGTERM, lambda *_: sys.exit(0))
print(f"listening on 127.0.0.1:{port}, {delay}s per request", flush=True)
try:
    HTTPServer(("127.0.0.1", port), Handler).serve_forever()
finally:
    print(f"{received} received total", flush=True)
