"""Serve the client matrix's Cloud Run revision: answer every GET on $PORT with 200."""

import os
from http.server import BaseHTTPRequestHandler, HTTPServer


class Handler(BaseHTTPRequestHandler):
    """Reply to any GET so the rollout operation sees a ready revision."""

    def do_GET(self) -> None:
        body = b"client matrix revision\n"
        self.send_response(200)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *_: object) -> None:
        """Keep request lines out of the core log."""


if __name__ == "__main__":
    HTTPServer(("127.0.0.1", int(os.environ["PORT"])), Handler).serve_forever()
