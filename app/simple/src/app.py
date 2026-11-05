"""Dependency-free deployment check, with no access to workspace data."""

import os
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlsplit


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        path = urlsplit(self.path).path
        if path == "/":
            status, content_type, body = (
                200,
                "text/html; charset=utf-8",
                b"<!doctype html><html><head><title>Pairity</title></head>"
                b"<body><h1>Pairity is running</h1>"
                b"<p>Basic Databricks App deployment succeeded.</p></body></html>",
            )
        elif path == "/health":
            status, content_type, body = 200, "application/json", b'{"status":"ok"}'
        else:
            status, content_type, body = 404, "text/plain", b"Not found"
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format, *args):
        # Do not log request URLs, headers or runtime credentials.
        pass


if __name__ == "__main__":
    server = ThreadingHTTPServer(
        ("0.0.0.0", int(os.environ.get("DATABRICKS_APP_PORT", "8000"))), Handler
    )
    print(f"Pairity simple app listening on port {server.server_port}", flush=True)
    server.serve_forever()
