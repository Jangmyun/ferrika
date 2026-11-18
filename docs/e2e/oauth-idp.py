"""Test-only OAuth 2.0 client-credentials token endpoint for kafka-oauth-compose.yml.

POST /token with client_secret_basic or client_secret_post. Any client id is
accepted when the secret is "e2e-secret"; the answer is an unsecured JWT
(alg "none") with sub = client id, which Kafka's unsecured validator accepts.
TOKEN_LIFETIME (seconds, default 120) is short so token refresh is exercised.
Never use this outside a local test.
"""

import base64
import json
import os
import time
import urllib.parse
from http.server import BaseHTTPRequestHandler, HTTPServer

SECRET = os.environ.get("CLIENT_SECRET", "e2e-secret")
LIFETIME = int(os.environ.get("TOKEN_LIFETIME", "120"))


def b64url(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode()


class Handler(BaseHTTPRequestHandler):
    def do_POST(self):
        length = int(self.headers.get("Content-Length", "0"))
        form = urllib.parse.parse_qs(self.rfile.read(length).decode())
        client_id = (form.get("client_id") or [None])[0]
        secret = (form.get("client_secret") or [None])[0]
        auth = self.headers.get("Authorization", "")
        if auth.startswith("Basic "):
            pair = base64.b64decode(auth[6:]).decode()
            user, _, password = pair.partition(":")
            client_id = urllib.parse.unquote_plus(user)
            secret = urllib.parse.unquote_plus(password)
        if (form.get("grant_type") or [None])[0] != "client_credentials":
            return self.reply(400, {"error": "unsupported_grant_type"})
        if not client_id or secret != SECRET:
            return self.reply(401, {"error": "invalid_client", "error_description": "bad secret"})
        now = int(time.time())
        claims = {"sub": client_id, "iat": now, "exp": now + LIFETIME}
        scope = (form.get("scope") or [None])[0]
        if scope:
            claims["scope"] = scope
        token = b64url(b'{"alg":"none"}') + "." + b64url(json.dumps(claims).encode()) + "."
        self.reply(200, {"access_token": token, "token_type": "Bearer", "expires_in": LIFETIME})

    def reply(self, status, body):
        data = json.dumps(body).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)


HTTPServer(("0.0.0.0", 18080), Handler).serve_forever()
