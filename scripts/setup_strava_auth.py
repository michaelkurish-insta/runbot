#!/usr/bin/env python3
"""One-time Strava OAuth2 setup — exchanges authorization code for tokens.

Prerequisites:
  1. Create a Strava API app at https://www.strava.com/settings/api
  2. Set redirect URI to: http://localhost:8090/callback
  3. Set env vars: STRAVA_CLIENT_ID and STRAVA_CLIENT_SECRET

Usage:
  python scripts/setup_strava_auth.py
"""

import argparse
import json
import os
import secrets
import sys
import time
import webbrowser
from http.server import HTTPServer, BaseHTTPRequestHandler
from pathlib import Path
from urllib.parse import urlparse, parse_qs, urlencode

# Add project root to path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from runbase.config import load_config

REDIRECT_PORT = 8090
REDIRECT_PATH = "/callback"


class CallbackHandler(BaseHTTPRequestHandler):
    """Captures the OAuth callback and extracts the authorization code."""

    auth_code = None
    auth_error = None
    expected_state = None
    scopes = []

    def do_GET(self):
        parsed = urlparse(self.path)
        if parsed.path == REDIRECT_PATH:
            params = parse_qs(parsed.query)
            if not secrets.compare_digest(params.get("state", [""])[0], self.expected_state or ""):
                self.send_response(400)
                self.end_headers()
                self.wfile.write(b"Invalid authorization state. Use the current Runbot login link.")
                return
            if "code" in params:
                CallbackHandler.auth_code = params["code"][0]
                CallbackHandler.scopes = params.get("scope", [""])[0].split(",")
                self.send_response(200)
                self.send_header("Content-Type", "text/html")
                self.end_headers()
                self.wfile.write(
                    b"<html><body><h2>Authorization received</h2>"
                    b"<p>Runbot is finishing the connection. You can return to the chat.</p>"
                    b"</body></html>"
                )
            else:
                error = params.get("error", ["unknown"])[0]
                CallbackHandler.auth_error = error
                self.send_response(400)
                self.send_header("Content-Type", "text/html")
                self.end_headers()
                self.wfile.write(
                    b"<html><body><h2>Authorization was not completed.</h2></body></html>"
                )
        else:
            self.send_response(404)
            self.end_headers()

    def log_message(self, format, *args):
        pass  # Suppress request logging


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--no-browser", action="store_true", help="Print the login link without opening a browser")
    parser.add_argument("--timeout", type=int, default=900, help="Seconds to wait for authorization (default: 900)")
    args = parser.parse_args()
    config = load_config()
    strava_cfg = config.get("strava", {})

    client_id = strava_cfg.get("client_id") or os.environ.get("STRAVA_CLIENT_ID")
    client_secret = strava_cfg.get("client_secret") or os.environ.get("STRAVA_CLIENT_SECRET")

    if not str(client_id or "").isdigit() or not client_secret or str(client_secret).startswith("$"):
        print("Error: STRAVA_CLIENT_ID and STRAVA_CLIENT_SECRET must be set.")
        print("Set them as environment variables or in config/config.yaml under strava:")
        sys.exit(1)

    token_path = Path(strava_cfg.get("token_file", "~/runbase/state/strava_tokens.json")).expanduser()

    # Build auth URL
    CallbackHandler.auth_code = None
    CallbackHandler.auth_error = None
    CallbackHandler.expected_state = secrets.token_urlsafe(24)
    auth_url = "https://www.strava.com/oauth/authorize?" + urlencode({
        "client_id": client_id, "response_type": "code",
        "redirect_uri": f"http://localhost:{REDIRECT_PORT}{REDIRECT_PATH}",
        "scope": "read,activity:read_all", "approval_prompt": "force",
        "state": CallbackHandler.expected_state,
    })

    # Start callback server
    server = HTTPServer(("localhost", REDIRECT_PORT), CallbackHandler)

    print(f"Authorize Runbot with Strava:\n{auth_url}\n", flush=True)
    if not args.no_browser:
        webbrowser.open(auth_url)

    print("Waiting for callback...")
    server.timeout = 1
    deadline = time.monotonic() + args.timeout
    try:
        while not CallbackHandler.auth_code and not CallbackHandler.auth_error and time.monotonic() < deadline:
            server.handle_request()
    finally:
        server.server_close()

    if not CallbackHandler.auth_code:
        print("Authorization was declined or timed out. Run the setup command again to retry.")
        sys.exit(1)

    print("Got authorization code, exchanging for tokens...")

    # Exchange code for tokens via stravalib
    from stravalib import Client

    from runbase.ingest.strava_sync import _configure_client_credentials
    _configure_client_credentials(config)
    client = Client()
    try:
        token_response = client.exchange_code_for_token(
            client_id=int(client_id), client_secret=client_secret,
            code=CallbackHandler.auth_code,
        )
    except Exception as exc:
        status = getattr(getattr(exc, "response", None), "status_code", None)
        print(f"Strava token exchange failed ({status or type(exc).__name__}). "
              "Check the app ID and secret, then authorize again.")
        sys.exit(1)

    tokens = {
        "access_token": token_response["access_token"],
        "refresh_token": token_response["refresh_token"],
        "expires_at": token_response["expires_at"],
        "scope": CallbackHandler.scopes,
    }

    # Save tokens
    token_path.parent.mkdir(parents=True, exist_ok=True)
    temporary = token_path.with_suffix(".tmp")
    fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "w") as f:
        json.dump(tokens, f, indent=2)
    os.chmod(temporary, 0o600)
    temporary.replace(token_path)

    print(f"\nTokens saved to {token_path}")
    print("You can now run: python -m runbase sync --strava")


if __name__ == "__main__":
    main()
