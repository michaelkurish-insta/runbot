#!/usr/bin/env python3
"""One-time Strava OAuth2 setup — exchanges authorization code for tokens.

Prerequisites:
  1. Create a Strava API app at https://www.strava.com/settings/api
  2. Set redirect URI to: http://localhost:8090/callback
  3. Set env vars: STRAVA_CLIENT_ID and STRAVA_CLIENT_SECRET

Usage:
  python scripts/setup_strava_auth.py
"""

import json
import os
import sys
import threading
import webbrowser
from http.server import HTTPServer, BaseHTTPRequestHandler
from pathlib import Path
from urllib.parse import urlparse, parse_qs

# Add project root to path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from runbase.config import load_config

REDIRECT_PORT = 8090
REDIRECT_PATH = "/callback"


class CallbackHandler(BaseHTTPRequestHandler):
    """Captures the OAuth callback and extracts the authorization code."""

    auth_code = None

    def do_GET(self):
        parsed = urlparse(self.path)
        if parsed.path == REDIRECT_PATH:
            params = parse_qs(parsed.query)
            if "code" in params:
                CallbackHandler.auth_code = params["code"][0]
                self.send_response(200)
                self.send_header("Content-Type", "text/html")
                self.end_headers()
                self.wfile.write(
                    b"<html><body><h2>Authorization successful!</h2>"
                    b"<p>You can close this tab and return to the terminal.</p>"
                    b"</body></html>"
                )
            else:
                error = params.get("error", ["unknown"])[0]
                self.send_response(400)
                self.send_header("Content-Type", "text/html")
                self.end_headers()
                self.wfile.write(
                    f"<html><body><h2>Error: {error}</h2></body></html>".encode()
                )
        else:
            self.send_response(404)
            self.end_headers()

    def log_message(self, format, *args):
        pass  # Suppress request logging


def main():
    config = load_config()
    strava_cfg = config.get("strava", {})

    client_id = strava_cfg.get("client_id") or os.environ.get("STRAVA_CLIENT_ID")
    client_secret = strava_cfg.get("client_secret") or os.environ.get("STRAVA_CLIENT_SECRET")

    if not client_id or not client_secret:
        print("Error: STRAVA_CLIENT_ID and STRAVA_CLIENT_SECRET must be set.")
        print("Set them as environment variables or in config/config.yaml under strava:")
        sys.exit(1)

    token_path = Path(strava_cfg.get("token_file", "~/runbase/state/strava_tokens.json")).expanduser()

    # Build auth URL
    auth_url = (
        f"https://www.strava.com/oauth/authorize"
        f"?client_id={client_id}"
        f"&response_type=code"
        f"&redirect_uri=http://localhost:{REDIRECT_PORT}{REDIRECT_PATH}"
        f"&scope=read,activity:read_all"
        f"&approval_prompt=auto"
    )

    # Start callback server
    server = HTTPServer(("localhost", REDIRECT_PORT), CallbackHandler)

    print(f"Opening browser for Strava authorization...")
    print(f"If the browser doesn't open, visit:\n  {auth_url}\n")
    webbrowser.open(auth_url)

    print("Waiting for callback...")
    # Serve one request
    server.handle_request()
    server.server_close()

    if not CallbackHandler.auth_code:
        print("Error: No authorization code received.")
        sys.exit(1)

    print("Got authorization code, exchanging for tokens...")

    # Exchange code for tokens via stravalib
    from stravalib import Client

    client = Client()
    token_response = client.exchange_code_for_token(
        client_id=int(client_id),
        client_secret=client_secret,
        code=CallbackHandler.auth_code,
    )

    tokens = {
        "access_token": token_response["access_token"],
        "refresh_token": token_response["refresh_token"],
        "expires_at": token_response["expires_at"],
    }

    # Save tokens
    token_path.parent.mkdir(parents=True, exist_ok=True)
    with open(token_path, "w") as f:
        json.dump(tokens, f, indent=2)

    print(f"\nTokens saved to {token_path}")
    print("You can now run: python -m runbase sync --strava")


if __name__ == "__main__":
    main()
