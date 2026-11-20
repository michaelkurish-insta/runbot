#!/usr/bin/env python3
"""One-time Garmin Connect authentication setup.

Authenticates with Garmin Connect and saves session tokens
for reuse by the sync module.

Prerequisites:
  1. Set env vars: GARMIN_EMAIL and GARMIN_PASSWORD
     (or configure in config/config.yaml under garmin:)

Usage:
  python scripts/setup_garmin_auth.py
"""

import os
import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from runbase.config import load_config


def main():
    config = load_config()
    garmin_cfg = config.get("garmin", {})

    email = garmin_cfg.get("email") or os.environ.get("GARMIN_EMAIL")
    password = garmin_cfg.get("password") or os.environ.get("GARMIN_PASSWORD")

    if not email or not password or str(email).startswith('$') or str(password).startswith('$'):
        print("Error: GARMIN_EMAIL and GARMIN_PASSWORD must be set.")
        print("Set them as environment variables or in config/config.yaml under garmin:")
        sys.exit(1)

    token_store = Path(
        garmin_cfg.get("token_store", "~/runbase/state/garmin_tokens")
    ).expanduser()

    from garminconnect import Garmin

    print(f"Authenticating with Garmin Connect as {email}...")

    garmin = Garmin(email, password, prompt_mfa=lambda: input("MFA code: ").strip())

    try:
        garmin.login()
    except Exception as e:
        print(f"Login failed: {e}")
        sys.exit(1)

    from runbase.ingest.garmin_sync import _save_session
    _save_session(garmin, token_store)

    print(f"\nTokens saved to {token_store}")
    print("You can now run: python -m runbase sync --garmin")


if __name__ == "__main__":
    main()
