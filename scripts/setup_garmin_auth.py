#!/usr/bin/env python3
"""One-time Garmin Connect authentication setup.

Authenticates with Garmin Connect and saves session tokens via garth
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

    if not email or not password:
        print("Error: GARMIN_EMAIL and GARMIN_PASSWORD must be set.")
        print("Set them as environment variables or in config/config.yaml under garmin:")
        sys.exit(1)

    token_store = Path(
        garmin_cfg.get("token_store", "~/runbase/state/garmin_tokens")
    ).expanduser()

    from garminconnect import Garmin

    print(f"Authenticating with Garmin Connect as {email}...")

    garmin = Garmin(email, password)

    try:
        garmin.login()
    except Exception as e:
        error_str = str(e).lower()
        if "mfa" in error_str or "verification" in error_str or "two-factor" in error_str:
            print("\nMFA required. Enter the code from your authenticator app or email:")
            mfa_code = input("MFA code: ").strip()
            try:
                garmin.login(mfa_code)
            except Exception as e2:
                print(f"Error with MFA: {e2}")
                sys.exit(1)
        else:
            print(f"Login failed: {e}")
            sys.exit(1)

    # Save session tokens via garth
    token_store.mkdir(parents=True, exist_ok=True)
    garmin.garth.dump(str(token_store))

    print(f"\nTokens saved to {token_store}")
    print("You can now run: python -m runbase sync --garmin")


if __name__ == "__main__":
    main()
