"""
setup_accounts.py — Import X accounts from vendor list, connect via proxy, save cookies.

Vendor format (one account per line):
    login:password:email:email_password:Token

Usage:
    python scripts/setup_accounts.py accounts_import.txt [--proxies proxies.txt] [--out-dir .]

What it does:
    1. Parses each line → username / password / email / auth_token
    2. Connects through a rotating proxy (from proxies.txt or PROXIES env var)
    3. Uses auth_token to authenticate (no full login flow needed)
    4. Saves cookies_<username>.json in --out-dir
    5. Appends new entries to accounts.json (skips duplicates)
"""

import argparse
import asyncio
import json
import os
import sys

# Allow running from project root or scripts/
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from twikit import Client


# ── Helpers ──────────────────────────────────────────────────────────────────

def load_proxies(path: str | None) -> list[str]:
    """Load proxy list from file or PROXIES env var (one per line, http://user:pass@host:port)."""
    sources = []
    if path and os.path.exists(path):
        with open(path, encoding="utf-8") as f:
            sources = [l.strip() for l in f if l.strip() and not l.startswith("#")]
    if not sources:
        raw = os.getenv("PROXIES", "")
        sources = [p.strip() for p in raw.split("\n") if p.strip()]
    return sources


def parse_vendor_line(line: str) -> dict | None:
    """Parse login:password:email:email_password:Token → dict or None."""
    line = line.strip()
    if not line or line.startswith("#"):
        return None
    parts = line.split(":")
    if len(parts) < 5:
        print(f"  SKIP (bad format, expected 5 fields): {line[:60]}")
        return None
    username = parts[0].strip()
    password = parts[1].strip()
    # email may contain ':'  (rare but possible) — join middle parts
    # last field is always Token, second-to-last is email_password
    token = parts[-1].strip()
    email_password = parts[-2].strip()
    email = ":".join(parts[2:-2]).strip()
    if not username or not token:
        print(f"  SKIP (missing username or token): {line[:60]}")
        return None
    return {
        "username": username,
        "password": password,
        "email": email,
        "email_password": email_password,
        "auth_token": token,
    }


def load_accounts_json(path: str) -> list[dict]:
    if os.path.exists(path):
        try:
            with open(path, encoding="utf-8") as f:
                data = json.load(f)
            return data if isinstance(data, list) else []
        except Exception:
            return []
    return []


def save_accounts_json(path: str, accounts: list[dict]) -> None:
    # Only write the fields the bot uses — drop email_password
    clean = [
        {k: v for k, v in acc.items() if k != "email_password"}
        for acc in accounts
    ]
    with open(path, "w", encoding="utf-8") as f:
        json.dump(clean, f, indent=4)


# ── Core: connect + save cookies ─────────────────────────────────────────────

async def setup_account(acc: dict, proxy: str | None, out_dir: str) -> bool:
    username = acc["username"]
    cookie_path = os.path.join(out_dir, f"cookies_{username}.json")

    if os.path.exists(cookie_path):
        print(f"  [{username}] cookies already exist — skipping login, keeping file")
        return True

    try:
        client = Client("en-US", proxy=proxy or None)
        client.set_cookies({"auth_token": acc["auth_token"]})
        client.save_cookies(cookie_path)
        print(f"  [{username}] OK — cookies saved to {cookie_path}")
        return True
    except Exception as e:
        print(f"  [{username}] ERROR — {e}")
        return False


async def run(args):
    proxies = load_proxies(args.proxies)
    if not proxies:
        print("WARN: No proxies loaded — connecting without proxy (not recommended for prod)")

    # Parse vendor file
    with open(args.input, encoding="utf-8") as f:
        lines = f.readlines()

    parsed = [r for l in lines if (r := parse_vendor_line(l))]
    if not parsed:
        print("ERROR: No valid accounts parsed from input file.")
        sys.exit(1)
    print(f"Parsed {len(parsed)} account(s) from {args.input}")

    # Load existing accounts.json
    accounts_path = os.path.join(args.out_dir, "accounts.json")
    existing = load_accounts_json(accounts_path)
    existing_usernames = {a["username"].lower() for a in existing}

    # Connect and save cookies
    proxy_idx = 0
    ok, skip, fail = 0, 0, 0

    for acc in parsed:
        proxy = proxies[proxy_idx % len(proxies)] if proxies else None
        proxy_idx += 1
        print(f"\n→ {acc['username']} (proxy: {proxy or 'none'})")

        success = await setup_account(acc, proxy, args.out_dir)
        if success:
            ok += 1
        else:
            fail += 1
            if args.skip_failed:
                continue

        # Add to accounts.json if not already there
        if acc["username"].lower() not in existing_usernames:
            existing.append(acc)
            existing_usernames.add(acc["username"].lower())
        else:
            skip += 1
            print(f"  [{acc['username']}] already in accounts.json — skipped")

        await asyncio.sleep(args.delay)

    save_accounts_json(accounts_path, existing)
    print(f"\nDone. OK={ok}  failed={fail}  already_existed={skip}")
    print(f"accounts.json saved to {accounts_path}")


# ── CLI ───────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Import X accounts from vendor list")
    parser.add_argument("input", help="Vendor account list (login:pass:email:email_pass:token)")
    parser.add_argument("--proxies", default="proxies.txt", help="Proxy list file (default: proxies.txt)")
    parser.add_argument("--out-dir", default=".", help="Where to write cookies_*.json + accounts.json (default: .)")
    parser.add_argument("--delay", type=float, default=2.0, help="Seconds between accounts (default: 2)")
    parser.add_argument("--skip-failed", action="store_true", help="Skip failed accounts instead of stopping")
    args = parser.parse_args()

    if not os.path.exists(args.input):
        print(f"ERROR: Input file not found: {args.input}")
        sys.exit(1)

    asyncio.run(run(args))


if __name__ == "__main__":
    main()
