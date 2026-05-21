"""
Send DMs from the terminal using your Discord bot token.

Uses .env: DISCORD_TOKEN or DISCORD_BOT_TOKEN (or DM_BOT_TOKEN for a dedicated DM bot).

Examples:
  python scripts/send_dm.py --user 123456789012345678 --message "Hey, quick update..."
  python scripts/send_dm.py --users 111,222,333 --message "Thanks for joining!"
  python scripts/send_dm.py --users-file ids.txt --file message.txt
  python scripts/send_dm.py --guild 987654321 --role 111222333 --message "Event tonight"
  python scripts/send_dm.py --guild 987654321 --role 111222333 --message "Hi" --dry-run
  python scripts/send_dm.py --user 123 --message "Test" --yes --delay 1.5

ids.txt: one Discord user ID per line (# comments allowed).
"""
from __future__ import annotations

import argparse
import asyncio
import os
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from dotenv import load_dotenv

load_dotenv(_ROOT / ".env", override=True)


def _parse_user_ids(raw: str) -> list[int]:
    out: list[int] = []
    for part in (raw or "").replace(";", ",").split(","):
        part = part.strip()
        if not part:
            continue
        out.append(int(part))
    return out


def _load_ids_file(path: Path) -> list[int]:
    ids: list[int] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        ids.append(int(line.split()[0]))
    return ids


def _resolve_message(args: argparse.Namespace) -> str:
    if args.file:
        text = Path(args.file).read_text(encoding="utf-8").strip()
        if not text:
            raise SystemExit(f"Message file is empty: {args.file}")
        return text
    msg = (args.message or "").strip()
    if not msg:
        raise SystemExit("Provide --message or --file")
    return msg


async def _collect_targets(client, args: argparse.Namespace) -> list[int]:
    import discord

    targets: list[int] = []
    seen: set[int] = set()

    def add(uid: int) -> None:
        if uid not in seen:
            seen.add(uid)
            targets.append(uid)

    if args.user:
        add(int(args.user))
    if args.users:
        for uid in _parse_user_ids(args.users):
            add(uid)
    if args.users_file:
        for uid in _load_ids_file(Path(args.users_file)):
            add(uid)

    if args.guild and args.role:
        guild = client.get_guild(int(args.guild))
        if guild is None:
            guild = await client.fetch_guild(int(args.guild))
        role = guild.get_role(int(args.role))
        if role is None:
            roles = await guild.fetch_roles()
            role = discord.utils.get(roles, id=int(args.role))
        if role is None:
            raise SystemExit(f"Role {args.role} not found in guild {args.guild}")
        try:
            await guild.chunk()
        except Exception as e:
            print(f"Warning: guild.chunk() failed ({e}). Member list may be incomplete.", flush=True)
        members = [m for m in guild.members if role in m.roles and not m.bot]
        if not members:
            raise SystemExit(
                f"No members cached for role {role.name} ({role.id}). "
                "Enable **Server Members Intent** for the bot in the Discord Developer Portal, "
                "re-invite the bot if needed, then retry."
            )
        for member in members:
            add(member.id)

    if not targets:
        raise SystemExit(
            "No recipients. Use --user, --users, --users-file, or --guild + --role."
        )
    return targets


async def _run(args: argparse.Namespace) -> None:
    import discord

    token = (
        (args.token or "").strip()
        or (os.getenv("DM_BOT_TOKEN") or "").strip()
        or (os.getenv("DISCORD_TOKEN") or "").strip()
        or (os.getenv("DISCORD_BOT_TOKEN") or "").strip()
    )
    if not token:
        raise SystemExit(
            "Set DISCORD_TOKEN, DISCORD_BOT_TOKEN, or DM_BOT_TOKEN in .env "
            "(or pass --token)."
        )

    message = _resolve_message(args)
    delay = max(0.5, float(args.delay))

    intents = discord.Intents.default()
    if args.guild and args.role:
        intents.members = True
    client = discord.Client(intents=intents)

    @client.event
    async def on_ready():
        try:
            print(f"Logged in as {client.user} ({client.user.id})", flush=True)
            user_ids = await _collect_targets(client, args)
            print(f"Recipients: {len(user_ids)}", flush=True)
            if args.dry_run:
                for uid in user_ids[:50]:
                    print(f"  - {uid}", flush=True)
                if len(user_ids) > 50:
                    print(f"  ... +{len(user_ids) - 50} more", flush=True)
                return

            preview = message if len(message) <= 120 else message[:117] + "..."
            print(f"Message preview: {preview!r}", flush=True)

            if not args.yes:
                confirm = input(f"Send to {len(user_ids)} user(s)? [y/N]: ").strip().lower()
                if confirm not in ("y", "yes"):
                    print("Cancelled.", flush=True)
                    return

            ok = 0
            fail = 0
            for i, uid in enumerate(user_ids, 1):
                try:
                    user = client.get_user(uid) or await client.fetch_user(uid)
                    await user.send(message)
                    ok += 1
                    print(f"[{i}/{len(user_ids)}] OK → {user} ({uid})", flush=True)
                except discord.Forbidden:
                    fail += 1
                    print(f"[{i}/{len(user_ids)}] SKIP (DMs closed) → {uid}", flush=True)
                except Exception as e:
                    fail += 1
                    print(f"[{i}/{len(user_ids)}] FAIL → {uid}: {e}", flush=True)
                if i < len(user_ids):
                    await asyncio.sleep(delay)

            print(f"Done. Sent: {ok} | Failed: {fail}", flush=True)
        finally:
            await client.close()

    try:
        await client.start(token)
    except KeyboardInterrupt:
        await client.close()


def main() -> None:
    p = argparse.ArgumentParser(description="Send Discord DMs from the terminal.")
    p.add_argument("--message", "-m", help="DM body text")
    p.add_argument("--file", "-f", help="Read message body from a text file")
    p.add_argument("--user", type=int, help="Single Discord user ID")
    p.add_argument("--users", help="Comma-separated user IDs")
    p.add_argument("--users-file", help="File with one user ID per line")
    p.add_argument("--guild", type=int, help="Guild ID (with --role)")
    p.add_argument("--role", type=int, help="Role ID — DM all non-bot members")
    p.add_argument(
        "--token",
        help="Bot token override (default: DM_BOT_TOKEN, then DISCORD_TOKEN)",
    )
    p.add_argument("--delay", type=float, default=2.0, help="Seconds between DMs (default 2)")
    p.add_argument("--dry-run", action="store_true", help="List targets only")
    p.add_argument("--yes", "-y", action="store_true", help="Skip confirmation prompt")
    args = p.parse_args()
    asyncio.run(_run(args))


if __name__ == "__main__":
    main()
