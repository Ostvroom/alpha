"""SQLite persistence for premium payment claims and monthly subscriptions."""
from __future__ import annotations

import sqlite3
import secrets
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import List, Optional, Tuple, Dict, Any

from app_paths import DATA_DIR, ensure_dirs

ensure_dirs()
DB_FILE = str(DATA_DIR / "payments.db")

REFERRAL_CODE_LEN = 8  # short, human-shareable (uppercase letters + digits)
ACCESS_CODE_LEN = 10   # early access unlock code (uppercase letters + digits)


def canonical_tx_hash(chain: str, tx_hash: str) -> str:
    """EVM hashes are case-insensitive; Solana signatures are case-sensitive."""
    if chain in ("eth_mainnet", "eth_base"):
        return tx_hash.lower()
    return tx_hash


def _conn():
    return sqlite3.connect(DB_FILE)


def init_db() -> None:
    conn = _conn()
    c = conn.cursor()
    c.execute(
        """
        CREATE TABLE IF NOT EXISTS payment_claims (
            tx_hash TEXT NOT NULL,
            chain TEXT NOT NULL,
            user_id INTEGER NOT NULL,
            guild_id INTEGER NOT NULL,
            tier TEXT NOT NULL,
            amount_raw TEXT NOT NULL,
            verified_at TEXT NOT NULL,
            PRIMARY KEY (tx_hash, chain)
        )
        """
    )
    c.execute(
        """
        CREATE TABLE IF NOT EXISTS premium_subscriptions (
            user_id INTEGER NOT NULL,
            guild_id INTEGER NOT NULL,
            expires_at TEXT NOT NULL,
            last_tx_hash TEXT,
            PRIMARY KEY (user_id, guild_id)
        )
        """
    )

    # --- Referrals / internal credit ledger ---
    c.execute(
        """
        CREATE TABLE IF NOT EXISTS referral_codes (
            user_id INTEGER PRIMARY KEY,
            code TEXT NOT NULL UNIQUE,
            created_at TEXT NOT NULL
        )
        """
    )
    c.execute(
        """
        CREATE TABLE IF NOT EXISTS referrals (
            referred_user_id INTEGER PRIMARY KEY,
            referrer_user_id INTEGER NOT NULL,
            code_used TEXT,
            guild_id INTEGER,
            source TEXT,
            invite_code TEXT,
            created_at TEXT NOT NULL,
            FOREIGN KEY(referrer_user_id) REFERENCES referral_codes(user_id)
        )
        """
    )
    # Migrations (best-effort)
    for stmt in (
        "ALTER TABLE referrals ADD COLUMN guild_id INTEGER",
        "ALTER TABLE referrals ADD COLUMN source TEXT",
        "ALTER TABLE referrals ADD COLUMN invite_code TEXT",
    ):
        try:
            c.execute(stmt)
        except sqlite3.OperationalError:
            pass
    c.execute(
        """
        CREATE TABLE IF NOT EXISTS referral_credits (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            referrer_user_id INTEGER NOT NULL,
            referred_user_id INTEGER NOT NULL,
            tx_hash TEXT NOT NULL,
            chain TEXT NOT NULL,
            tier TEXT NOT NULL,
            amount_raw TEXT NOT NULL,
            credited_raw TEXT NOT NULL,
            created_at TEXT NOT NULL,
            UNIQUE (tx_hash, chain, referrer_user_id)
        )
        """
    )

    # --- Website early access codes (issued after verified claim) ---
    c.execute(
        """
        CREATE TABLE IF NOT EXISTS access_codes (
            tx_hash TEXT NOT NULL,
            chain TEXT NOT NULL,
            user_id INTEGER NOT NULL,
            code TEXT NOT NULL UNIQUE,
            created_at TEXT NOT NULL,
            redeemed_at TEXT,
            redeemed_ip TEXT,
            PRIMARY KEY (tx_hash, chain)
        )
        """
    )
    conn.commit()
    conn.close()


def _code_alphabet() -> str:
    # Avoid ambiguous chars (0/O, 1/I)
    return "23456789ABCDEFGHJKLMNPQRSTUVWXYZ"


def _new_code() -> str:
    alpha = _code_alphabet()
    return "".join(secrets.choice(alpha) for _ in range(REFERRAL_CODE_LEN))


def _new_access_code() -> str:
    alpha = _code_alphabet()
    return "".join(secrets.choice(alpha) for _ in range(ACCESS_CODE_LEN))


def issue_access_code_for_claim(*, tx_hash: str, chain: str, user_id: int) -> str:
    """
    Idempotently issue an early-access code for a verified claim (tx_hash+chain).
    Returns the existing code if already issued.
    """
    init_db()
    conn = _conn()
    c = conn.cursor()
    c.execute(
        "SELECT code FROM access_codes WHERE tx_hash = ? AND chain = ? LIMIT 1",
        (canonical_tx_hash(chain, tx_hash), chain),
    )
    row = c.fetchone()
    if row and row[0]:
        conn.close()
        return str(row[0])

    created = datetime.now(timezone.utc).isoformat()
    for _ in range(40):
        code = _new_access_code()
        try:
            c.execute(
                """
                INSERT INTO access_codes (tx_hash, chain, user_id, code, created_at)
                VALUES (?, ?, ?, ?, ?)
                """,
                (canonical_tx_hash(chain, tx_hash), chain, int(user_id), code, created),
            )
            conn.commit()
            conn.close()
            return code
        except sqlite3.IntegrityError:
            continue
    conn.close()
    raise RuntimeError("Could not generate unique access code")


def redeem_access_code(code: str, *, ip: str = "") -> Tuple[bool, str, Optional[int]]:
    """
    Redeem an access code one-time (best-effort; repeated redeems return ok).
    Returns (ok, message, user_id).
    """
    s = (code or "").strip().upper()
    if not s:
        return False, "Missing code.", None
    init_db()
    conn = _conn()
    c = conn.cursor()
    c.execute("SELECT user_id, redeemed_at FROM access_codes WHERE code = ? LIMIT 1", (s,))
    row = c.fetchone()
    if not row:
        conn.close()
        return False, "Invalid access code.", None

    user_id = int(row[0])
    redeemed_at = row[1]
    if redeemed_at:
        conn.close()
        return True, "Code already redeemed.", user_id

    try:
        c.execute(
            "UPDATE access_codes SET redeemed_at = ?, redeemed_ip = ? WHERE code = ?",
            (datetime.now(timezone.utc).isoformat(), (ip or "")[:64] or None, s),
        )
        conn.commit()
    except Exception:
        pass
    conn.close()
    return True, "Redeemed.", user_id


def get_or_create_referral_code(user_id: int) -> str:
    """Stable code per user. Create if missing."""
    conn = _conn()
    c = conn.cursor()
    c.execute("SELECT code FROM referral_codes WHERE user_id = ?", (int(user_id),))
    row = c.fetchone()
    if row and row[0]:
        conn.close()
        return str(row[0])
    # create unique
    created = datetime.now(timezone.utc).isoformat()
    for _ in range(30):
        code = _new_code()
        try:
            c.execute(
                "INSERT INTO referral_codes (user_id, code, created_at) VALUES (?, ?, ?)",
                (int(user_id), code, created),
            )
            conn.commit()
            conn.close()
            return code
        except sqlite3.IntegrityError:
            continue
    conn.close()
    raise RuntimeError("Could not generate unique referral code")


def lookup_referrer_by_code(code: str) -> Optional[int]:
    s = (code or "").strip().upper()
    if not s:
        return None
    conn = _conn()
    c = conn.cursor()
    c.execute("SELECT user_id FROM referral_codes WHERE code = ? LIMIT 1", (s,))
    row = c.fetchone()
    conn.close()
    return int(row[0]) if row else None


def set_referral(
    referred_user_id: int,
    referrer_user_id: int,
    *,
    code_used: str = "",
    guild_id: Optional[int] = None,
    source: str = "",
    invite_code: str = "",
) -> Tuple[bool, str]:
    """
    Link referred user → referrer. One-time only.
    Returns (ok, message).
    """
    referred_user_id = int(referred_user_id)
    referrer_user_id = int(referrer_user_id)
    if referred_user_id == referrer_user_id:
        return False, "You cannot refer yourself."
    conn = _conn()
    c = conn.cursor()
    c.execute("SELECT 1 FROM referrals WHERE referred_user_id = ? LIMIT 1", (referred_user_id,))
    if c.fetchone() is not None:
        conn.close()
        return False, "Referral already set for this user."
    try:
        c.execute(
            """
            INSERT INTO referrals
            (referred_user_id, referrer_user_id, code_used, guild_id, source, invite_code, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                referred_user_id,
                referrer_user_id,
                (code_used or "").strip().upper()[:32],
                int(guild_id) if guild_id else None,
                (source or "").strip()[:20] or None,
                (invite_code or "").strip()[:64] or None,
                datetime.now(timezone.utc).isoformat(),
            ),
        )
        conn.commit()
    except sqlite3.IntegrityError as e:
        conn.close()
        return False, f"Could not set referral: {e}"
    conn.close()
    return True, "Referral saved."


def get_referrer(referred_user_id: int, guild_id: Optional[int] = None) -> Optional[int]:
    conn = _conn()
    c = conn.cursor()
    if guild_id:
        c.execute(
            "SELECT referrer_user_id FROM referrals WHERE referred_user_id = ? AND (guild_id = ? OR guild_id IS NULL) LIMIT 1",
            (int(referred_user_id), int(guild_id)),
        )
    else:
        c.execute("SELECT referrer_user_id FROM referrals WHERE referred_user_id = ? LIMIT 1", (int(referred_user_id),))
    row = c.fetchone()
    conn.close()
    return int(row[0]) if row else None


@dataclass
class ReferralRecord:
    referrer_user_id: int
    invite_code: Optional[str]
    source: Optional[str]
    created_at: Optional[str]


def get_referral_record(referred_user_id: int) -> Optional[ReferralRecord]:
    """Stored invite/referral row for this user (from join tracking), if any."""
    conn = _conn()
    c = conn.cursor()
    c.execute(
        """
        SELECT referrer_user_id, invite_code, source, created_at
        FROM referrals WHERE referred_user_id = ? LIMIT 1
        """,
        (int(referred_user_id),),
    )
    row = c.fetchone()
    conn.close()
    if not row:
        return None
    return ReferralRecord(
        referrer_user_id=int(row[0]),
        invite_code=(row[1] or None),
        source=(row[2] or None),
        created_at=(row[3] or None),
    )


def insert_referral_credit(
    *,
    referrer_user_id: int,
    referred_user_id: int,
    tx_hash: str,
    chain: str,
    tier: str,
    amount_raw: int,
    credited_raw: int,
) -> bool:
    """Idempotent credit insert. Returns True if inserted."""
    conn = _conn()
    c = conn.cursor()
    try:
        c.execute(
            """
            INSERT INTO referral_credits
            (referrer_user_id, referred_user_id, tx_hash, chain, tier, amount_raw, credited_raw, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                int(referrer_user_id),
                int(referred_user_id),
                canonical_tx_hash(chain, tx_hash),
                chain,
                tier,
                str(int(amount_raw)),
                str(int(credited_raw)),
                datetime.now(timezone.utc).isoformat(),
            ),
        )
        conn.commit()
        conn.close()
        return True
    except sqlite3.IntegrityError:
        conn.close()
        return False


def referral_balance_by_chain(user_id: int) -> Dict[str, int]:
    """Sum of credited_raw grouped by chain (raw units)."""
    conn = _conn()
    c = conn.cursor()
    c.execute(
        """
        SELECT chain, SUM(CAST(credited_raw AS INTEGER))
        FROM referral_credits
        WHERE referrer_user_id = ?
        GROUP BY chain
        """,
        (int(user_id),),
    )
    rows = c.fetchall()
    conn.close()
    out: Dict[str, int] = {}
    for ch, total in rows:
        try:
            out[str(ch)] = int(total or 0)
        except Exception:
            out[str(ch)] = 0
    return out


def claim_exists(tx_hash: str, chain: str) -> bool:
    conn = _conn()
    c = conn.cursor()
    c.execute(
        "SELECT 1 FROM payment_claims WHERE tx_hash = ? AND chain = ? LIMIT 1",
        (canonical_tx_hash(chain, tx_hash), chain),
    )
    row = c.fetchone()
    conn.close()
    return row is not None


def insert_claim(
    tx_hash: str,
    chain: str,
    user_id: int,
    guild_id: int,
    tier: str,
    amount_raw: str,
) -> None:
    conn = _conn()
    c = conn.cursor()
    c.execute(
        """
        INSERT INTO payment_claims (tx_hash, chain, user_id, guild_id, tier, amount_raw, verified_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (
            canonical_tx_hash(chain, tx_hash),
            chain,
            user_id,
            guild_id,
            tier,
            amount_raw,
            datetime.now(timezone.utc).isoformat(),
        ),
    )
    conn.commit()
    conn.close()


def claims_today_utc(user_id: int) -> int:
    start = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    conn = _conn()
    c = conn.cursor()
    c.execute(
        """
        SELECT COUNT(*) FROM payment_claims
        WHERE user_id = ? AND verified_at >= ?
        """,
        (user_id, start.isoformat()),
    )
    n = c.fetchone()[0]
    conn.close()
    return int(n)


@dataclass
class SubscriptionRow:
    user_id: int
    guild_id: int
    expires_at: datetime
    last_tx_hash: Optional[str]


def get_subscription(user_id: int, guild_id: int) -> Optional[SubscriptionRow]:
    conn = _conn()
    c = conn.cursor()
    c.execute(
        "SELECT user_id, guild_id, expires_at, last_tx_hash FROM premium_subscriptions WHERE user_id = ? AND guild_id = ?",
        (user_id, guild_id),
    )
    row = c.fetchone()
    conn.close()
    if not row:
        return None
    try:
        exp = datetime.fromisoformat(row[2].replace("Z", "+00:00"))
        if exp.tzinfo is None:
            exp = exp.replace(tzinfo=timezone.utc)
    except (ValueError, TypeError):
        return None
    return SubscriptionRow(
        user_id=row[0],
        guild_id=row[1],
        expires_at=exp,
        last_tx_hash=row[3],
    )


def upsert_monthly_subscription(
    user_id: int,
    guild_id: int,
    tx_hash: str,
    chain: str,
    monthly_days: int,
) -> datetime:
    now = datetime.now(timezone.utc)
    sub = get_subscription(user_id, guild_id)
    if sub and sub.expires_at > now:
        new_exp = sub.expires_at + timedelta(days=monthly_days)
    else:
        new_exp = now + timedelta(days=monthly_days)
    conn = _conn()
    c = conn.cursor()
    c.execute(
        """
        INSERT INTO premium_subscriptions (user_id, guild_id, expires_at, last_tx_hash)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(user_id, guild_id) DO UPDATE SET
            expires_at = excluded.expires_at,
            last_tx_hash = excluded.last_tx_hash
        """,
        (user_id, guild_id, new_exp.isoformat(), canonical_tx_hash(chain, tx_hash)),
    )
    conn.commit()
    conn.close()
    return new_exp


def delete_subscription(user_id: int, guild_id: int) -> None:
    conn = _conn()
    c = conn.cursor()
    c.execute(
        "DELETE FROM premium_subscriptions WHERE user_id = ? AND guild_id = ?",
        (user_id, guild_id),
    )
    conn.commit()
    conn.close()


def list_expired_subscriptions(now: Optional[datetime] = None) -> List[Tuple[int, int]]:
    """Return (user_id, guild_id) for subscriptions at or past expiry."""
    t = now or datetime.now(timezone.utc)
    conn = _conn()
    c = conn.cursor()
    c.execute(
        "SELECT user_id, guild_id FROM premium_subscriptions WHERE expires_at <= ?",
        (t.isoformat(),),
    )
    rows = c.fetchall()
    conn.close()
    return [(int(r[0]), int(r[1])) for r in rows]


def has_website_access(user_id: int) -> bool:
    """
    Website access gate helper:
    - Any redeemed access code grants access
    - Any active monthly subscription grants access
    - Any lifetime claim grants access
    """
    uid = int(user_id or 0)
    if uid <= 0:
        return False

    init_db()
    now_iso = datetime.now(timezone.utc).isoformat()
    conn = _conn()
    c = conn.cursor()

    # Redeemed early-access code (one-time claim flow)
    c.execute(
        """
        SELECT 1
        FROM access_codes
        WHERE user_id = ? AND redeemed_at IS NOT NULL
        LIMIT 1
        """,
        (uid,),
    )
    if c.fetchone():
        conn.close()
        return True

    # Active subscription (monthly, renewable)
    c.execute(
        """
        SELECT 1
        FROM premium_subscriptions
        WHERE user_id = ? AND expires_at > ?
        LIMIT 1
        """,
        (uid, now_iso),
    )
    if c.fetchone():
        conn.close()
        return True

    # Lifetime tier claim
    c.execute(
        """
        SELECT 1
        FROM payment_claims
        WHERE user_id = ? AND tier = 'lifetime'
        LIMIT 1
        """,
        (uid,),
    )
    ok = bool(c.fetchone())
    conn.close()
    return ok
