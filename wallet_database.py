import sqlite3
from typing import List, Dict, Optional
from datetime import datetime

from app_paths import DATA_DIR, ensure_dirs

ensure_dirs()
DB_FILE = str(DATA_DIR / "wallets.db")

def init_db():
    """Initialize the database table"""
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute('''
        CREATE TABLE IF NOT EXISTS tracked_wallets (
            address TEXT PRIMARY KEY,
            chain TEXT NOT NULL,
            label TEXT,
            added_at TEXT
        )
    ''')
    # Migration: add pfp_url column if it doesn't exist
    try:
        c.execute("ALTER TABLE tracked_wallets ADD COLUMN pfp_url TEXT")
    except sqlite3.OperationalError:
        pass  # Column already exists
    try:
        c.execute("ALTER TABLE tracked_wallets ADD COLUMN x_url TEXT")
    except sqlite3.OperationalError:
        pass
    conn.commit()
    conn.close()
    seed_eth_whales()

def add_wallet_db(address: str, chain: str, label: str = "", x_url: Optional[str] = None) -> bool:
    """Add a wallet. Returns True if added, False if duplicate."""
    try:
        conn = sqlite3.connect(DB_FILE)
        c = conn.cursor()
        c.execute(
            "INSERT INTO tracked_wallets (address, chain, label, added_at, x_url) VALUES (?, ?, ?, ?, ?)",
            (address, chain, label, datetime.now().isoformat(), x_url),
        )
        conn.commit()
        conn.close()
        return True
    except sqlite3.IntegrityError:
        conn.close()
        return False

def get_wallets_by_chain(chain: str) -> Dict[str, str]:
    """Get all wallets for a specific chain as a dict {address: label}"""
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("SELECT address, label FROM tracked_wallets WHERE chain = ?", (chain,))
    rows = c.fetchall()
    conn.close()
    return {row[0]: row[1] for row in rows}

def remove_wallet_db(address: str) -> bool:
    """Remove a wallet. Returns True if removed."""
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("DELETE FROM tracked_wallets WHERE address = ?", (address,))
    changes = conn.total_changes
    conn.commit()
    conn.close()
    return changes > 0

def get_all_wallets_db() -> List[tuple]:
    """Get all wallets for /list command: (address, chain, label, x_url)"""
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("SELECT address, chain, label, x_url FROM tracked_wallets")
    rows = c.fetchall()
    conn.close()
    return rows


def get_x_url(address: str) -> Optional[str]:
    """Optional X (Twitter) profile URL for a tracked wallet."""
    addr = address.strip().lower()
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("SELECT x_url FROM tracked_wallets WHERE lower(address) = ?", (addr,))
    row = c.fetchone()
    conn.close()
    return row[0] if row and row[0] else None


def upsert_eth_wallet(address: str, label: str, x_url: Optional[str] = None) -> None:
    """Insert or replace ETH whale row (used for seed + batch updates)."""
    addr = address.strip().lower()
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute(
        """
        INSERT INTO tracked_wallets (address, chain, label, added_at, x_url)
        VALUES (?, 'ETH', ?, ?, ?)
        ON CONFLICT(address) DO UPDATE SET
            label = excluded.label,
            x_url = excluded.x_url
        """,
        (addr, label, datetime.now().isoformat(), x_url),
    )
    conn.commit()
    conn.close()


# Seed ETH NFT whale wallets (labels + optional X profile links)
_ETH_WHALE_SEED: List[tuple] = [
    ("0x82dcba7a8bf3aa462040038ecb3d5d90901676e8", "aliu_eth", "https://x.com/MintedNFTs"),
    ("0x8a8035f056af830b7205c58c1dc037f826fc2b92", "serc.eth", None),
    ("0x239e9e51a7a881221c4cec228e1d093b5a434dc8", "sprnodes.eth", None),
    ("0xf06bed3f0dad7932d8d00fe48c36751f5c10be23", "paschamo.eth", None),
    ("0x3145a28b75e41c1a1ad664ca2e9c91d2e49c0b79", "0x3145...0b79", None),
    ("0x73da1af06106a7f3ac717ef0fd637177175d98b7", "br4ted", None),
    ("0xaf29ab7418516cc3f22e609dc783d75864ab545a", "ctrlplus.eth", None),
    ("0xb4d64772218d36f97974e8bb6ef0d01b026c9a14", "GORILLA", "https://x.com/CryptoGorillaYT"),
    ("0x54be3a794282c030b15e43ae2bb182e14c409c5e", "DINGALING", "https://x.com/dingalingts"),
    ("0x1b523dc90a79cf5ee5d095825e586e33780f7188", "JRNY", "https://x.com/JRNYcrypto"),
    ("0x442dccee68425828c106a3662014b4f131e3bd9b", "JIMMY", None),
    ("0x020ca66c30bec2c4fe3861a94e4db4a498a35872", "MACHI", None),
    ("0x1919db36ca2fa2e15f9000fd9cdc2edcf863e685", "PUNKS OTC", "https://x.com/punksOTC"),
    ("0x51787a2c56d710c68140bdadefd3a98bff96feb4", "SEEDPHRASE", "https://x.com/seedphrase"),
    ("0xed2ab4948ba6a909a7751dec4f34f303eb8c7236", "FRANKLIN", None),
    ("0x2238c8b16c36628b8f1f36486675c1e2a30debf1", "SETH", "https://x.com/btcismyname"),
    ("0xd387a6e4e84a6c86bd90c158c6028a58cc8ac459", "PRANKSY", "https://x.com/pranksy"),
    ("0xc6400a5584db71e41b0e5dfbdc769b54b91256cd", "PUNK6529", "https://x.com/punk6529"),
    ("0xdcae87821fa6caea05dbc2811126f4bc7ff73bd1", "OSF", "https://x.com/osf_rekt"),
    ("0x5ea9681c3ab9b5739810f8b91ae65ec47de62119", "GaryVee", "https://x.com/garyvee"),
    ("0x35860583266f6c6cad540ef07b4d36ec0d925916", "Fewocious", "https://x.com/fewocious"),
    ("0xc6b0562605d35ee710138402b878ffe6f2e23807", "beeple", "https://x.com/beeple"),
    ("0xf0d6999725115e3ead3d927eb3329d63afaec09b", "gmoney", "https://x.com/gmoneyNFT"),
    ("0x419beee486a63971332cee7170c2f675d92ac5d3", "xcopys", "https://x.com/XCOPYART"),
    ("0x8e6804337f8d774ce3eb4d4c12bad9dfab2f56ad", "anon alt", None),
    ("0xdbd47f66aa2f00b3db03397f260ce9728298c495", "TMA", "https://x.com/tma_420?s=21"),
    ("0x94ef56efad3cf722cd385bd6d5178c3063b83d1a", "0x94ef...3d1a", None),
]


def seed_eth_whales() -> None:
    """Upsert known ETH NFT whale wallets from the built-in seed list."""
    for addr, label, x_url in _ETH_WHALE_SEED:
        upsert_eth_wallet(addr, label, x_url)

def update_pfp_db(address: str, pfp_url: str) -> bool:
    """Update the profile picture URL for a wallet."""
    try:
        conn = sqlite3.connect(DB_FILE)
        c = conn.cursor()
        c.execute("UPDATE tracked_wallets SET pfp_url = ? WHERE address = ?", (pfp_url, address))
        conn.commit()
        conn.close()
        return True
    except Exception:
        return False

def get_pfp_url(address: str) -> Optional[str]:
    """Get the stored PFP URL for a wallet."""
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("SELECT pfp_url FROM tracked_wallets WHERE address = ?", (address,))
    row = c.fetchone()
    conn.close()
    return row[0] if row and row[0] else None
