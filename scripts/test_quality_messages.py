import sys
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import engagement
from engagement import is_quality_message


assert not is_quality_message("gm")
assert not is_quality_message("hello everyone")
assert not is_quality_message("https://example.com @everyone")
assert not is_quality_message("!help show all commands please")
assert not is_quality_message("aaaaaaa this this this")
assert is_quality_message("Ethereum volume is improving, but I would wait for confirmation.")
assert is_quality_message("This NFT collection has stronger holders than it had last week.")


class FakeAuthor:
    id = 123456789012345678
    bot = False
    created_at = datetime.now(timezone.utc) - timedelta(days=365)
    joined_at = datetime.now(timezone.utc) - timedelta(days=30)


class FakeChannel:
    id = 987654321012345678
    name = "hidden-testing"
    parent = None


class FakeMessage:
    guild = object()
    author = FakeAuthor()
    channel = FakeChannel()

    def __init__(self, index: int):
        self.id = 1000 + index
        self.content = (
            f"Quality test message number {index} discusses NFT community strategy clearly."
        )


with tempfile.TemporaryDirectory() as temp_dir:
    engagement.DB_PATH = Path(temp_dir) / "engagement.db"
    engagement.init_db()
    results = [engagement.award_message(FakeMessage(i)) for i in range(10)]
    assert all(ok for ok, _reason, _count in results), results
    assert engagement.get_quality_message_counts(FakeAuthor.id) == (10, 0)

print("Discord quality-message filtering and rapid distinct counting verified.")
