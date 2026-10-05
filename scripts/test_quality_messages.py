import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from engagement import is_quality_message


assert not is_quality_message("gm")
assert not is_quality_message("hello everyone")
assert not is_quality_message("https://example.com @everyone")
assert not is_quality_message("!help show all commands please")
assert not is_quality_message("aaaaaaa this this this")
assert is_quality_message("Ethereum volume is improving, but I would wait for confirmation.")
assert is_quality_message("This NFT collection has stronger holders than it had last week.")

print("Discord quality-message filtering verified.")
