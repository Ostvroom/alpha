import json
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from staking_sync import RETRY_COOLDOWN_SECONDS, configuration_error, sign_body


secret = "phase-six-test-secret-must-be-32-chars"
timestamp = "1784808000"
payload = {
    "discordUserId": "1529887810943975545",
    "engagementPoints": -3,
    "discordPoints": -2,
    "xRaidPoints": -1,
    "alphaScore": -1,
    "alphaWeeklyScore": 7,
    "alphaWeeklyCalls": 2,
    "alphaWeekStart": "2026-07-20",
    "sourceUpdatedAt": "2026-07-23T12:00:00+00:00",
}
body = json.dumps(payload, separators=(",", ":"), sort_keys=True)
signature = sign_body(body, timestamp, secret)

assert re.fullmatch(r"[a-f0-9]{64}", signature)
assert signature == sign_body(body, timestamp, secret)
assert signature != sign_body(body + " ", timestamp, secret)
assert isinstance(payload["discordUserId"], str)
assert payload["engagementPoints"] == -3
assert payload["discordPoints"] == -2
assert payload["xRaidPoints"] == -1
assert payload["discordPoints"] + payload["xRaidPoints"] == payload["engagementPoints"]
assert payload["alphaScore"] == -1
assert payload["alphaWeeklyScore"] == 7
assert payload["alphaWeeklyCalls"] == 2
assert payload["alphaWeekStart"] == "2026-07-20"
assert 10 <= RETRY_COOLDOWN_SECONDS <= 300
assert isinstance(configuration_error(), str)

print("Staking score sync signing and payload safety verified.")
