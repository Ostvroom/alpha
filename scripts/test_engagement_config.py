"""Ensure the admin command renders the current quality-chat reward rules."""

import asyncio
import sys
from pathlib import Path
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from engagement_commands import EngagementCommands


class FakeContext:
    guild = object()

    def __init__(self):
        self.embeds = []

    async def send(self, *, embed, **_kwargs):
        self.embeds.append(embed)


async def verify():
    ctx = FakeContext()
    cog = EngagementCommands(SimpleNamespace())
    await EngagementCommands.engage_config.callback(cog, ctx)
    assert len(ctx.embeds) == 1
    fields = {field.name: field.value for field in ctx.embeds[0].fields}
    assert "10 accepted messages = 1 $V3" in fields["Quality chat rewards"]
    assert "10 accepted messages = 5 $V3" in fields["Quality chat rewards"]
    assert "Boosted channels" in fields


asyncio.run(verify())
print("Discord engagement config command verified.")
