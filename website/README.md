# nerdsalpha.xyz website (decoupled from the bot)

This FastAPI app (`website_server.py`) used to run **in-process** inside the
Discord bot (started as a daemon thread from `main.py`). It served the public
nerdsalpha.xyz site (projects, KOL alerts, daily finds, feed, Discord OAuth)
and mirrored engagement/points into Supabase.

It has been **removed from the bot's runtime**:

- The bot no longer imports or starts this module.
- `main.py` now runs only a tiny stdlib health endpoint so Render's
  web-service port check stays green — no FastAPI / uvicorn / Supabase weight
  in the bot process.
- Engagement points and Alpha Score reach the **new staking website** through
  `staking_sync.py` (HMAC-signed HTTP push). The bot never talks to Supabase
  directly anymore; the append-only ledger in `engagement.db` is the source of
  truth.

## If you want to run this site again

It needs to be hosted **separately** (its own service/repo), not inside the
bot. It still expects the old env vars (`SUPABASE_URL`,
`SUPABASE_SERVICE_ROLE_KEY`, `DISCORD_OAUTH_*`, etc.) and shares the SQLite
databases under `DATA_DIR`, so co-locating it with the bot's data disk would
be required for the project/feed pages to work.

Kept in the repo (rather than deleted) so nothing is lost and it can be
revived or ported if needed.
