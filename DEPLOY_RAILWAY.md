# GDE.WTF Manager: Docker deployment on Railway

This repository runs `iris.py` with PostgreSQL, not the older Pear CM/Supabase configuration.

## Configure the service

1. Create a Railway service from `awesome371828/gdewtfmanager`, branch `main`.
2. Use the repository's `Dockerfile` for the build. Do not override its start command: the Dockerfile starts `entrypoint.sh`, which handles unexpected process exits and stops on Telegram polling conflicts.
3. Configure these variables in Railway's service settings:
   - `TELEGRAM_BOT_TOKEN` — Telegram bot token.
   - `DATABASE_URL` — PostgreSQL connection string.
   - `BOT_OWNER_ID` — Telegram ID of the bot owner/operator.
   - `GDEWTF_API_KEY` — key for `https://map.gde.wtf/api/ext`.
   - Optional: `GDEWTF_API_BASE` if the API base URL differs.
4. Apply `schema.sql` to the PostgreSQL database referenced by `DATABASE_URL` before starting the bot.
5. Ensure the service has exactly one active replica using this Telegram token. Stop other hosts/deployments first.
6. After deployment, inspect logs for successful polling startup and test `!ping`, `!модераторы`, and a safe read-only command before testing moderation actions.

## Notes

- The Docker build runs syntax compilation and regression tests; a failing check blocks the image build.
- The app uses PostgreSQL through `postgres_adapter.py`. Do not configure legacy `SUPABASE_URL` or `SUPABASE_SERVICE_KEY` variables for this repository.
- A Telegram 409 polling conflict means another process already owns the token. It is a deployment problem, not something to solve by starting more copies.
- Restart policies can recover from transient crashes but cannot repair invalid secrets, a broken database schema, or an unavailable external API.
