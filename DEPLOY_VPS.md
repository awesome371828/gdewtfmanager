# Permanent GDE.WTF Manager hosting (VPS + Docker)

This repository runs the GDE.WTF Telegram manager from `iris.py` using PostgreSQL. The Docker image compiles the Python modules and runs the regression tests before the image can be built.

## Requirements

- Linux VPS with Docker Engine and the Docker Compose plugin.
- PostgreSQL reachable from the bot host.
- Telegram bot token and GDE.WTF external API key.

## 1. Get this repository

```bash
git clone https://github.com/awesome371828/gdewtfmanager.git
cd gdewtfmanager
```

## 2. Configure secrets locally

Create `.env` in the repository root. Do not commit it:

```dotenv
TELEGRAM_BOT_TOKEN=YOUR_TELEGRAM_BOT_TOKEN
DATABASE_URL=YOUR_POSTGRESQL_CONNECTION_STRING
BOT_OWNER_ID=6652898792
GDEWTF_API_KEY=YOUR_GDEWTF_EXTERNAL_API_KEY
# Optional:
# GDEWTF_API_BASE=https://map.gde.wtf/api/ext
```

Restrict access to the file:

```bash
chmod 600 .env
```

## 3. Prepare the database

Review and apply `schema.sql` to the PostgreSQL database specified by `DATABASE_URL`. The schema is designed for additive/idempotent updates, but make a backup before applying changes to a database with existing data.

The bot uses PostgreSQL directly through `postgres_adapter.py`; it does not use `SUPABASE_URL` or `SUPABASE_SERVICE_KEY`.

## 4. Build and start

```bash
sudo docker compose up -d --build
sudo docker compose logs --tail=150 -f
```

The Docker build runs `py_compile` and `python -m unittest discover -s tests -v`. If either check fails, the image build stops instead of deploying code that failed its regression checks.

## 5. Verify and update

Check container status:

```bash
sudo docker compose ps
sudo docker inspect -f '{{.State.Status}} | restarts={{.RestartCount}}' pear-cm
```

After a code update:

```bash
git pull
sudo docker compose up -d --build
```

## Important: exactly one active poller

Only one running instance may use the same Telegram bot token. Stop old deployments and local copies before starting a new one. A Telegram polling conflict (409) is treated as a fatal configuration conflict and exits with code 23; do not run multiple replicas with the same token.

## Limits

The container can recover from transient process failures, but this does not make Telegram, PostgreSQL, or the external GDE.WTF API infallible. Check runtime logs and verify real commands after deployment. Never put bot tokens, database passwords, or API keys into source code or commits.
