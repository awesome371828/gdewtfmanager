FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl gnupg \
    && install -d /usr/share/postgresql-common/pgdg \
    && curl -fsSL https://www.postgresql.org/media/keys/ACCC4CF8.asc | gpg --dearmor -o /usr/share/postgresql-common/pgdg/apt.postgresql.org.gpg \
    && . /etc/os-release \
    && echo "deb [signed-by=/usr/share/postgresql-common/pgdg/apt.postgresql.org.gpg] https://apt.postgresql.org/pub/repos/apt ${VERSION_CODENAME}-pgdg main" > /etc/apt/sources.list.d/pgdg.list \
    && apt-get update \
    && apt-get install -y --no-install-recommends postgresql-client-17 \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN python -m pip install --upgrade pip \
    && python -m pip install -r requirements.txt

# Copy every local module imported by the application, including PostgreSQL adapter.
COPY iris.py moderation_full.py pear_core.py postgres_adapter.py reliability.py confirmations.py durable_runtime.py backup_worker.py schema.sql entrypoint.sh ./
COPY tests ./tests
COPY migrations ./migrations
# Static regression tests inspect the build recipe; include it in the test image.
COPY Dockerfile ./Dockerfile
COPY .dockerignore ./.dockerignore
RUN chmod +x /app/entrypoint.sh

# Fail the image build early on syntax errors and regression-test failures.
RUN python -m py_compile iris.py moderation_full.py pear_core.py postgres_adapter.py reliability.py confirmations.py durable_runtime.py backup_worker.py \
    && python -m unittest discover -s tests -v

# Run the bot directly. Docker's restart policy handles process/host restarts.
CMD ["/app/entrypoint.sh"]
