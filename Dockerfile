FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN python -m pip install --upgrade pip \
    && python -m pip install -r requirements.txt

# Copy every local module imported by the application, including PostgreSQL adapter.
COPY iris.py moderation_full.py pear_core.py postgres_adapter.py schema.sql entrypoint.sh ./
COPY tests ./tests
# Static regression tests inspect the build recipe; include it in the test image.
COPY Dockerfile ./Dockerfile
RUN chmod +x /app/entrypoint.sh

# Fail the image build early on syntax errors and regression-test failures.
RUN python -m py_compile iris.py moderation_full.py pear_core.py postgres_adapter.py \
    && python -m unittest discover -s tests -v

# Run the bot directly. Docker's restart policy handles process/host restarts.
CMD ["/app/entrypoint.sh"]
