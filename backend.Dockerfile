# syntax=docker/dockerfile:1.4
# bookworm = stable Debian; "slim" without tag often tracks testing (trixie) — slower / heavier apt metadata.
FROM python:3.13-slim-bookworm

ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    DEBIAN_FRONTEND=noninteractive \
    POETRY_VIRTUALENVS_IN_PROJECT=true \
    SEARCH_LEVENSHTEIN_DIR=/app/search_bundle \
    UVICORN_RELOAD=0

EXPOSE 8000
WORKDIR /app

# Cache apt between builds
RUN --mount=type=cache,target=/var/cache/apt,sharing=locked \
    --mount=type=cache,target=/var/lib/apt/lists,sharing=locked \
    apt-get update \
    && apt-get install -y --no-install-recommends \
        libpq-dev gcc g++ build-essential python3-dev \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir poetry

# Dependencies layer
COPY pyproject.toml poetry.lock ./
RUN poetry install --no-root --no-interaction

# Levenshtein binary lives outside ./src so dev can mount ./src without breaking the binary
COPY src/scripts/searching_mechanism/levenshtein_length.cpp /app/search_bundle/
COPY src/scripts/searching_mechanism/titles_only.csv /app/search_bundle/
RUN g++ -O2 /app/search_bundle/levenshtein_length.cpp -o /app/search_bundle/levenshtein_length

# App code
COPY src ./src

CMD ["/app/.venv/bin/python", "-m", "src.microservices.recommendation_system_project"]
