FROM python:3.11-slim

WORKDIR /app

ARG PIP_INDEX_URL=https://pypi.org/simple
ARG PIP_TRUSTED_HOSTS=

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && update-ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.lock .
RUN set -e; \
    TRUST_ARGS=""; \
    for host in $PIP_TRUSTED_HOSTS; do \
        TRUST_ARGS="$TRUST_ARGS --trusted-host $host"; \
    done; \
    pip install --no-cache-dir --index-url "$PIP_INDEX_URL" $TRUST_ARGS -r requirements.lock

COPY . .

EXPOSE 8000
