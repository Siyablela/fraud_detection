# One image runs every process; the command selects which:
#   API (default)   uvicorn app.main:app
#   worker          python -m app.worker
#   batch job       python -m app.batch_worker
#   migrations      alembic upgrade head

ARG PYTHON_IMAGE=python:3.14.10-slim-bookworm@sha256:516b51cb9db2ef061e465c803e50f64361b82cefd91e525b889b967b83e83527

# --- build: resolve hashed, pinned dependencies into a self-contained venv ------
FROM ${PYTHON_IMAGE} AS build
ENV PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1
RUN python -m venv /opt/venv
COPY requirements.lock /tmp/requirements.lock
RUN /opt/venv/bin/pip install --require-hashes --only-binary=:all: -r /tmp/requirements.lock \
    && /opt/venv/bin/pip uninstall --yes pip

# --- runtime: no compilers, no pip, non-root ------------------------------------
FROM ${PYTHON_IMAGE} AS runtime
ENV PATH=/opt/venv/bin:$PATH \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1
# Nothing installs packages at runtime, and pip's vendored libraries (urllib3,
# msgpack, setuptools) carry CVEs that would fail the image scan.
RUN python -m pip uninstall --yes --root-user-action=ignore pip \
    && groupadd --system --gid 10001 fraud \
    && useradd --system --uid 10001 --gid fraud --no-create-home --shell /usr/sbin/nologin fraud
WORKDIR /app
COPY --from=build /opt/venv /opt/venv
COPY alembic.ini ./
COPY alembic ./alembic
COPY app ./app

# Numeric UID so Kubernetes runAsNonRoot can verify it without resolving names.
USER 10001:10001
EXPOSE 8000 9100
HEALTHCHECK --interval=10s --timeout=5s --start-period=15s --retries=3 \
    CMD ["python", "-m", "app.healthcheck", "api"]
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000", "--no-server-header", "--timeout-graceful-shutdown", "20"]
