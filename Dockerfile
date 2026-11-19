FROM python:3.15-slim-trixie@sha256:65a65fc7b30b06cb6df4930fd4650697cb23f4c2698ab81fc87e4b031bc36928 AS base

# Pin the Debian security fix for CVE-2026-85091 and CVE-2026-27171.
RUN apt-get update && \
    apt-get install --yes --no-install-recommends zlib1g=1:1.3.dfsg+really1.3.1-1+deb13u1 && \
    rm -rf /var/lib/apt/lists/*

FROM base AS dependencies
ENV PIP_DISABLE_PIP_VERSION_CHECK=1
WORKDIR /build
COPY requirements.lock .
RUN python -m venv /opt/reliability && \
    /opt/reliability/bin/python -m pip install --no-cache-dir --only-binary=:all: --require-hashes -r requirements.lock && \
    /opt/reliability/bin/python -m pip check && \
    /opt/reliability/bin/python -m pip uninstall -y pip

FROM base AS runtime
ENV PATH="/opt/reliability/bin:${PATH}" \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    HOME=/tmp \
    MAINTENANCE_EXCLUSIVE_DB=1 \
    MAINTENANCE_MODE=simulation \
    MAINTENANCE_DB=/var/lib/reliability/cluster.sqlite3
WORKDIR /srv/reliability
RUN groupadd --gid 10001 reliability && \
    useradd --uid 10001 --gid 10001 --no-create-home --shell /usr/sbin/nologin reliability && \
    install -d -o 10001 -g 10001 -m 0750 /var/lib/reliability && \
    python -m pip uninstall -y pip
COPY --from=dependencies /opt/reliability /opt/reliability
COPY app ./app
USER 10001:10001
VOLUME ["/var/lib/reliability"]
EXPOSE 8000
STOPSIGNAL SIGTERM
HEALTHCHECK --interval=15s --timeout=4s --start-period=15s --retries=3 \
    CMD ["python", "-c", "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8000/ready', timeout=3).close()"]
CMD ["python", "-m", "uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "1", "--timeout-graceful-shutdown", "20", "--no-access-log"]
