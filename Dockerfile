FROM python:3.14.8-slim-bookworm@sha256:48b13b003dda20b16f9442b8475aa05fe21bf6579a8c881db92ffb4d8fd20f83 AS builder

WORKDIR /build
COPY requirements-build.lock requirements.lock pyproject.toml README.md ./
COPY src/ ./src/
RUN python -m pip install --no-cache-dir --require-hashes -r requirements-build.lock \
    && python -m pip install --no-cache-dir --no-compile --require-hashes \
        --target /opt/application -r requirements.lock \
    && python -m pip install --no-cache-dir --no-compile --no-deps --no-build-isolation \
        --target /opt/application .

FROM python:3.14.8-slim-bookworm@sha256:48b13b003dda20b16f9442b8475aa05fe21bf6579a8c881db92ffb4d8fd20f83

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/opt/application \
    SENTINEL_CONFIG=/app/config.toml \
    SENTINEL_STATE_DIR=/var/lib/sentinel

RUN groupadd --gid 10001 sentinel \
    && useradd --uid 10001 --gid 10001 --no-create-home \
        --home-dir /app --shell /usr/sbin/nologin sentinel \
    && mkdir -p /app /var/lib/sentinel \
    && chown 10001:10001 /var/lib/sentinel

COPY --from=builder /opt/application/ /opt/application/
COPY config/idle.toml /app/config.toml
COPY config/idle.toml config/demo.toml config/sites.example.toml config/telegram.example.toml /app/config/
COPY config/checks.example.toml /app/config/checks.example.toml
COPY config/ingress.example.toml /app/config/ingress.example.toml
COPY demo/site_fixture.py /app/demo/site_fixture.py

USER 10001:10001
WORKDIR /app
STOPSIGNAL SIGTERM
HEALTHCHECK --interval=5s --timeout=3s --start-period=10s --retries=3 \
    CMD ["python", "-m", "sentinel_bot", "healthcheck"]
ENTRYPOINT ["python", "-m", "sentinel_bot"]
CMD ["run"]
