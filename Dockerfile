# ── Build stage ────────────────────────────────────────────────
FROM docker.io/python:3.12 AS builder

RUN pip install --user pipenv
WORKDIR /build
ENV PIPENV_VENV_IN_PROJECT=1

COPY Pipfile Pipfile.lock* ./
RUN /root/.local/bin/pipenv sync

# ── Runtime stage ─────────────────────────────────────────────
FROM docker.io/python:3.12-slim-bookworm

WORKDIR /app

COPY --from=builder /build/.venv/ /venv/
ENV PATH="/venv/bin:$PATH"

COPY src/ src/
COPY config.yaml .

RUN useradd -m -u 1001 -g 33 straker
USER straker

CMD ["python", "-m", "src.main"]
