FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1

# lxml wheels need no toolchain on slim; keep image lean.
RUN apt-get update && apt-get install -y --no-install-recommends curl ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY app ./app

EXPOSE 8000

# Single worker by design: the ingestion engine and the HTTP server share one
# event loop so polling, enrichment and fan-out stay in-process and fast.
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
