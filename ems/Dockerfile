FROM python:3.12-slim
WORKDIR /app
RUN apt-get update && apt-get install -y --no-install-recommends curl && rm -rf /var/lib/apt/lists/*
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
HEALTHCHECK --interval=30s --timeout=5s --retries=3 \
    CMD test $(( $(date +%s) - $(stat -c %Y /var/log/ems/heartbeat 2>/dev/null || echo 0) )) -lt 120
CMD ["python", "-u", "main.py"]
