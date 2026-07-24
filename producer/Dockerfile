FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY tfl_producer.py .

# Unbuffered so docker logs shows the per-poll metrics line immediately
ENV PYTHONUNBUFFERED=1

CMD ["python", "tfl_producer.py"]
