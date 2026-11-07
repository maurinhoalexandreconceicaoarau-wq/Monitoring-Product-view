FROM python:3.12-slim

WORKDIR /app

COPY requirements-dashboard.txt .
RUN python -m pip install --no-cache-dir -r requirements-dashboard.txt

COPY app.py dashboard_backend.py ./
COPY pipeline ./pipeline

ENV PYTHONUNBUFFERED=1

EXPOSE 8501
