FROM python:3.11-slim

WORKDIR /app
RUN pip install --no-cache-dir "fastapi==0.115.0" "uvicorn==0.30.6" "psycopg[binary]==3.2.3"
COPY main.py .

USER nobody
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
