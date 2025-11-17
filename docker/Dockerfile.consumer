# Consumer PostgreSQL Dockerfile
FROM python:3.10-slim

# 작업 디렉토리 설정
WORKDIR /app

# Python 패키지 설치
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 프로젝트 파일 복사
COPY config ./config
COPY src/consumer ./src/consumer
COPY src/utils ./src/utils

# PYTHONPATH 설정
ENV PYTHONPATH=/app

# Consumer 실행
CMD ["python", "-u", "src/consumer/consumer_postgres.py"]
