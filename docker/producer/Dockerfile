# Producer Dockerfile
FROM python:3.10-slim

# 작업 디렉토리 설정
WORKDIR /app

# Python 패키지 설치
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 프로젝트 파일 복사
COPY config ./config
COPY src/producer ./src/producer
COPY src/utils ./src/utils
COPY data ./data

# PYTHONPATH 설정
ENV PYTHONPATH=/app

# Producer 실행
CMD ["python", "-u", "src/producer/producer.py"]
