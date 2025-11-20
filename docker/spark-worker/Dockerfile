FROM apache/spark:3.4.2-python3

USER root

WORKDIR /app

# 소스 코드 복사
COPY . /app

# Python symlink 생성
RUN ln -s /usr/bin/python3 /usr/bin/python

# Python 패키지 설치
RUN pip install --no-cache-dir \
    python-dotenv==1.0.0 \
    psycopg2-binary==2.9.9 \
    requests

# Spark 환경변수 설정
ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$PATH
ENV SPARK_WORKER_WEBUI_PORT=8080

# Worker는 Master와 연결 (시작 시 master URL 전달)
# 기본값: spark://spark-master:7077
CMD ["/opt/spark/bin/spark-class", "org.apache.spark.deploy.worker.Worker", "spark://spark-master:7077"]
