# Spark Batch Processing Image

FROM apache/spark:3.4.2-python3

USER root

WORKDIR /app

# 소스 코드 복사
COPY . /app

# Python symlink 생성 (spark가 python을 찾을 수 있도록)
RUN ln -s /usr/bin/python3 /usr/bin/python

# Python 패키지 설치 (최소 패키지만 설치)
RUN pip install --no-cache-dir \
    python-dotenv==1.0.0 \
    psycopg2-binary==2.9.9 \
    requests

# Spark 환경변수 설정
ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$PATH

# Spark Streaming 실행 (로컬 JAR 파일 사용)
CMD ["spark-submit", \
     "--jars", "/app/src/spark/jars/postgresql-42.6.0.jar,/app/src/spark/jars/spark-sql-kafka-0-10_2.12-3.5.0.jar,/app/src/spark/jars/kafka-clients-3.5.1.jar,/app/src/spark/jars/commons-lang3-3.12.0.jar,/app/src/spark/jars/commons-pool2-2.11.1.jar", \
     "/app/src/spark/jobs/streaming_consumer.py"]
