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
ENV SPARK_MASTER_HOST=0.0.0.0
ENV SPARK_MASTER_PORT=7077
ENV SPARK_MASTER_WEBUI_PORT=8080

# Master 실행
CMD ["/opt/spark/bin/spark-class", "org.apache.spark.deploy.master.Master", "--host", "0.0.0.0", "--port", "7077"]
