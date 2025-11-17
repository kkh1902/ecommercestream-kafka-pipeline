"""
Spark Streaming Job
Kafka에서 데이터를 읽어 통계 데이터를 추가 후 PostgreSQL에 저장
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_timestamp, current_timestamp,
    count as spark_count, window, avg, max as spark_max, min as spark_min
)
from pyspark.sql.types import StructType, StructField, StringType, LongType
import sys
import os
import traceback
from pathlib import Path

# 상위 디렉토리 경로 추가
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))))

from config.settings import (
    KAFKA_BROKERS,
    KAFKA_TOPIC,
    POSTGRES_HOST,
    POSTGRES_PORT,
    POSTGRES_DB,
    POSTGRES_USER,
    POSTGRES_PASSWORD
)
from src.utils.logger import get_logger

# 로그 디렉토리 생성
log_dir = Path("logs/spark")
log_dir.mkdir(parents=True, exist_ok=True)

# 로거 설정
logger = get_logger(__name__, log_file=str(log_dir / "spark_streaming.log"))


class StreamingConsumer:
    def __init__(self):
        self.spark = None
        self.kafka_bootstrap_servers = ','.join(KAFKA_BROKERS)
        self.jdbc_url = f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
        self.jdbc_properties = {
            "user": POSTGRES_USER,
            "password": POSTGRES_PASSWORD,
            "driver": "org.postgresql.Driver"
        }

    def create_spark_session(self):
        """Spark Session 생성"""
        print("Spark Session 생성 중...")

        self.spark = SparkSession.builder \
            .appName("Clickstream Processor") \
            .config("spark.jars.packages",
                   "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
                   "org.postgresql:postgresql:42.6.0") \
            .config("spark.sql.streaming.checkpointLocation", "/tmp/spark-checkpoint") \
            .config("spark.sql.adaptive.enabled", "true") \
            .master("local[*]") \
            .getOrCreate()

        self.spark.sparkContext.setLogLevel("WARN")
        print(f"Spark Session 생성 완료")
        return self.spark

    def define_schema(self):
        """Kafka 메시지 스키마 정의"""
        return StructType([
            StructField("timestamp", LongType(), True),
            StructField("visitorid", LongType(), True),
            StructField("event", StringType(), True),
            StructField("itemid", LongType(), True),
            StructField("transactionid", LongType(), True)
        ])

    def read_from_kafka(self):
        """Kafka에서 스트림 읽기"""
        print(f"Kafka 연결: {self.kafka_bootstrap_servers}")
        print(f"토픽: {KAFKA_TOPIC}")

        schema = self.define_schema()

        df = self.spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.kafka_bootstrap_servers) \
            .option("subscribe", KAFKA_TOPIC) \
            .option("startingOffsets", "latest") \
            .option("maxOffsetsPerTrigger", 1000) \
            .load()

        # JSON 파싱
        parsed_df = df.selectExpr("CAST(value AS STRING) as json") \
            .select(from_json(col("json"), schema).alias("data")) \
            .filter(col("data").isNotNull()) \
            .select("data.*")

        # Timestamp 변환
        # 옛날 데이터(2015년)의 timestamp를 날짜/시간으로 변환
        # Unix timestamp (밀리초) -> 초로 변환 -> timestamp 타입으로 변환
        valid_df = parsed_df.withColumn(
            "event_time",
            to_timestamp(col("timestamp") / 1000)
        )

        return valid_df

    def add_statistics(self, df):
        """통계 데이터 추가"""
        print("통계 데이터 추가 중...")

        # 기본 데이터와 처리 시간 추가
        stats_df = df.withColumn("processed_at", current_timestamp())

        return stats_df

    def write_to_postgres(self, batch_df, batch_id):
        """PostgreSQL에 데이터 저장"""
        try:
            record_count = batch_df.count()
            if record_count > 0:
                print(f"배치 {batch_id}: {record_count}개 레코드 저장 중...")

                # 저장할 컬럼만 선택 (clickstream_events 테이블 스키마에 맞춤)
                # timestamp를 Unix timestamp (밀리초) -> 초 -> TIMESTAMP로 변환
                df_to_save = batch_df.select(
                    to_timestamp(col("timestamp") / 1000).alias("timestamp"),
                    col("visitorid").cast("integer"),
                    col("event"),
                    col("itemid").cast("integer"),
                    col("transactionid").cast("integer")
                )

                df_to_save.write \
                    .mode("append") \
                    .jdbc(
                        url=self.jdbc_url,
                        table="clickstream_events",
                        properties=self.jdbc_properties
                    )

                print(f"배치 {batch_id}: clickstream_events 테이블에 {record_count}개 저장 완료")
        except Exception as e:
            print(f"배치 {batch_id} 저장 실패: {e}")
            traceback.print_exc()

    def run(self):
        """파이프라인 실행"""
        print("=" * 60)
        print("Spark Streaming 시작")
        print("=" * 60)

        try:
            self.create_spark_session()

            # Kafka에서 읽기
            raw_df = self.read_from_kafka()

            # 통계 추가
            stats_df = self.add_statistics(raw_df)

            # PostgreSQL에 저장
            query = stats_df.writeStream \
                .outputMode("append") \
                .foreachBatch(self.write_to_postgres) \
                .start()

            print("Spark Streaming 실행 중...")
            query.awaitTermination()

        except KeyboardInterrupt:
            print("\n사용자가 중단했습니다")
        except Exception as e:
            print(f"\n에러: {e}")
            traceback.print_exc()
            raise
        finally:
            if self.spark:
                self.spark.stop()
                print("Spark Session 종료")


def main():
    consumer = StreamingConsumer()
    consumer.run()


if __name__ == '__main__':
    main()