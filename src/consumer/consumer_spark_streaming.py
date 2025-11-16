"""
Kafka Consumer 2 (Spark Streaming)
Kafka 데이터를 읽어서:
1. statistics_events 테이블에 저장 (통계용)
2. ml_prepared_events 테이블에 저장 (ML용)
두 테이블 모두 item_properties에서 categoryid를 JOIN하여 추가
"""

import sys
import os
from pathlib import Path

# 상위 디렉토리 경로 추가
project_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(project_root))

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, LongType, IntegerType, StringType
from pyspark.sql.functions import (
    from_json, col, when, coalesce, current_timestamp,
    to_timestamp, row_number, monotonically_increasing_id,
    hour, dayofweek, date_format, count, max as spark_max,
    dense_rank, lag
)
from pyspark.sql.window import Window

from config.settings import (
    KAFKA_BROKERS,
    KAFKA_TOPIC,
    POSTGRES_HOST,
    POSTGRES_PORT,
    POSTGRES_DB,
    POSTGRES_USER,
    POSTGRES_PASSWORD,
    CSV_FILE_PATH
)
from src.utils.logger import get_logger

logger = get_logger(__name__)


class SparkStreamingConsumer:
    """Spark Streaming 기반 Kafka Consumer"""

    def __init__(self):
        """초기화"""
        self.spark = None
        self.kafka_brokers = ",".join(KAFKA_BROKERS)
        self.postgres_url = f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
        self.postgres_properties = {
            "user": POSTGRES_USER,
            "password": POSTGRES_PASSWORD,
            "driver": "org.postgresql.Driver"
        }

    def create_spark_session(self):
        """Spark Session 생성"""
        logger.info("Spark Session 생성 중...")

        self.spark = SparkSession.builder \
            .appName("Kafka-Consumer-Streaming") \
            .config("spark.jars.packages",
                    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
                    "org.postgresql:postgresql:42.6.0") \
            .config("spark.sql.streaming.checkpointLocation", "/tmp/spark_checkpoint") \
            .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
            .getOrCreate()

        self.spark.sparkContext.setLogLevel("WARN")
        logger.info("✅ Spark Session 생성 완료")
        return self.spark

    def load_item_properties(self):
        """item_properties CSV 로드"""
        logger.info("item_properties 로드 중...")

        try:
            # CSV 파일들 로드 (part1, part2)
            df_part1 = self.spark.read.csv(
                "data/raw/item_properties_part1.csv",
                header=True,
                inferSchema=True
            )
            df_part2 = self.spark.read.csv(
                "data/raw/item_properties_part2.csv",
                header=True,
                inferSchema=True
            )

            # 두 파일 병합
            df_properties = df_part1.union(df_part2)

            # categoryid 추출 (property = 'categoryid' 인 행들)
            df_categories = df_properties.filter(
                col("property") == "categoryid"
            ).select(
                col("itemid").cast(IntegerType()).alias("itemid"),
                col("value").cast(IntegerType()).alias("categoryid")
            ).dropDuplicates(["itemid"])

            logger.info(f"✅ item_properties 로드 완료: {df_categories.count()}개 상품")
            return df_categories

        except Exception as e:
            logger.error(f"item_properties 로드 실패: {e}", exc_info=True)
            raise

    def load_category_tree(self):
        """category_tree CSV 로드"""
        logger.info("category_tree 로드 중...")

        try:
            df_categories = self.spark.read.csv(
                "data/raw/category_tree.csv",
                header=True,
                inferSchema=True
            )

            logger.info(f"✅ category_tree 로드 완료: {df_categories.count()}개 카테고리")
            return df_categories

        except Exception as e:
            logger.error(f"category_tree 로드 실패: {e}", exc_info=True)
            raise

    def read_kafka_stream(self):
        """Kafka에서 스트리밍 데이터 읽기"""
        logger.info(f"Kafka 토픽 '{KAFKA_TOPIC}' 구독 중...")

        schema = StructType([
            StructField("timestamp", LongType(), True),
            StructField("visitorid", IntegerType(), True),
            StructField("event", StringType(), True),
            StructField("itemid", IntegerType(), True),
            StructField("transactionid", IntegerType(), True)
        ])

        df_stream = self.spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.kafka_brokers) \
            .option("subscribe", KAFKA_TOPIC) \
            .option("startingOffsets", "latest") \
            .option("maxOffsetsPerTrigger", 10000) \
            .load()

        # JSON 파싱
        df_parsed = df_stream.select(
            from_json(col("value").cast(StringType()), schema).alias("data")
        ).select("data.*")

        logger.info("✅ Kafka 스트림 읽기 설정 완료")
        return df_parsed

    def enrich_with_category(self, df_stream, df_categories):
        """스트림 데이터에 카테고리 정보 추가"""
        logger.info("카테고리 정보 JOIN 중...")

        df_enriched = df_stream.join(
            df_categories,
            on="itemid",
            how="left"
        ).select(
            col("timestamp"),
            col("visitorid"),
            col("itemid"),
            col("categoryid"),
            col("event"),
            col("transactionid"),
            current_timestamp().alias("created_at")
        )

        return df_enriched

    def add_primary_key(self, df):
        """Primary Key 추가 (id 컬럼)"""
        window_spec = Window.orderBy(monotonically_increasing_id())
        df_with_id = df.withColumn(
            "id",
            row_number().over(window_spec)
        )
        return df_with_id.select(
            "id", "timestamp", "visitorid", "itemid", "categoryid",
            "event", "transactionid", "created_at"
        )

    def prepare_statistics_events(self, df):
        """statistics_events 테이블용 특성 추가 (통계/BI용)"""
        logger.info("statistics_events 특성 추가 중...")

        # timestamp를 UNIX milliseconds로 가정하고 변환
        df_stats = df.withColumn(
            "event_timestamp",
            to_timestamp(col("timestamp") / 1000.0)
        ).withColumn(
            # 날짜 정보
            "event_date",
            date_format(col("event_timestamp"), "yyyy-MM-dd")
        ).withColumn(
            # 시간대 (0-23)
            "hour_of_day",
            hour(col("event_timestamp"))
        ).withColumn(
            # 요일 (1=Sunday, 7=Saturday)
            "day_of_week",
            dayofweek(col("event_timestamp"))
        ).withColumn(
            # 구매 여부 (transactionid 있으면 구매)
            "is_purchase",
            when(col("transactionid").isNotNull(), 1).otherwise(0)
        )

        # 최종 선택 (id는 이미 있음)
        return df_stats.select(
            "id", "timestamp", "visitorid", "itemid", "categoryid",
            "event", "transactionid", "event_date", "hour_of_day",
            "day_of_week", "is_purchase", "created_at"
        )

    def clean_and_prepare_ml_events(self, df):
        """ml_prepared_events 테이블용 정제 및 특성 추가"""
        logger.info("ml_prepared_events 정제 및 특성 추가 중...")

        # 1. NULL 값 처리
        df_cleaned = df.filter(
            col("visitorid").isNotNull() &
            col("itemid").isNotNull() &
            col("event").isNotNull() &
            col("timestamp").isNotNull() &
            (col("timestamp") > 0)  # timestamp 유효성 확인
        )

        # 2. 특성 추가
        df_features = df_cleaned.withColumn(
            "event_timestamp",
            to_timestamp(col("timestamp") / 1000.0)
        ).withColumn(
            # 사용자가 구매한 적이 있는지 (Window 함수)
            "is_buyer",
            when(
                col("transactionid").isNotNull(),
                1
            ).otherwise(0)
        ).withColumn(
            # 이벤트 시간대
            "event_hour",
            hour(col("event_timestamp"))
        ).withColumn(
            # 요일
            "event_dow",
            dayofweek(col("event_timestamp"))
        ).withColumn(
            # 월
            "event_month",
            date_format(col("event_timestamp"), "MM")
        )

        # 3. Window 함수로 사용자 관점 특성 추가
        # 사용자별 현재까지의 이벤트 수
        user_window = Window.partitionBy("visitorid") \
            .orderBy(col("timestamp").asc()) \
            .rowsBetween(Window.unboundedPreceding, Window.currentRow)

        df_user_features = df_features.withColumn(
            "user_event_count",
            count("*").over(user_window)
        ).withColumn(
            # 사용자의 첫 이벤트인지
            "is_user_first_event",
            when(col("user_event_count") == 1, 1).otherwise(0)
        )

        # 4. 상품 관점 특성 추가
        # 상품별 현재까지의 이벤트 수
        item_window = Window.partitionBy("itemid") \
            .orderBy(col("timestamp").asc()) \
            .rowsBetween(Window.unboundedPreceding, Window.currentRow)

        df_item_features = df_user_features.withColumn(
            "item_event_count",
            count("*").over(item_window)
        ).withColumn(
            # 상품의 첫 이벤트인지
            "is_item_first_event",
            when(col("item_event_count") == 1, 1).otherwise(0)
        )

        # 최종 선택 (정제된 데이터 + 특성)
        return df_item_features.select(
            "id", "timestamp", "visitorid", "itemid", "categoryid",
            "event", "transactionid",
            # 정제 관련
            "is_buyer",
            # 시간 관련
            "event_hour", "event_dow", "event_month",
            # 사용자 관련 특성
            "user_event_count", "is_user_first_event",
            # 상품 관련 특성
            "item_event_count", "is_item_first_event",
            "created_at"
        )

    def write_to_postgres(self, df, table_name, mode="append"):
        """PostgreSQL에 저장"""
        logger.info(f"PostgreSQL {table_name} 테이블에 저장 중...")

        query = df.writeStream \
            .format("jdbc") \
            .option("url", self.postgres_url) \
            .option("dbtable", table_name) \
            .option("user", POSTGRES_USER) \
            .option("password", POSTGRES_PASSWORD) \
            .option("checkpointLocation", f"/tmp/checkpoint_{table_name}") \
            .option("mode", mode) \
            .start()

        return query

    def start(self):
        """Consumer 시작"""
        try:
            logger.info("=" * 60)
            logger.info("Spark Streaming Consumer 시작")
            logger.info("=" * 60)

            # 1. Spark Session 생성
            self.create_spark_session()

            # 2. CSV 데이터 로드
            df_categories = self.load_item_properties()

            # 3. Kafka 스트림 읽기
            df_stream = self.read_kafka_stream()

            # 4. 카테고리 정보 JOIN
            df_enriched = self.enrich_with_category(df_stream, df_categories)

            # 5. Primary Key 추가
            df_with_id = self.add_primary_key(df_enriched)

            # 6. 각 테이블에 맞게 특성 추가 및 정제
            df_stats_prepared = self.prepare_statistics_events(df_with_id)
            df_ml_prepared = self.clean_and_prepare_ml_events(df_with_id)

            # 7. 두 테이블에 저장
            logger.info("저장 시작: statistics_events, ml_prepared_events")

            query_stats = self.write_to_postgres(df_stats_prepared, "statistics_events")
            query_ml = self.write_to_postgres(df_ml_prepared, "ml_prepared_events")

            # 8. 스트림 실행
            logger.info("✅ Spark Streaming 시작")
            logger.info("Ctrl+C로 종료")

            self.spark.streams.awaitAnyTermination()

        except KeyboardInterrupt:
            logger.warning("사용자가 중단했습니다")
        except Exception as e:
            logger.error(f"에러 발생: {e}", exc_info=True)
            raise
        finally:
            self.stop()

    def stop(self):
        """Consumer 종료"""
        logger.info("Spark Streaming 종료 중...")
        if self.spark:
            self.spark.stop()
        logger.info("=" * 60)


def main():
    """메인 실행 함수"""
    consumer = SparkStreamingConsumer()
    consumer.start()


if __name__ == "__main__":
    main()
