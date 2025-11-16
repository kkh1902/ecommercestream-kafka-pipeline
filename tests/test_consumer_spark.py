"""
Consumer 2 (Spark Streaming) 단위 테스트
"""

import sys
import os
from pathlib import Path
import json

# 상위 디렉토리 경로 추가
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, LongType, IntegerType, StringType
from pyspark.sql.functions import from_json, col


def create_test_spark_session():
    """테스트용 Spark Session 생성"""
    return SparkSession.builder \
        .appName("Test-Consumer-Spark") \
        .master("local[*]") \
        .config("spark.driver.memory", "2g") \
        .getOrCreate()


def test_kafka_message_parsing():
    """Kafka 메시지 파싱 테스트"""
    print("\n🧪 테스트 1: Kafka 메시지 파싱")

    spark = create_test_spark_session()

    # 테스트 데이터 생성
    test_messages = [
        '{"timestamp": 1433221332117, "visitorid": 257597, "event": "view", "itemid": 355908, "transactionid": null}',
        '{"timestamp": 1433224214164, "visitorid": 992329, "event": "click", "itemid": 248676, "transactionid": null}',
        '{"timestamp": 1433221999827, "visitorid": 111016, "event": "purchase", "itemid": 318965, "transactionid": 12345}',
    ]

    # DataFrame 생성
    df = spark.createDataFrame([(msg,) for msg in test_messages], ["value"])

    # 스키마 정의
    schema = StructType([
        StructField("timestamp", LongType(), True),
        StructField("visitorid", IntegerType(), True),
        StructField("event", StringType(), True),
        StructField("itemid", IntegerType(), True),
        StructField("transactionid", IntegerType(), True)
    ])

    # JSON 파싱
    df_parsed = df.select(
        from_json(col("value"), schema).alias("data")
    ).select("data.*")

    # 검증
    result = df_parsed.collect()
    assert len(result) == 3, f"예상 3개, 실제 {len(result)}개"
    assert result[0]['visitorid'] == 257597, "첫 번째 visitorid 확인"
    assert result[2]['transactionid'] == 12345, "purchase transactionid 확인"

    print("✅ 메시지 파싱 성공")
    spark.stop()


def test_category_feature():
    """카테고리 특성 추가 테스트"""
    print("\n🧪 테스트 2: 카테고리 특성 추가")

    spark = create_test_spark_session()

    # 원본 데이터
    data1 = [
        (1433221332117, 257597, "view", 355908, None),
        (1433224214164, 992329, "click", 248676, None),
    ]
    df1 = spark.createDataFrame(data1, ["timestamp", "visitorid", "event", "itemid", "transactionid"])

    # 카테고리 정보
    data2 = [
        (355908, 1234),
        (248676, 5678),
    ]
    df2 = spark.createDataFrame(data2, ["itemid", "categoryid"])

    # JOIN
    df_joined = df1.join(df2, on="itemid", how="left")

    result = df_joined.select("itemid", "categoryid").collect()
    assert result[0]['categoryid'] == 1234, "첫 번째 카테고리 확인"
    assert result[1]['categoryid'] == 5678, "두 번째 카테고리 확인"

    print("✅ 카테고리 JOIN 성공")
    spark.stop()


def test_statistics_features():
    """통계 특성 추가 테스트"""
    print("\n🧪 테스트 3: 통계 특성 추가 (event_date, hour_of_day, is_purchase)")

    spark = create_test_spark_session()

    from pyspark.sql.functions import (
        to_timestamp, date_format, hour, dayofweek, when,
        current_timestamp
    )

    # 테스트 데이터 (milliseconds 기준)
    data = [
        (1, 1433221332117, 257597, "view", 355908, None),      # 2015-06-02 03:08:52
        (2, 1433224214164, 992329, "purchase", 248676, 12345),  # 2015-06-02 04:10:14
    ]
    df = spark.createDataFrame(data,
        ["id", "timestamp", "visitorid", "event", "itemid", "transactionid"])

    # 특성 추가
    df_stats = df.withColumn(
        "event_timestamp",
        to_timestamp(col("timestamp") / 1000.0)
    ).withColumn(
        "event_date",
        date_format(col("event_timestamp"), "yyyy-MM-dd")
    ).withColumn(
        "hour_of_day",
        hour(col("event_timestamp"))
    ).withColumn(
        "day_of_week",
        dayofweek(col("event_timestamp"))
    ).withColumn(
        "is_purchase",
        when(col("transactionid").isNotNull(), 1).otherwise(0)
    )

    result = df_stats.select(
        "timestamp", "event_date", "hour_of_day", "day_of_week", "is_purchase"
    ).collect()

    # 검증
    assert result[0]['event_date'] == "2015-06-02", "첫 번째 날짜 확인"
    assert result[0]['hour_of_day'] == 3, "첫 번째 시간 확인"
    assert result[0]['is_purchase'] == 0, "첫 번째 구매 여부 확인"
    assert result[1]['is_purchase'] == 1, "두 번째 구매 여부 확인"

    print("✅ 통계 특성 추가 성공")
    spark.stop()


def test_ml_cleaning():
    """ML 데이터 정제 테스트"""
    print("\n🧪 테스트 4: ML 데이터 정제 (NULL 제거, 타입 변환)")

    spark = create_test_spark_session()

    # 정제 전 데이터 (NULL 포함)
    data = [
        (1, 1433221332117, 257597, "view", 355908, None),
        (2, 1433224214164, None, "click", 248676, None),  # NULL visitorid
        (3, 0, 111016, "view", 318965, None),              # timestamp 0
        (4, 1433221999827, 111016, "purchase", 318965, 12345),
    ]
    df = spark.createDataFrame(data,
        ["id", "timestamp", "visitorid", "event", "itemid", "transactionid"])

    # 정제
    df_cleaned = df.filter(
        col("visitorid").isNotNull() &
        col("itemid").isNotNull() &
        col("event").isNotNull() &
        col("timestamp").isNotNull() &
        (col("timestamp") > 0)
    )

    result = df_cleaned.collect()
    assert len(result) == 2, f"정제 후 2개, 실제 {len(result)}개"
    assert result[0]['id'] == 1, "첫 번째 ID 확인"
    assert result[1]['id'] == 4, "두 번째 ID 확인"

    print("✅ ML 데이터 정제 성공")
    spark.stop()


def run_all_tests():
    """모든 테스트 실행"""
    print("\n" + "="*60)
    print("🚀 Consumer 2 (Spark Streaming) 테스트 시작")
    print("="*60)

    try:
        test_kafka_message_parsing()
        test_category_feature()
        test_statistics_features()
        test_ml_cleaning()

        print("\n" + "="*60)
        print("✅ 모든 테스트 통과!")
        print("="*60)

    except AssertionError as e:
        print(f"\n❌ 테스트 실패: {e}")
        raise
    except Exception as e:
        print(f"\n❌ 예상치 못한 오류: {e}")
        raise


if __name__ == "__main__":
    run_all_tests()
