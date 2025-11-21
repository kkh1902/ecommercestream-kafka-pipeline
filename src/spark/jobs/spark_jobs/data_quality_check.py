"""
Spark Job: 데이터 무결성 검증 (Data Quality Check)

검증 범위:
1. Raw Data (raw_clickstream_events)
   - 전체 레코드 개수
   - NULL 값 비율 (visitorid, itemid, event, timestamp)
   - 유효하지 않은 이벤트 타입
   - 타임스탬프 범위

2. Processed Data (clickstream_events)
   - 전체 레코드 개수
   - NULL 값 비율
   - 처리율

결과 저장:
- data_quality_results 테이블에 각 테이블별 품질 점수 저장
- status: PASSED(95% 이상), WARNING(90~95%), FAILED(90% 미만)
"""

import sys
import os
import json
from pathlib import Path
from datetime import datetime, timedelta

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when, min, max, lit
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType, TimestampType

# 프로젝트 경로 설정
current_file = Path(__file__).resolve()
PROJECT_ROOT = current_file.parent.parent.parent.parent.parent

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config.settings import (
    POSTGRES_HOST,
    POSTGRES_PORT,
    POSTGRES_DB,
    POSTGRES_USER,
    POSTGRES_PASSWORD
)

# 로그 디렉토리 설정
log_dir = PROJECT_ROOT / 'airflow' / 'logs' / 'spark_jobs'
log_dir.mkdir(parents=True, exist_ok=True)
log_file = log_dir / f'data_quality_check_{datetime.now().strftime("%Y%m%d")}.log'


def main():
    """데이터 무결성 검증"""

    print("\n" + "="*70)
    print("Spark Job: 데이터 무결성 검증")
    print("="*70)

    # ================================================================
    # 1. Spark 세션 생성
    # ================================================================

    jar_dir = PROJECT_ROOT / "src" / "spark" / "jars"
    jar_files = [
        str(jar_dir / "postgresql-42.7.3.jar"),
    ]
    jar_list = ",".join(jar_files)

    spark = SparkSession.builder \
        .appName("DataQualityCheck") \
        .config("spark.jars", jar_list) \
        .config("spark.sql.adaptive.enabled", "true") \
        .getOrCreate()

    # Docker 환경 또는 로컬 환경 자동 감지
    if os.getenv('AIRFLOW_HOME'):
        # Docker 환경: postgres는 Docker 네트워크 호스트명
        postgres_host = 'postgres'
    else:
        # 로컬 개발: localhost 사용
        postgres_host = POSTGRES_HOST if POSTGRES_HOST != 'localhost' else 'postgres'

    postgres_url = f"jdbc:postgresql://{postgres_host}:{POSTGRES_PORT}/{POSTGRES_DB}"
    postgres_properties = {
        "user": POSTGRES_USER,
        "password": POSTGRES_PASSWORD,
        "driver": "org.postgresql.Driver"
    }

    # ================================================================
    # 2. 검증 결과 저장을 위한 DataFrame 스키마
    # ================================================================

    quality_schema = StructType([
        StructField("check_date", StringType(), True),
        StructField("table_name", StringType(), True),
        StructField("total_records", LongType(), True),
        StructField("quality_score", DoubleType(), True),
        StructField("status", StringType(), True),
        StructField("null_count", LongType(), True),
        StructField("invalid_count", LongType(), True),
        StructField("details", StringType(), True),
        StructField("created_at", TimestampType(), True)
    ])

    results_list = []

    # ================================================================
    # 3️⃣ RAW DATA 검증
    # ================================================================

    print("\n" + "-"*70)
    print("📌 1️⃣ RAW DATA 검증 (raw_clickstream_events)")
    print("-"*70)

    try:
        df_raw = spark.read.jdbc(postgres_url, "raw_clickstream_events",
                                 properties=postgres_properties)

        # 한 번의 스캔으로 모든 통계 계산 (병렬)
        raw_stats = df_raw.agg(
            count("*").alias("total_records"),
            count(when(col("visitorid").isNull(), 1)).alias("null_visitorid"),
            count(when(col("itemid").isNull(), 1)).alias("null_itemid"),
            count(when(col("event").isNull(), 1)).alias("null_event"),
            count(when(col("timestamp").isNull(), 1)).alias("null_timestamp"),
            count(when(
                ~col("event").isin(['view', 'addtocart', 'transaction', 'removefromcart']),
                1
            )).alias("invalid_events"),
            min(col("timestamp")).alias("min_timestamp"),
            max(col("timestamp")).alias("max_timestamp")
        ).collect()[0]

        raw_total = raw_stats['total_records']
        raw_null_visitorid = raw_stats['null_visitorid']
        raw_null_itemid = raw_stats['null_itemid']
        raw_null_event = raw_stats['null_event']
        raw_null_timestamp = raw_stats['null_timestamp']
        raw_invalid_events = raw_stats['invalid_events']
        raw_total_issues = raw_null_visitorid + raw_null_itemid + raw_null_event + raw_null_timestamp + raw_invalid_events

        raw_quality_score = (raw_total - raw_total_issues) / raw_total if raw_total > 0 else 0.0

        # 상태 결정
        if raw_quality_score >= 0.95:
            raw_status = 'PASSED'
        elif raw_quality_score >= 0.90:
            raw_status = 'WARNING'
        else:
            raw_status = 'FAILED'

        print(f"✅ 전체 레코드: {raw_total:,}")
        print(f"⚠️  NULL 값 (visitorid): {raw_null_visitorid:,}")
        print(f"⚠️  NULL 값 (itemid): {raw_null_itemid:,}")
        print(f"⚠️  NULL 값 (event): {raw_null_event:,}")
        print(f"⚠️  NULL 값 (timestamp): {raw_null_timestamp:,}")
        print(f"❌ 유효하지 않은 이벤트: {raw_invalid_events:,}")
        print(f"📊 품질 점수: {raw_quality_score:.2%}")
        print(f"📋 상태: {raw_status}")
        print(f"   타임스탬프 범위: {raw_stats['min_timestamp']} ~ {raw_stats['max_timestamp']}")

        raw_result = {
            'check_date': str(datetime.now().date()),
            'table_name': 'raw_clickstream_events',
            'total_records': int(raw_total),
            'quality_score': float(raw_quality_score),
            'status': raw_status,
            'null_count': int(raw_total_issues),
            'invalid_count': int(raw_invalid_events),
            'details': json.dumps({
                'null_visitorid': int(raw_null_visitorid),
                'null_itemid': int(raw_null_itemid),
                'null_event': int(raw_null_event),
                'null_timestamp': int(raw_null_timestamp),
                'timestamp_range': f"{raw_stats['min_timestamp']} ~ {raw_stats['max_timestamp']}"
            }),
            'created_at': datetime.now()
        }
        results_list.append(raw_result)

    except Exception as e:
        print(f"❌ Raw Data 검증 실패: {e}")
        raise

    # ================================================================
    # 4️⃣ PROCESSED DATA 검증
    # ================================================================

    print("\n" + "-"*70)
    print("📌 2️⃣ PROCESSED DATA 검증 (clickstream_events)")
    print("-"*70)

    try:
        df_processed = spark.read.jdbc(postgres_url, "clickstream_events",
                                       properties=postgres_properties)

        processed_stats = df_processed.agg(
            count("*").alias("total_records"),
            count(when(col("visitorid").isNull(), 1)).alias("null_visitorid"),
            count(when(col("itemid").isNull(), 1)).alias("null_itemid")
        ).collect()[0]

        processed_total = processed_stats['total_records']
        processed_null_count = processed_stats['null_visitorid'] + processed_stats['null_itemid']
        processed_quality_score = (processed_total - processed_null_count) / processed_total if processed_total > 0 else 0.0
        processing_ratio = (processed_total / raw_total * 100) if raw_total > 0 else 0.0

        # 상태 결정
        if processed_quality_score >= 0.95:
            processed_status = 'PASSED'
        elif processed_quality_score >= 0.90:
            processed_status = 'WARNING'
        else:
            processed_status = 'FAILED'

        print(f"✅ 전체 레코드: {processed_total:,}")
        print(f"⚠️  NULL 값 (visitorid): {processed_stats['null_visitorid']:,}")
        print(f"⚠️  NULL 값 (itemid): {processed_stats['null_itemid']:,}")
        print(f"📊 품질 점수: {processed_quality_score:.2%}")
        print(f"📋 상태: {processed_status}")
        print(f"   처리율: {processing_ratio:.1f}% ({processed_total:,} / {raw_total:,})")

        processed_result = {
            'check_date': str(datetime.now().date()),
            'table_name': 'clickstream_events',
            'total_records': int(processed_total),
            'quality_score': float(processed_quality_score),
            'status': processed_status,
            'null_count': int(processed_null_count),
            'invalid_count': 0,
            'details': json.dumps({
                'null_visitorid': int(processed_stats['null_visitorid']),
                'null_itemid': int(processed_stats['null_itemid']),
                'processing_ratio': f"{processing_ratio:.1f}%"
            }),
            'created_at': datetime.now()
        }
        results_list.append(processed_result)

    except Exception as e:
        print(f"❌ Processed Data 검증 실패: {e}")
        raise

    # ================================================================
    # 5️⃣ 검증 결과 저장
    # ================================================================

    print("\n" + "-"*70)
    print("📌 3️⃣ 검증 결과 저장")
    print("-"*70)

    try:
        quality_results_df = spark.createDataFrame(results_list, quality_schema)

        quality_results_df.write.format("jdbc") \
            .option("url", postgres_url) \
            .option("dbtable", "data_quality_results") \
            .option("mode", "append") \
            .options(**postgres_properties) \
            .save()

        print(f"✅ 검증 결과 저장 완료 ({len(results_list)}개 테이블)")

    except Exception as e:
        print(f"❌ 검증 결과 저장 실패: {e}")
        raise

    # ================================================================
    # 완료
    # ================================================================

    print("\n" + "="*70)
    print("✅ 데이터 무결성 검증 완료!")
    print("="*70 + "\n")

    spark.stop()


if __name__ == '__main__':
    main()
