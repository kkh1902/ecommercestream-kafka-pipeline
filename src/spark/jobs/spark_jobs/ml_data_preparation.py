"""
Spark Job: ML 추천 시스템 데이터 준비
raw_clickstream_events에서 특성 엔지니어링을 수행하여
학습 데이터를 준비하고 저장

특성 엔지니어링:
- 시간 특성: hour, day_of_week, month
- 사용자 특성: cumulative event count, is_first_event
- 상품 특성: cumulative event count, is_first_event
- 타겟: is_buyer (transaction 여부)
"""

import sys
import os
from pathlib import Path
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, countDistinct, row_number, when,
    unix_timestamp, from_unixtime, date_format, dense_rank,
    first, last, sum as spark_sum, avg
)
from pyspark.sql.window import Window

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
log_file = log_dir / f'ml_data_preparation_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'


def main():
    """ML 데이터 준비 Spark Job"""

    print("=" * 70)
    print("Spark Job: ML 추천 시스템 데이터 준비")
    print("=" * 70)

    # Spark 세션 생성
    jar_dir = PROJECT_ROOT / "src" / "spark" / "jars"
    jar_files = [
        "postgresql-42.6.0.jar",
        "spark-sql-kafka-0-10_2.12-3.5.0.jar",
        "kafka-clients-3.5.1.jar",
        "commons-lang3-3.12.0.jar",
        "commons-pool2-2.11.1.jar"
    ]
    jar_paths = ",".join([str(jar_dir / jar) for jar in jar_files])

    spark = SparkSession.builder \
        .appName("MLDataPreparation") \
        .master("local[*]") \
        .config("spark.jars", jar_paths) \
        .config("spark.sql.shuffle.partitions", "10") \
        .getOrCreate()

    try:
        postgres_url = f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
        postgres_properties = {
            "user": POSTGRES_USER,
            "password": POSTGRES_PASSWORD,
            "driver": "org.postgresql.Driver"
        }

        print("\n[Step 1] raw_clickstream_events 데이터 로드")
        print("-" * 70)

        # 1. raw_clickstream_events 로드
        df_raw = spark.read.jdbc(
            postgres_url,
            "raw_clickstream_events",
            properties=postgres_properties
        )
        print(f"✓ 원본 데이터 로드 완료: {df_raw.count():,}개 레코드")

        # 2. 데이터 정제 및 특성 엔지니어링
        print("\n[Step 2] 특성 엔지니어링")
        print("-" * 70)

        # timestamp를 밀리초에서 초로 변환 후 시간 특성 생성
        df_features = df_raw \
            .withColumn("timestamp_sec", col("timestamp") / 1000) \
            .withColumn("event_timestamp", from_unixtime(col("timestamp_sec"))) \
            .withColumn("event_hour", date_format(col("event_timestamp"), "H").cast("int")) \
            .withColumn("event_dow", date_format(col("event_timestamp"), "u").cast("int")) \
            .withColumn("event_month", date_format(col("event_timestamp"), "M").cast("int"))

        # 구매 여부 (transaction 있으면 1, 없으면 0)
        df_features = df_features.withColumn(
            "is_buyer",
            when(col("transactionid").isNotNull(), 1).otherwise(0)
        )

        # 사용자별 누적 이벤트 수 및 첫 이벤트 여부
        window_user = Window.partitionBy("visitorid").orderBy("timestamp")
        df_features = df_features \
            .withColumn("user_event_count", row_number().over(window_user)) \
            .withColumn("is_user_first_event", when(col("user_event_count") == 1, 1).otherwise(0))

        # 상품별 누적 이벤트 수 및 첫 이벤트 여부
        window_item = Window.partitionBy("itemid").orderBy("timestamp")
        df_features = df_features \
            .withColumn("item_event_count", row_number().over(window_item)) \
            .withColumn("is_item_first_event", when(col("item_event_count") == 1, 1).otherwise(0))

        # event 컬럼을 숫자로 인코딩 (view=0, addtocart=1, transaction=2)
        event_mapping = {"view": 0, "addtocart": 1, "transaction": 2}
        df_features = df_features.withColumn(
            "event_encoded",
            when(col("event") == "view", 0)
            .when(col("event") == "addtocart", 1)
            .when(col("event") == "transaction", 2)
            .otherwise(0)
        )

        # 필요한 컬럼만 선택
        feature_columns = [
            "id", "timestamp", "visitorid", "itemid", "event", "transactionid",
            "event_hour", "event_dow", "event_month",
            "event_encoded", "is_buyer",
            "user_event_count", "is_user_first_event",
            "item_event_count", "is_item_first_event"
        ]
        df_features = df_features.select(feature_columns)

        print(f"✓ 특성 엔지니어링 완료")
        print(f"  - 행 수: {df_features.count():,}개")
        print(f"  - 특성 수: {len(feature_columns) - 3}개")  # -3: id, timestamp, transactionid

        # 3. Train/Test 분할 (시간순)
        print("\n[Step 3] Train/Test 분할 (80% / 20%)")
        print("-" * 70)

        total_count = df_features.count()
        train_ratio = 0.8
        train_count = int(total_count * train_ratio)

        # timestamp 기준으로 정렬하여 분할
        df_features = df_features.orderBy("timestamp")
        df_train = df_features.limit(train_count)
        df_test = df_features.subtract(df_train)

        train_rows = df_train.count()
        test_rows = df_test.count()

        print(f"✓ Train/Test 분할 완료")
        print(f"  - Train: {train_rows:,}개 ({train_rows/total_count*100:.1f}%)")
        print(f"  - Test: {test_rows:,}개 ({test_rows/total_count*100:.1f}%)")

        # 4. X, y 분리 (X: 특성, y: 타겟)
        print("\n[Step 4] X, y 분리")
        print("-" * 70)

        x_columns = [
            "visitorid", "itemid", "event_encoded",
            "event_hour", "event_dow", "event_month",
            "user_event_count", "is_user_first_event",
            "item_event_count", "is_item_first_event"
        ]
        y_column = "is_buyer"

        df_train_x = df_train.select(x_columns)
        df_train_y = df_train.select(y_column)

        df_test_x = df_test.select(x_columns)
        df_test_y = df_test.select(y_column)

        print(f"✓ X, y 분리 완료")
        print(f"  - 특성 개수: {len(x_columns)}")
        print(f"  - 타겟: {y_column}")

        # 5. 데이터 저장
        print("\n[Step 5] 처리된 데이터 저장")
        print("-" * 70)

        output_dir = PROJECT_ROOT / "src" / "ml" / "data"
        output_dir.mkdir(parents=True, exist_ok=True)

        # Parquet 형식으로 저장 (압축됨)
        train_x_path = str(output_dir / "train_x")
        train_y_path = str(output_dir / "train_y")
        test_x_path = str(output_dir / "test_x")
        test_y_path = str(output_dir / "test_y")

        df_train_x.coalesce(1).write.mode("overwrite").parquet(train_x_path)
        df_train_y.coalesce(1).write.mode("overwrite").parquet(train_y_path)
        df_test_x.coalesce(1).write.mode("overwrite").parquet(test_x_path)
        df_test_y.coalesce(1).write.mode("overwrite").parquet(test_y_path)

        print(f"✓ 데이터 저장 완료")
        print(f"  - Train X: {train_x_path}")
        print(f"  - Train y: {train_y_path}")
        print(f"  - Test X: {test_x_path}")
        print(f"  - Test y: {test_y_path}")

        # 6. 통계 출력
        print("\n[Step 6] 데이터 통계")
        print("-" * 70)

        # 클래스 분포
        class_dist = df_train_y.groupBy(y_column).count().collect()
        print(f"✓ 클래스 분포 (Train):")
        for row in class_dist:
            label = "구매" if row[y_column] == 1 else "비구매"
            count = row["count"]
            pct = count / train_rows * 100
            print(f"  - {label}: {count:,}개 ({pct:.2f}%)")

        # 사용자/상품 수
        unique_visitors = df_train.select("visitorid").distinct().count()
        unique_items = df_train.select("itemid").distinct().count()
        print(f"✓ 고유값:")
        print(f"  - 사용자: {unique_visitors:,}명")
        print(f"  - 상품: {unique_items:,}개")

        print("\n" + "=" * 70)
        print("✅ ML 데이터 준비 완료")
        print("=" * 70)

    except Exception as e:
        print(f"\n❌ 에러 발생: {e}")
        import traceback
        traceback.print_exc()
        raise

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
