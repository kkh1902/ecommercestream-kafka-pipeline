"""
Spark Job: 일일 통계 배치 처리
clickstream_events에서 일일 통계를 집계하여 daily_statistics에 저장

집계 항목:
- total_sales: 거래 개수 (event = 'transaction' 개수)
- total_events: 전체 이벤트 수
- unique_visitors: 고유 방문자 수

테스트 모드: 최근 10일 데이터를 일별로 집계 (테스트용)
프로덕션 모드: 어제 데이터만 집계 (운영용)
"""

import sys
import os
from pathlib import Path
from datetime import datetime, timedelta

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, countDistinct, date_format

# 프로젝트 경로 설정
# 현재 파일의 경로: src/spark/jobs/spark_jobs/batch_statistics.py
# 프로젝트 루트: ../../../../.. (5단계 위)
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
log_file = log_dir / f'batch_statistics_{datetime.now().strftime("%Y%m%d")}.log'


def main():
    """일일 통계 배치 처리"""

    print("Spark Job: 일일 통계 배치 처리")

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
    print(f"JAR 경로: {jar_paths}")

    spark = SparkSession.builder \
        .appName("BatchStatistics") \
        .master("local[*]") \
        .config("spark.jars", jar_paths) \
        .getOrCreate()

    try:
        postgres_url = f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
        postgres_properties = {
            "user": POSTGRES_USER,
            "password": POSTGRES_PASSWORD,
            "driver": "org.postgresql.Driver"
        }

        # clickstream_events 로드
        print("\nclickstream_events 데이터 로드 중")
        df = spark.read.jdbc(postgres_url, "clickstream_events", properties=postgres_properties)

        # timestamp를 날짜로 변환 (TIMESTAMP 타입은 이미 변환된 형태)
        df_with_date = df.withColumn(
            "event_date",
            date_format(col("timestamp"), "yyyy-MM-dd")
        )

        # TEST  2015-06-03 ~ 2015-06-13 데이터 필터링 (테스트용)
        start_date = "2015-06-03"
        end_date = "2015-06-13"

        print(f"집계 기간: {start_date} ~ {end_date} (11일)")

        # 최근 10일 데이터 필터링
        df_recent = df_with_date.filter(
            (col("event_date") >= start_date) & (col("event_date") <= end_date)
        )

        # 통계 계산
        print("\n일별 통계 계산 중")

        # Spark SQL로 일별 집계
        # SQL에서 사용할 수 있는 임시 테이블로 등록
        # 같은 세션(프로세스) 내이면 어디서든 사용 가능
        # CTE를 사용하려면 데이터 소스가 실제 테이블이거나 이미 등록된 뷰여야 합니다.
        # df_recent는 Spark의 메모리 DataFrame이기 때문에, 직접 CTE에서 사용할 수 없습니다.
        df_recent.createOrReplaceTempView("clickstream_temp")

        result_df = spark.sql("""
            SELECT
                CAST(event_date AS DATE) as stats_date,
                COUNT(CASE WHEN event = 'transaction' THEN 1 END) as total_sales,
                COUNT(*) as total_events,
                COUNT(DISTINCT visitorid) as unique_visitors
            FROM clickstream_temp
            GROUP BY event_date
            ORDER BY event_date DESC
        """)

        # 결과 확인
        results = result_df.collect()
        print(f"  총 {len(results)}일의 데이터 처리")

        for row in results:
            print(f"    {row['stats_date']}: 거래 {int(row['total_sales']):,}, 이벤트 {int(row['total_events']):,}, 방문자 {int(row['unique_visitors']):,}")

        # daily_statistics에 저장
        print("\n daily_statistics에 저장 중")

        # PostgreSQL에 저장
        result_df.write \
            .format("jdbc") \
            .option("url", postgres_url) \
            .option("dbtable", "daily_statistics") \
            .option("user", POSTGRES_USER) \
            .option("password", POSTGRES_PASSWORD) \
            .option("driver", "org.postgresql.Driver") \
            .mode("append") \
            .save()

        print(f"{len(results)}개 행 저장 완료")

        print("Spark Job: 일일 통계 배치 처리 완료")


    except Exception as e:
        print(f"Spark Job 실패: {e}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
