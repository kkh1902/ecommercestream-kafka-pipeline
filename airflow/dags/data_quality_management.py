"""
데이터 품질관리 DAG
매일 새벽 1시에 데이터 품질 검증 및 모니터링
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import json

# 기본 설정
default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'start_date': datetime(2025, 11, 1),
    'email_on_failure': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'data_quality_management',
    default_args=default_args,
    description='데이터 품질 검증 및 모니터링',
    schedule_interval='0 1 * * *',  # 매일 새벽 1시
    catchup=False,
    tags=['data-quality', 'monitoring'],
)


# ===== Task 1: raw_events 품질 검증 =====
def check_raw_events_quality(**context):
    """raw_events 테이블 품질 검증"""
    postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
    conn = postgres_hook.get_conn()
    cursor = conn.cursor()

    execution_date = context['execution_date']
    yesterday = (execution_date - timedelta(days=1)).strftime("%Y-%m-%d")

    # 1. NULL 값 검증
    cursor.execute("""
        SELECT
            COUNT(*) as total,
            COUNT(*) FILTER (WHERE visitorid IS NULL) as null_visitorid,
            COUNT(*) FILTER (WHERE itemid IS NULL) as null_itemid,
            COUNT(*) FILTER (WHERE event IS NULL) as null_event,
            COUNT(*) FILTER (WHERE timestamp IS NULL) as null_timestamp
        FROM clickstream_events
        WHERE DATE(created_at) = %s
    """, (yesterday,))

    null_stats = cursor.fetchone()
    print(f"raw_events NULL 검증: {null_stats}")

    # 2. timestamp 유효성 검증
    cursor.execute("""
        SELECT
            COUNT(*) as total,
            COUNT(*) FILTER (WHERE timestamp <= 0) as invalid_timestamp,
            COUNT(*) FILTER (WHERE timestamp > EXTRACT(EPOCH FROM NOW()) * 1000) as future_timestamp
        FROM clickstream_events
        WHERE DATE(created_at) = %s
    """, (yesterday,))

    timestamp_stats = cursor.fetchone()
    print(f"raw_events timestamp 검증: {timestamp_stats}")

    # 3. event 타입 검증
    cursor.execute("""
        SELECT event, COUNT(*) as count
        FROM clickstream_events
        WHERE DATE(created_at) = %s
        GROUP BY event
        ORDER BY count DESC
    """, (yesterday,))

    event_stats = cursor.fetchall()
    print(f"raw_events event 타입: {event_stats}")

    cursor.close()
    conn.close()

    return {
        'null_stats': null_stats,
        'timestamp_stats': timestamp_stats,
        'event_stats': event_stats
    }


t1_raw_quality = PythonOperator(
    task_id='check_raw_events_quality',
    python_callable=check_raw_events_quality,
    dag=dag,
)


# ===== Task 2: statistics_events 품질 검증 =====
def check_statistics_events_quality(**context):
    """statistics_events 테이블 품질 검증"""
    postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
    conn = postgres_hook.get_conn()
    cursor = conn.cursor()

    execution_date = context['execution_date']
    yesterday = (execution_date - timedelta(days=1)).strftime("%Y-%m-%d")

    # 1. 레코드 개수 검증
    cursor.execute("""
        SELECT
            COUNT(*) as total,
            COUNT(DISTINCT event_date) as unique_dates,
            COUNT(DISTINCT visitorid) as unique_visitors,
            COUNT(DISTINCT itemid) as unique_items
        FROM statistics_events
        WHERE event_date = %s
    """, (yesterday,))

    basic_stats = cursor.fetchone()
    print(f"statistics_events 기본 통계: {basic_stats}")

    # 2. 특성 완성도 검증
    cursor.execute("""
        SELECT
            COUNT(*) as total,
            COUNT(*) FILTER (WHERE hour_of_day IS NULL) as null_hour,
            COUNT(*) FILTER (WHERE day_of_week IS NULL) as null_dow,
            COUNT(*) FILTER (WHERE is_purchase IS NULL) as null_purchase,
            COUNT(DISTINCT categoryid) as unique_categories
        FROM statistics_events
        WHERE event_date = %s
    """, (yesterday,))

    feature_stats = cursor.fetchone()
    print(f"statistics_events 특성 검증: {feature_stats}")

    cursor.close()
    conn.close()

    return {
        'basic_stats': basic_stats,
        'feature_stats': feature_stats
    }


t2_stats_quality = PythonOperator(
    task_id='check_statistics_events_quality',
    python_callable=check_statistics_events_quality,
    dag=dag,
)


# ===== Task 3: ml_prepared_events 품질 검증 =====
def check_ml_events_quality(**context):
    """ml_prepared_events 테이블 품질 검증"""
    postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
    conn = postgres_hook.get_conn()
    cursor = conn.cursor()

    # 1. 정제 효율성 검증 (raw vs ml)
    cursor.execute("""
        SELECT
            (SELECT COUNT(*) FROM clickstream_events) as raw_count,
            (SELECT COUNT(*) FROM ml_prepared_events) as ml_count,
            ROUND(100.0 * (SELECT COUNT(*) FROM ml_prepared_events)
                  / NULLIF((SELECT COUNT(*) FROM clickstream_events), 0), 2) as retention_rate
    """)

    quality_stats = cursor.fetchone()
    print(f"ml_prepared_events 정제율: {quality_stats}")

    # 2. 특성 완성도 검증
    cursor.execute("""
        SELECT
            COUNT(*) as total,
            COUNT(*) FILTER (WHERE is_buyer IS NULL) as null_is_buyer,
            COUNT(*) FILTER (WHERE event_hour IS NULL) as null_hour,
            COUNT(*) FILTER (WHERE user_event_count IS NULL) as null_user_count,
            COUNT(*) FILTER (WHERE item_event_count IS NULL) as null_item_count
        FROM ml_prepared_events
        LIMIT 1
    """)

    feature_stats = cursor.fetchone()
    print(f"ml_prepared_events 특성 검증: {feature_stats}")

    cursor.close()
    conn.close()

    return {
        'quality_stats': quality_stats,
        'feature_stats': feature_stats
    }


t3_ml_quality = PythonOperator(
    task_id='check_ml_events_quality',
    python_callable=check_ml_events_quality,
    dag=dag,
)


# ===== Task 4: 품질 보고서 생성 =====
t4_quality_report = PostgresOperator(
    task_id='generate_quality_report',
    postgres_conn_id='postgres_default',
    sql="""
        -- 품질 메트릭 저장 (향후 대시보드용)
        -- 현재는 로그로 기록
        SELECT
            CURRENT_DATE as check_date,
            'raw_events' as table_name,
            COUNT(*) as record_count,
            COUNT(*) FILTER (WHERE visitorid IS NULL
                OR itemid IS NULL
                OR event IS NULL
                OR timestamp IS NULL) as anomaly_count,
            ROUND(100.0 * COUNT(*) FILTER (WHERE visitorid IS NOT NULL
                AND itemid IS NOT NULL
                AND event IS NOT NULL
                AND timestamp IS NOT NULL)
              / NULLIF(COUNT(*), 0), 2) as quality_score
        FROM clickstream_events
        WHERE DATE(created_at) = CURRENT_DATE - INTERVAL '1 day'
    """,
    dag=dag,
)


# ===== Task 의존성 =====
[t1_raw_quality, t2_stats_quality, t3_ml_quality] >> t4_quality_report
