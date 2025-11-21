"""
데이터 무결성 검증 DAG (Spark 기반)
매일 새벽 3시에 raw_clickstream_events와 clickstream_events의 데이터 품질을 검증하여 data_quality_results에 저장

검증 항목:
- Raw Data: 레코드 개수, NULL 값, 유효하지 않은 이벤트 타입
- Processed Data: 처리 완료 여부, 데이터 품질 점수
"""

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
from pathlib import Path
import psycopg2
from psycopg2 import Error
import logging

logger = logging.getLogger(__name__)

# 기본 설정
AIRFLOW_HOME = os.getenv('AIRFLOW_HOME', '/opt/airflow')
if os.path.exists('/opt/airflow/project'):
    PROJECT_ROOT = '/opt/airflow/project'
else:
    PROJECT_ROOT = str(Path(AIRFLOW_HOME).parent)

# DAG 설정
default_args = {
    'owner': 'analytics-team',
    'depends_on_past': False,
    'start_date': datetime(2025, 11, 1),
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'data_quality_check',
    default_args=default_args,
    description='데이터 무결성 검증 (Spark) - Raw Data & Processed Data',
    schedule_interval='0 3 * * *',  # 매일 새벽 3시 (daily_statistics_batch 이후)
    catchup=False,
    tags=['data-quality', 'spark', 'validation'],
)

# Spark 공통 설정
spark_conf = {
    'spark.driver.memory': '2g',
    'spark.executor.memory': '2g',
    'spark.executor.cores': '2',
    'spark.network.timeout': '600s',
    'spark.task.maxFailures': '2'
}

# ===== Task 1: Spark 데이터 품질 검증 (메인) =====
t_spark_quality_check = SparkSubmitOperator(
    task_id='spark_data_quality_check',
    application=f'{PROJECT_ROOT}/src/spark/jobs/data_quality_check.py',
    conf=spark_conf,
    packages='org.postgresql:postgresql:42.6.0',
    jars=f'{PROJECT_ROOT}/src/spark/jars/postgresql-42.7.3.jar',
    py_files=[
        f'{PROJECT_ROOT}/config/settings.py',
        f'{PROJECT_ROOT}/src/utils/logger.py'
    ],
    driver_memory='2g',
    executor_memory='2g',
    executor_cores=2,
    num_executors=3,
    master='spark://spark-master:7077',  # Docker Cluster 마스터
    deploy_mode='client',
    dag=dag,
)

# ===== Task 2: 검증 결과 알림 =====
def notify_quality_result(**context):
    """
    데이터 품질 검증 결과를 조회하여 Slack 알림

    실패 조건:
    - status = 'FAILED' → 빨강 알림
    - status = 'WARNING' → 노랑 알림
    - status = 'PASSED' → 초록 알림 (선택)
    """

    try:
        # PostgreSQL 연결 (Docker 네트워크)
        conn = psycopg2.connect(
            host='postgres',
            port=5432,
            database='ecommerce',
            user='admin',
            password='admin123'
        )

        cursor = conn.cursor()

        # 어제 검증 결과 조회
        cursor.execute("""
            SELECT table_name, quality_score, status, details
            FROM data_quality_results
            WHERE check_date = CURRENT_DATE
            ORDER BY created_at DESC
        """)

        results = cursor.fetchall()

        if not results:
            logger.warning("어제 검증 결과가 없습니다")
            cursor.close()
            conn.close()
            return

        # 검증 결과 로깅
        logger.info("=" * 60)
        logger.info("📊 데이터 품질 검증 결과")
        logger.info("=" * 60)

        has_failure = False
        has_warning = False

        for table_name, quality_score, status, details in results:
            logger.info(f"\n📌 테이블: {table_name}")
            logger.info(f"   점수: {quality_score:.2%}")
            logger.info(f"   상태: {status}")

            if status == 'FAILED':
                has_failure = True
                logger.error(f"   ❌ FAILED: {details}")
            elif status == 'WARNING':
                has_warning = True
                logger.warning(f"   ⚠️  WARNING: {details}")
            else:
                logger.info(f"   ✅ PASSED")

        logger.info("=" * 60)

        # TODO: Slack 알림 연동 (나중에 구현)
        # if has_failure:
        #     send_slack_alert("🔴 데이터 품질 검증 실패", results, "danger")
        # elif has_warning:
        #     send_slack_alert("🟡 데이터 품질 경고", results, "warning")
        # else:
        #     send_slack_alert("🟢 데이터 품질 정상", results, "good")

        cursor.close()
        conn.close()

    except Error as e:
        logger.error(f"데이터베이스 연결 오류: {e}")
        raise


t_notify_result = PythonOperator(
    task_id='notify_quality_result',
    python_callable=notify_quality_result,
    provide_context=True,
    dag=dag,
)

# ===== Task 의존성 =====
t_spark_quality_check >> t_notify_result
