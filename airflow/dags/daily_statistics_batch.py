"""
일일 통계 수집 DAG (Spark 기반)
매일 새벽 2시에 clickstream_events에서 일일 통계를 집계하여 daily_statistics에 저장
"""

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta
import os
from pathlib import Path

# 기본 설정
AIRFLOW_HOME = os.getenv('AIRFLOW_HOME', '/opt/airflow')
if os.path.exists('/opt/airflow/project'):
    PROJECT_ROOT = '/opt/airflow/project'
else:
    PROJECT_ROOT = str(Path(AIRFLOW_HOME).parent)

# DAG 설정
default_args = {
    'owner': 'analytics-team',
    'depends_on_past': False, # 어제 실패해도 그냥실행 되게 
    'start_date': datetime(2025, 11, 1),
    'email_on_failure': False,
    'retries': 2, # 2회 재시도
    'retry_delay': timedelta(minutes=5), # 재시도 하기전 5분 텀
}

dag = DAG(
    'daily_statistics_batch',
    default_args=default_args,
    description='일일 통계 수집 (Spark) - 매출, 이벤트수, 방문자수',
    schedule_interval='0 2 * * *',  # 매일 새벽 2시
    catchup=False, # start_date 이후의 과거 날짜를 한번에 몰아서 실행할지?  False->오늘 부터 앞으로만 스케줄 , 과거 분량은 안 메꿈
    tags=['statistics', 'spark', 'analytics'],
)

# Spark 공통 설정 
spark_conf = {
    'spark.driver.memory': '2g', # Dricver 프로세스 할당할메모리
    'spark.executor.memory': '2g', # Executor (연산수행하는 워커) 하나당 메모리 용량
    'spark.executor.cores': '2', #Executor당 CPU 코어 수. 병렬 작업 수에 영향
    'spark.network.timeout': '600s', # 네트워크 통신 타임아웃 (10분)
    'spark.task.maxFailures': '2' # 하나의 task가 실패했을 때, 몇 번까지 재시도할지
}

# ===== Task: Spark로 일일 통계 수집 =====
# spark dag submit operator 최적화 할려면?
t_batch_statistics = SparkSubmitOperator(
    task_id='collect_daily_statistics',
    application=f'{PROJECT_ROOT}/airflow/dags/spark_jobs/batch_statistics.py',  # python script 경로
    conf=spark_conf,
    packages='org.postgresql:postgresql:42.6.0', # Spark가 JDBC로 PostgreSQL에 붙을 때 필요한 드라이버
    jars=f'{PROJECT_ROOT}/src/spark/jars/postgresql-42.7.3.jar', # 로컬에 있는 jar 파일도 추가로 넣어줌
    py_files=[
        f'{PROJECT_ROOT}/config/settings.py',
        f'{PROJECT_ROOT}/src/utils/logger.py'
    ],
    driver_memory='2g',   # Spark driver 메모리
    executor_memory='2g', # executor 메모리 설정
    executor_cores=2,  # executor당 코어 수
    num_executors=1,  # executor 개수 1개
    dag=dag,
)

# Task 실행
t_batch_statistics
