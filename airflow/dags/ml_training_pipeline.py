"""
ML 모델 학습 파이프라인 DAG
매주 월요일 새벽 4시에 추천 모델 재학습
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# 기본 설정
default_args = {
    'owner': 'ml-team',
    'depends_on_past': False,
    'start_date': datetime(2025, 11, 1),
    'email_on_failure': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'ml_training_pipeline',
    default_args=default_args,
    description='ML 모델 학습 파이프라인',
    schedule_interval='0 4 * * 1',  # 매주 월요일 새벽 4시
    catchup=False,
    tags=['ml', 'model-training'],
)


# ===== Task 1: 데이터 준비 =====
def prepare_training_data(**context):
    """
    ml_prepared_events에서 학습 데이터 준비
    - 특성 선택
    - 데이터 분할 (train/test)
    - 정규화
    """
    # TODO: 구현 필요
    print("[TODO] 학습 데이터 준비")
    pass


t1_prepare_data = PythonOperator(
    task_id='prepare_training_data',
    python_callable=prepare_training_data,
    dag=dag,
)


# ===== Task 2: 모델 학습 =====
def train_recommendation_model(**context):
    """
    추천 모델 학습
    - 알고리즘: TBD (Collaborative Filtering, Matrix Factorization 등)
    - 하이퍼파라미터 튜닝
    - 모델 저장
    """
    # TODO: 구현 필요
    print("[TODO] 추천 모델 학습")
    pass


t2_train_model = PythonOperator(
    task_id='train_recommendation_model',
    python_callable=train_recommendation_model,
    dag=dag,
)


# ===== Task 3: 모델 평가 =====
def evaluate_model(**context):
    """
    학습된 모델 평가
    - 정확도 (Precision, Recall, NDCG 등)
    - 기존 모델과 비교
    - 배포 가능성 판단
    """
    # TODO: 구현 필요
    print("[TODO] 모델 평가")
    pass


t3_evaluate = PythonOperator(
    task_id='evaluate_model',
    python_callable=evaluate_model,
    dag=dag,
)


# ===== Task 4: 모델 배포 =====
def deploy_model(**context):
    """
    새 모델 배포
    - 모델 버전 관리
    - A/B 테스트 (선택사항)
    - 실시간 예측 서빙
    """
    # TODO: 구현 필요
    print("[TODO] 모델 배포")
    pass


t4_deploy = PythonOperator(
    task_id='deploy_model',
    python_callable=deploy_model,
    dag=dag,
)


# ===== Task 5: 모니터링 =====
def monitor_model_performance(**context):
    """
    배포된 모델 모니터링
    - 예측 성능 추적
    - 데이터 드리프트 감지
    - 성능 저하 시 알림
    """
    # TODO: 구현 필요
    print("[TODO] 모델 성능 모니터링")
    pass


t5_monitor = PythonOperator(
    task_id='monitor_model_performance',
    python_callable=monitor_model_performance,
    dag=dag,
)


# ===== Task 의존성 =====
t1_prepare_data >> t2_train_model >> t3_evaluate >> t4_deploy >> t5_monitor
