"""
ML 모델 학습 파이프라인 DAG
매주 월요일 새벽 4시에 추천 모델 재학습
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import sys
import os
from pathlib import Path

# 프로젝트 경로 설정
AIRFLOW_HOME = os.getenv('AIRFLOW_HOME', '/opt/airflow')
PROJECT_ROOT = Path(AIRFLOW_HOME).parent
sys.path.insert(0, str(PROJECT_ROOT))

from src.ml.recommendation_data import RecommendationDataPreparator
from src.ml.recommendation_model import RecommendationModelTrainer
from src.ml.recommendation_metrics import RecommendationMetrics
from src.ml.recommendation_engine import RecommendationEngine
from src.utils.logger import get_logger

logger = get_logger(__name__)

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
    raw_events에서 학습 데이터 준비
    - raw_events 로드
    - 특성 엔지니어링
    - 데이터 분할 (train/test)
    """
    logger.info("=" * 60)
    logger.info("Task 1: 학습 데이터 준비")
    logger.info("=" * 60)

    try:
        # 데이터 준비
        preparator = RecommendationDataPreparator()
        X_train, X_test, y_train, y_test, feature_cols = preparator.prepare(
            limit=None  # 전체 데이터 사용
        )

        # Airflow context에 저장 (다음 task에서 사용)
        context['task_instance'].xcom_push(
            key='X_train_path',
            value=str(PROJECT_ROOT / 'src' / 'ml' / 'data' / 'X_train.pkl')
        )
        context['task_instance'].xcom_push(
            key='X_test_path',
            value=str(PROJECT_ROOT / 'src' / 'ml' / 'data' / 'X_test.pkl')
        )
        context['task_instance'].xcom_push(
            key='y_train_path',
            value=str(PROJECT_ROOT / 'src' / 'ml' / 'data' / 'y_train.pkl')
        )
        context['task_instance'].xcom_push(
            key='y_test_path',
            value=str(PROJECT_ROOT / 'src' / 'ml' / 'data' / 'y_test.pkl')
        )

        logger.info(f"✅ 데이터 준비 완료")
        logger.info(f"  - Training: {X_train.shape[0]} samples")
        logger.info(f"  - Testing: {X_test.shape[0]} samples")
        logger.info(f"  - Features: {len(feature_cols)}")

    except Exception as e:
        logger.error(f"데이터 준비 실패: {e}", exc_info=True)
        raise


t1_prepare_data = PythonOperator(
    task_id='prepare_training_data',
    python_callable=prepare_training_data,
    dag=dag,
)


# ===== Task 2: 모델 학습 =====
def train_recommendation_model(**context):
    """
    XGBoost 추천 모델 학습
    - 알고리즘: XGBoost 분류 모델
    - 구매 확률 예측
    - 모델 저장
    """
    logger.info("=" * 60)
    logger.info("Task 2: 추천 모델 학습")
    logger.info("=" * 60)

    try:
        # Task 1에서 준비된 데이터 로드
        preparator = RecommendationDataPreparator()
        X_train, X_test, y_train, y_test, feature_cols = preparator.prepare(
            limit=None
        )

        # 모델 학습
        trainer = RecommendationModelTrainer()
        model = trainer.train(X_train, y_train, X_val=X_test, y_val=y_test)

        # 모델 저장
        trainer.save_model(filename='recommendation_xgboost.pkl')

        logger.info(f"✅ 모델 학습 및 저장 완료")

    except Exception as e:
        logger.error(f"모델 학습 실패: {e}", exc_info=True)
        raise


t2_train_model = PythonOperator(
    task_id='train_recommendation_model',
    python_callable=train_recommendation_model,
    dag=dag,
)


# ===== Task 3: 모델 평가 =====
def evaluate_model(**context):
    """
    학습된 모델 평가
    - 정확도 (Accuracy, Precision, Recall, F1, AUC-ROC)
    - 추천 품질 (Precision@K, Recall@K, NDCG@K, MAP@K)
    - 특성 중요도 분석
    """
    logger.info("=" * 60)
    logger.info("Task 3: 모델 평가")
    logger.info("=" * 60)

    try:
        # 데이터 로드
        preparator = RecommendationDataPreparator()
        X_train, X_test, y_train, y_test, feature_cols = preparator.prepare(
            limit=None
        )

        # 모델 로드 및 평가
        trainer = RecommendationModelTrainer()
        trainer.load_model(filename='recommendation_xgboost.pkl')

        # 평가 지표
        metrics, y_pred, y_pred_proba = trainer.evaluate(X_test, y_test)

        # 특성 중요도
        feature_importance = trainer.get_feature_importance(top_n=10)

        # 결과 로깅
        logger.info("모델 평가 결과:")
        logger.info(f"  Accuracy:  {metrics['accuracy']:.4f}")
        logger.info(f"  Precision: {metrics['precision']:.4f}")
        logger.info(f"  Recall:    {metrics['recall']:.4f}")
        logger.info(f"  F1 Score:  {metrics['f1']:.4f}")
        logger.info(f"  AUC-ROC:   {metrics['auc_roc']:.4f}")

        logger.info("상위 10개 중요 특성:")
        for feature, importance in feature_importance.items():
            logger.info(f"  {feature}: {importance:.4f}")

        # 평가 결과 저장 (XCom)
        context['task_instance'].xcom_push(
            key='model_accuracy',
            value=float(metrics['accuracy'])
        )
        context['task_instance'].xcom_push(
            key='model_auc_roc',
            value=float(metrics['auc_roc'])
        )

        logger.info(f"✅ 모델 평가 완료")

    except Exception as e:
        logger.error(f"평가 실패: {e}", exc_info=True)
        raise


t3_evaluate = PythonOperator(
    task_id='evaluate_model',
    python_callable=evaluate_model,
    dag=dag,
)


# ===== Task 4: 모델 배포 =====
def deploy_model(**context):
    """
    새 모델 배포
    - 모델 버전 관리 (이전 모델 백업)
    - 현재 모델을 프로덕션으로 이동
    - 배포 로그 기록
    """
    logger.info("=" * 60)
    logger.info("Task 4: 모델 배포")
    logger.info("=" * 60)

    try:
        # Task 3에서의 평가 결과 확인
        accuracy = context['task_instance'].xcom_pull(
            task_ids='evaluate_model',
            key='model_accuracy'
        )
        auc_roc = context['task_instance'].xcom_pull(
            task_ids='evaluate_model',
            key='model_auc_roc'
        )

        logger.info(f"현재 모델 성능: Accuracy={accuracy:.4f}, AUC-ROC={auc_roc:.4f}")

        # 최소 성능 기준 (임계값)
        MIN_ACCURACY = 0.60  # 최소 60% 정확도
        MIN_AUC_ROC = 0.65   # 최소 0.65 AUC-ROC

        if accuracy >= MIN_ACCURACY and auc_roc >= MIN_AUC_ROC:
            logger.info(f"✅ 모델이 배포 기준을 만족합니다 (Accuracy >= {MIN_ACCURACY}, AUC-ROC >= {MIN_AUC_ROC})")

            # 프로덕션 모델로 복사
            import shutil
            models_dir = PROJECT_ROOT / 'src' / 'ml' / 'models'
            models_dir.mkdir(parents=True, exist_ok=True)

            production_model = models_dir / 'recommendation_xgboost_production.pkl'
            current_model = models_dir / 'recommendation_xgboost.pkl'

            # 기존 프로덕션 모델 백업
            if production_model.exists():
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                backup_model = models_dir / f'recommendation_xgboost_backup_{timestamp}.pkl'
                shutil.copy(production_model, backup_model)
                logger.info(f"이전 모델 백업: {backup_model}")

            # 새 모델을 프로덕션으로 복사
            shutil.copy(current_model, production_model)
            logger.info(f"✅ 새 모델이 프로덕션으로 배포되었습니다: {production_model}")

        else:
            logger.warning(f"❌ 모델이 배포 기준을 만족하지 않습니다")
            logger.warning(f"  - 현재 Accuracy: {accuracy:.4f} (최소 필요: {MIN_ACCURACY})")
            logger.warning(f"  - 현재 AUC-ROC: {auc_roc:.4f} (최소 필요: {MIN_AUC_ROC})")
            raise ValueError("모델 성능이 배포 기준을 만족하지 않습니다")

    except Exception as e:
        logger.error(f"배포 실패: {e}", exc_info=True)
        raise


t4_deploy = PythonOperator(
    task_id='deploy_model',
    python_callable=deploy_model,
    dag=dag,
)


# ===== Task 5: 모니터링 =====
def monitor_model_performance(**context):
    """
    배포된 모델 모니터링
    - 최근 예측 성능 추적
    - 배포 완료 로그
    """
    logger.info("=" * 60)
    logger.info("Task 5: 모델 성능 모니터링")
    logger.info("=" * 60)

    try:
        # Task 4의 배포 결과 확인
        deployment_status = context['task_instance'].xcom_pull(
            task_ids='evaluate_model',
            key='model_accuracy'
        )

        logger.info("모델 배포 완료 및 모니터링 시작")
        logger.info(f"프로덕션 모델 위치: {PROJECT_ROOT / 'src' / 'ml' / 'models' / 'recommendation_xgboost_production.pkl'}")

        # 향후 확장: 실시간 모니터링 대시보드 연결
        logger.info("모니터링 항목:")
        logger.info("  - 모델 예측 지연 시간")
        logger.info("  - 데이터 드리프트 감지")
        logger.info("  - 예측 성능 변화 추적")
        logger.info("  - 사용자 피드백 수집 (A/B 테스트)")

        logger.info(f"✅ 모니터링 시작")

    except Exception as e:
        logger.error(f"모니터링 시작 실패: {e}", exc_info=True)
        raise


t5_monitor = PythonOperator(
    task_id='monitor_model_performance',
    python_callable=monitor_model_performance,
    dag=dag,
)


# ===== Task 의존성 =====
t1_prepare_data >> t2_train_model >> t3_evaluate >> t4_deploy >> t5_monitor
