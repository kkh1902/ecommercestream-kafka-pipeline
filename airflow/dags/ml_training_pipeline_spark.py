"""
ML 모델 학습 파이프라인 DAG (Spark 통합)
매주 월요일 새벽 4시에 추천 모델 재학습

파이프라인:
1. Spark: raw_clickstream_events에서 데이터 정제 및 특성 엔지니어링
2. XGBoost: Spark에서 준비한 데이터로 모델 학습
3. 평가: 모델 성과 지표 계산
4. 배포: 성능 기준 충족 시 모델 배포
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import sys
import os
from pathlib import Path
import subprocess

# 프로젝트 경로 설정
AIRFLOW_HOME = os.getenv('AIRFLOW_HOME', '/opt/airflow')
PROJECT_ROOT = Path(AIRFLOW_HOME).parent
sys.path.insert(0, str(PROJECT_ROOT))

from src.ml.recommendation_model_spark import RecommendationModelTrainerSpark
from src.utils.logger import get_logger

logger = get_logger(__name__)

# Docker 환경 감지
IS_DOCKER = os.path.exists('/.dockerenv')

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
    'ml_training_pipeline_spark',
    default_args=default_args,
    description='ML 모델 학습 파이프라인 (Spark 통합)',
    schedule_interval='0 4 * * 1',  # 매주 월요일 새벽 4시
    catchup=False,
    tags=['ml', 'model-training', 'spark'],
)


# ===== Task 1: Spark 데이터 준비 =====
def prepare_data_with_spark(**context):
    """
    Spark Job으로 데이터 준비
    - raw_clickstream_events 로드
    - 특성 엔지니어링
    - Train/Test 분할
    - Parquet 저장
    """
    logger.info("=" * 60)
    logger.info("Task 1: Spark 데이터 준비")
    logger.info("=" * 60)

    try:
        # Docker 환경일 경우 경로 조정
        if IS_DOCKER:
            spark_job_path = "/opt/airflow/project/src/spark/jobs/spark_jobs/ml_data_preparation.py"
            jar_dir = "/opt/airflow/project/src/spark/jars"
        else:
            spark_job_path = str(PROJECT_ROOT / "src" / "spark" / "jobs" / "spark_jobs" / "ml_data_preparation.py")
            jar_dir = str(PROJECT_ROOT / "src" / "spark" / "jars")

        jar_files = [
            "postgresql-42.6.0.jar",
            "spark-sql-kafka-0-10_2.12-3.5.0.jar",
            "kafka-clients-3.5.1.jar",
            "commons-lang3-3.12.0.jar",
            "commons-pool2-2.11.1.jar"
        ]
        jar_paths = ",".join([f"{jar_dir}/{jar}" for jar in jar_files])

        # Spark-submit 명령어
        spark_command = [
            "spark-submit",
            "--master", "local[*]",
            "--jars", jar_paths,
            spark_job_path
        ]

        logger.info(f"Spark Job 실행 중: {spark_job_path}")
        logger.info(f"명령어: {' '.join(spark_command)}")

        # subprocess로 실행 (더 안정적)
        result = subprocess.run(
            spark_command,
            capture_output=True,
            text=True,
            timeout=3600  # 1시간 타임아웃
        )

        if result.returncode != 0:
            logger.error(f"Spark 작업 실패:")
            logger.error(f"STDOUT: {result.stdout}")
            logger.error(f"STDERR: {result.stderr}")
            raise RuntimeError(f"Spark 작업 실패: {result.stderr}")

        logger.info(f"Spark 작업 출력:\n{result.stdout}")
        logger.info("✅ Spark 데이터 준비 완료")

        # Airflow context에 저장
        context['task_instance'].xcom_push(
            key='data_prepared',
            value=True
        )

    except subprocess.TimeoutExpired:
        logger.error("Spark 작업 타임아웃 (1시간 초과)")
        raise
    except Exception as e:
        logger.error(f"Spark 작업 실패: {e}", exc_info=True)
        raise


# ===== Task 2: XGBoost 모델 학습 =====
def train_xgboost_model(**context):
    """
    Spark에서 준비한 Parquet 데이터로 XGBoost 모델 학습
    """
    logger.info("=" * 60)
    logger.info("Task 2: XGBoost 모델 학습")
    logger.info("=" * 60)

    try:
        # Spark 데이터 준비 확인
        data_prepared = context['task_instance'].xcom_pull(
            task_ids='prepare_data_with_spark',
            key='data_prepared'
        )

        if not data_prepared:
            raise RuntimeError("Spark 데이터 준비 실패")

        # Docker 환경일 경우 경로 조정
        if IS_DOCKER:
            data_dir = "/opt/airflow/project/src/ml/data"
            model_save_path = "/opt/airflow/project/src/ml/models"
        else:
            data_dir = str(PROJECT_ROOT / "src" / "ml" / "data")
            model_save_path = str(PROJECT_ROOT / "src" / "ml" / "models")

        logger.info(f"데이터 디렉토리: {data_dir}")
        logger.info(f"모델 저장 경로: {model_save_path}")

        # 모델 학습
        trainer = RecommendationModelTrainerSpark(
            model_save_path=model_save_path,
            data_dir=data_dir
        )
        X_train, X_test, y_train, y_test, feature_cols = trainer.load_spark_data()
        trainer.feature_columns = feature_cols

        # 모델 생성 및 학습
        trainer.create_model(
            n_estimators=100,
            max_depth=6,
            learning_rate=0.1
        )
        trainer.train(X_train, y_train)

        # 모델 평가
        metrics = trainer.evaluate(X_test, y_test)

        # 모델 저장
        trainer.save_model()

        # Airflow context에 저장 (다음 task에서 사용)
        # dict에서 Serialization 불가능한 값 제거
        metrics_clean = {
            'accuracy': float(metrics.get('accuracy', 0)),
            'precision': float(metrics.get('precision', 0)),
            'recall': float(metrics.get('recall', 0)),
            'f1': float(metrics.get('f1', 0)),
            'auc_roc': float(metrics.get('auc_roc', 0)),
        }

        context['task_instance'].xcom_push(
            key='model_metrics',
            value=metrics_clean
        )

        logger.info("✅ 모델 학습 및 평가 완료")

    except Exception as e:
        logger.error(f"모델 학습 실패: {e}", exc_info=True)
        raise


# ===== Task 3: 모델 배포 여부 결정 =====
def decide_deployment(**context):
    """
    모델 성과 지표 기반으로 배포 여부 결정
    - Accuracy >= 60%
    - AUC-ROC >= 0.65
    """
    logger.info("=" * 60)
    logger.info("Task 3: 모델 배포 여부 결정")
    logger.info("=" * 60)

    try:
        metrics = context['task_instance'].xcom_pull(
            task_ids='train_xgboost_model',
            key='model_metrics'
        )

        accuracy = metrics.get('accuracy', 0)
        auc_roc = metrics.get('auc_roc', 0)

        logger.info(f"모델 성과:")
        logger.info(f"  - Accuracy: {accuracy:.4f}")
        logger.info(f"  - AUC-ROC: {auc_roc:.4f}")

        # 배포 기준
        MIN_ACCURACY = 0.60
        MIN_AUC_ROC = 0.65

        should_deploy = (accuracy >= MIN_ACCURACY) and (auc_roc >= MIN_AUC_ROC)

        if should_deploy:
            logger.info(f"✅ 배포 기준 충족! 모델을 배포합니다.")
        else:
            logger.warning(f"⚠️ 배포 기준 미충족:")
            if accuracy < MIN_ACCURACY:
                logger.warning(f"   - Accuracy {accuracy:.4f} < {MIN_ACCURACY}")
            if auc_roc < MIN_AUC_ROC:
                logger.warning(f"   - AUC-ROC {auc_roc:.4f} < {MIN_AUC_ROC}")

        # Airflow context에 저장
        context['task_instance'].xcom_push(
            key='should_deploy',
            value=should_deploy
        )

    except Exception as e:
        logger.error(f"배포 결정 실패: {e}", exc_info=True)
        raise


# ===== Task 4: 모델 배포 =====
def deploy_model(**context):
    """
    성능 기준을 충족하는 경우 모델 배포
    """
    import shutil

    logger.info("=" * 60)
    logger.info("Task 4: 모델 배포")
    logger.info("=" * 60)

    try:
        should_deploy = context['task_instance'].xcom_pull(
            task_ids='decide_deployment',
            key='should_deploy'
        )

        # Docker 환경일 경우 경로 조정
        if IS_DOCKER:
            model_dir = Path("/opt/airflow/project/src/ml/models")
        else:
            model_dir = PROJECT_ROOT / "src" / "ml" / "models"

        model_file = model_dir / "recommendation_xgboost.pkl"

        if should_deploy:
            # 모델 백업 (기존 프로덕션 모델)
            backup_file = model_dir / f"recommendation_xgboost_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.pkl"

            if model_file.exists():
                shutil.copy2(str(model_file), str(backup_file))
                logger.info(f"✅ 기존 모델 백업: {backup_file}")

            # 현재 모델이 이미 저장되어 있음
            logger.info(f"✅ 모델 배포 완료!")
            logger.info(f"   - 모델 경로: {model_file}")

        else:
            logger.warning("⚠️ 배포 기준 미충족으로 모델 배포 스킵")

    except Exception as e:
        logger.error(f"모델 배포 실패: {e}", exc_info=True)
        raise


# ===== DAG Tasks =====

task_prepare_data = PythonOperator(
    task_id='prepare_data_with_spark',
    python_callable=prepare_data_with_spark,
    provide_context=True,
    dag=dag,
)

task_train_model = PythonOperator(
    task_id='train_xgboost_model',
    python_callable=train_xgboost_model,
    provide_context=True,
    dag=dag,
)

task_decide = PythonOperator(
    task_id='decide_deployment',
    python_callable=decide_deployment,
    provide_context=True,
    dag=dag,
)

task_deploy = PythonOperator(
    task_id='deploy_model',
    python_callable=deploy_model,
    provide_context=True,
    dag=dag,
)

# Task 의존성
task_prepare_data >> task_train_model >> task_decide >> task_deploy
