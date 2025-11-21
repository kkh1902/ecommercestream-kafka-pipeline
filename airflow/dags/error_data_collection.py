"""
에러 데이터 수집 DAG (Python 기반)
매시간 error_data topic에서 에러를 읽어 PostgreSQL에 저장하고 통계를 계산

스케줄: 매시간 정각 (0 * * * *)
예: 00:00, 01:00, 02:00, ... , 23:00
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
from pathlib import Path
import sys
import logging

logger = logging.getLogger(__name__)

# 기본 설정
AIRFLOW_HOME = os.getenv('AIRFLOW_HOME', '/opt/airflow')
if os.path.exists('/opt/airflow/project'):
    PROJECT_ROOT = '/opt/airflow/project'
else:
    PROJECT_ROOT = str(Path(AIRFLOW_HOME).parent)

# 프로젝트 경로 추가
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

# DAG 설정
default_args = {
    'owner': 'analytics-team',
    'depends_on_past': False,
    'start_date': datetime(2025, 11, 1),
    'email_on_failure': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'error_data_collection',
    default_args=default_args,
    description='에러 데이터 수집 및 분석 - Kafka error_data topic',
    schedule_interval='0 * * * *',  # 매시간 정각
    catchup=False,
    tags=['error-handling', 'kafka', 'analytics'],
)

# ============================================================
# Task 1: error_data topic lag 확인
# ============================================================

def check_error_topic_lag(**context):
    """
    error_data topic의 미처리 메시지 개수 확인

    반환:
    {
        'lag': int (미처리 메시지 개수),
        'status': str ('NORMAL', 'WARNING', 'CRITICAL'),
        'message': str
    }
    """
    try:
        from kafka import KafkaConsumer, TopicPartition
        from kafka.errors import KafkaError

        bootstrap_servers = 'kafka-broker-1:19092,kafka-broker-2:19093,kafka-broker-3:19094'

        consumer = KafkaConsumer(
            bootstrap_servers=bootstrap_servers.split(','),
            group_id='error_consumer_group',
            enable_auto_commit=False
        )

        partitions = consumer.partitions_for_topic('error_data')
        if not partitions:
            logger.warning("⚠️  error_data topic이 없거나 비어있습니다")
            return {
                'lag': 0,
                'status': 'WARNING',
                'message': 'error_data topic not found'
            }

        total_lag = 0

        for partition in partitions:
            tp = TopicPartition('error_data', partition)
            consumer.assign([tp])

            # 마지막 offset
            consumer.seek_to_end(tp)
            end_offset = consumer.position(tp)

            # Consumer group의 현재 offset
            consumer.seek_to_beginning(tp)
            beginning_offset = consumer.position(tp)

            # Consumer group 조회 (admin client 필요)
            # 간단히: end_offset - current_offset 으로 추정
            lag = end_offset - beginning_offset
            total_lag += lag

            logger.info(f"📌 Partition {partition}: lag = {lag}")

        consumer.close()

        # 상태 결정
        if total_lag == 0:
            status = 'NORMAL'
        elif total_lag < 1000:
            status = 'NORMAL'
        elif total_lag < 5000:
            status = 'WARNING'
        else:
            status = 'CRITICAL'

        result = {
            'lag': total_lag,
            'status': status,
            'message': f"Total lag: {total_lag} messages"
        }

        logger.info(f"✅ Lag 확인 완료: {result}")

        # XCom에 저장 (다음 Task에서 사용)
        context['task_instance'].xcom_push(
            key='error_topic_lag',
            value=result
        )

        return result

    except Exception as e:
        logger.error(f"❌ Lag 확인 실패: {e}")
        raise


t_check_lag = PythonOperator(
    task_id='check_error_topic_lag',
    python_callable=check_error_topic_lag,
    provide_context=True,
    dag=dag,
)

# ============================================================
# Task 2: 에러 데이터 수집 및 저장
# ============================================================

def collect_and_store_errors(**context):
    """
    error_data topic에서 에러를 읽어 PostgreSQL에 저장

    반환:
    {
        'collected': int (수집한 에러 개수),
        'stored': int (저장된 에러 개수),
        'failed': int (저장 실패 개수)
    }
    """
    try:
        from src.kafka.error_consumer import ErrorDataConsumer

        # Docker 환경 감지
        if os.getenv('AIRFLOW_HOME'):
            bootstrap_servers = 'kafka-broker-1:19092,kafka-broker-2:19093,kafka-broker-3:19094'
            postgres_host = 'postgres'
        else:
            bootstrap_servers = 'localhost:9092'
            postgres_host = 'localhost'

        postgres_config = {
            'host': postgres_host,
            'port': 5432,
            'database': 'ecommerce',
            'user': 'admin',
            'password': 'admin123'
        }

        # 에러 컨슈머 초기화
        logger.info("📥 Error Consumer 초기화 중...")
        consumer = ErrorDataConsumer(
            bootstrap_servers=bootstrap_servers,
            group_id='error_consumer_group',
            postgres_config=postgres_config
        )

        # 에러 메시지 소비 (최대 100개, 10초 대기)
        logger.info("📥 error_data topic 읽기 중...")
        errors = consumer.consume_errors(max_records=100, timeout_ms=10000)

        logger.info(f"✅ {len(errors)}개 에러 수집 완료")

        # 에러를 DB에 저장
        if errors:
            stored_count = consumer.store_errors_to_db(errors)
            logger.info(f"✅ {stored_count}/{len(errors)}개 에러 저장 완료")
        else:
            stored_count = 0
            logger.info("✅ 처리할 에러가 없습니다")

        consumer.close()

        result = {
            'collected': len(errors),
            'stored': stored_count,
            'failed': len(errors) - stored_count
        }

        logger.info(f"📊 수집 결과: {result}")

        # XCom에 저장
        context['task_instance'].xcom_push(
            key='error_collection_result',
            value=result
        )

        return result

    except Exception as e:
        logger.error(f"❌ 에러 수집 실패: {e}")
        raise


t_collect_errors = PythonOperator(
    task_id='collect_and_store_errors',
    python_callable=collect_and_store_errors,
    provide_context=True,
    dag=dag,
)

# ============================================================
# Task 3: 에러 통계 계산
# ============================================================

def calculate_error_statistics(**context):
    """
    지난 1시간의 에러 통계 계산 및 저장

    반환:
    {
        'total_errors': int,
        'critical_count': int,
        'pending_count': int,
        'by_error_type': dict,
        'by_module': dict
    }
    """
    try:
        from src.kafka.error_consumer import ErrorDataConsumer

        # Docker 환경 감지
        if os.getenv('AIRFLOW_HOME'):
            postgres_host = 'postgres'
        else:
            postgres_host = 'localhost'

        postgres_config = {
            'host': postgres_host,
            'port': 5432,
            'database': 'ecommerce',
            'user': 'admin',
            'password': 'admin123'
        }

        # 에러 컨슈머 초기화 (에러 소비 없이 통계만)
        consumer = ErrorDataConsumer(
            bootstrap_servers='localhost:9092',  # 실제로는 사용 안 함
            group_id='error_consumer_group',
            postgres_config=postgres_config
        )

        # 통계 계산
        logger.info("📊 에러 통계 계산 중...")
        stats = consumer.calculate_statistics()

        logger.info(f"📊 통계 계산 완료:")
        logger.info(f"   총 에러: {stats['total_errors']}개")
        logger.info(f"   Critical: {stats['critical_count']}개")
        logger.info(f"   Pending: {stats['pending_count']}개")

        # 통계를 DB에 저장
        stored_count = consumer.store_statistics_to_db(stats)
        logger.info(f"✅ {stored_count}개 통계 저장 완료")

        consumer.close()

        # XCom에 저장
        context['task_instance'].xcom_push(
            key='error_statistics',
            value=stats
        )

        return stats

    except Exception as e:
        logger.error(f"❌ 통계 계산 실패: {e}")
        raise


t_calculate_stats = PythonOperator(
    task_id='calculate_error_statistics',
    python_callable=calculate_error_statistics,
    provide_context=True,
    dag=dag,
)

# ============================================================
# Task 4: 에러 알림
# ============================================================

def notify_error_summary(**context):
    """
    에러 통계를 기반으로 Slack 알림 발송

    알림 조건:
    - critical_count > 0 → 🔴 Critical 알림
    - pending_count > 1000 → 🟡 Warning 알림
    - 정상 → 🟢 Success 알림 (선택)
    """
    try:
        from src.utils.error_notifier import ErrorNotifier
        import os

        # XCom에서 통계 가져오기
        task_instance = context['task_instance']
        stats = task_instance.xcom_pull(
            task_ids='calculate_error_statistics',
            key='error_statistics'
        )

        if not stats:
            logger.warning("⚠️  통계 정보가 없습니다")
            return {'status': 'skipped', 'message': 'No statistics available'}

        # Slack Webhook URL (환경변수 또는 설정)
        slack_webhook_url = os.getenv('SLACK_WEBHOOK_URL', '')

        notifier = ErrorNotifier(slack_webhook_url=slack_webhook_url if slack_webhook_url else None)

        # 알림 로직
        critical_count = stats.get('critical_count', 0)
        pending_count = stats.get('pending_count', 0)
        total_errors = stats.get('total_errors', 0)

        logger.info(f"📊 알림 판단: Critical={critical_count}, Pending={pending_count}")

        result = {'status': 'normal', 'alerts_sent': 0}

        if critical_count > 0:
            logger.error(f"🔴 Critical 에러 감지: {critical_count}개")
            success = notifier.send_critical_alert(
                critical_count=critical_count,
                error_types=list(stats.get('by_error_type', {}).keys()),
                additional_info={
                    'total_errors': total_errors,
                    'by_error_type': stats.get('by_error_type', {})
                },
                channel='#kafka-error'
            )
            if success:
                result['alerts_sent'] += 1

        elif pending_count > 1000:
            logger.warning(f"🟡 Warning: Pending 에러 많음: {pending_count}개")
            success = notifier.send_warning_alert(
                pending_count=pending_count,
                error_types=stats.get('by_error_type', {}),
                additional_info={
                    'total_errors': total_errors,
                    'by_module': stats.get('by_module', {})
                },
                channel='#kafka-error'
            )
            if success:
                result['alerts_sent'] += 1

        else:
            logger.info(f"🟢 정상: 에러 처리 완료 (총 {total_errors}개)")
            success = notifier.send_success_alert(
                total_errors=total_errors,
                stats=stats,
                channel='#kafka-error'
            )
            if success:
                result['alerts_sent'] += 1

        logger.info(f"✅ 알림 발송 완료: {result}")

        # XCom에 저장
        task_instance.xcom_push(
            key='notification_result',
            value=result
        )

        return result

    except Exception as e:
        logger.error(f"❌ 알림 발송 실패: {e}")
        # 알림 실패는 DAG 실패로 처리하지 않음 (로깅만)
        return {'status': 'failed', 'error': str(e)}


t_notify = PythonOperator(
    task_id='notify_error_summary',
    python_callable=notify_error_summary,
    provide_context=True,
    dag=dag,
)

# ============================================================
# Task 의존성
# ============================================================

t_check_lag >> t_collect_errors >> t_calculate_stats >> t_notify
