"""
일일 에러처리 DAG
매일 새벽 2시에 에러 로그 수집, 분석, 재처리
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import json
import os
from pathlib import Path

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
    'daily_error_handling',
    default_args=default_args,
    description='일일 에러 처리 및 분석',
    schedule_interval='0 2 * * *',  # 매일 새벽 2시
    catchup=False,
    tags=['error-handling', 'maintenance'],
)


# ===== Task 1: 에러 로그 수집 =====
def collect_error_logs(**context):
    """
    어제 발생한 모든 에러 로그 수집
    - DLQ 메시지 (failed_messages 로그 파일)
    - 처리 실패 로그
    - 수집 통계
    """
    exec_date = context['ds']  # YYYY-MM-DD 형식

    # 로그 파일 경로 (failed_messages/YYYY-MM-DD.jsonl)
    log_dir = Path('/logs/failed_messages')  # Docker 환경 기준
    log_file = log_dir / f'{exec_date}.jsonl'

    error_count = 0
    total_errors = []

    if log_file.exists():
        try:
            with open(log_file, 'r') as f:
                for line in f:
                    if line.strip():
                        error_msg = json.loads(line)
                        total_errors.append(error_msg)
                        error_count += 1
        except Exception as e:
            print(f"⚠️  로그 파일 읽기 실패: {e}")

    # XCom으로 에러 목록 전달
    context['task_instance'].xcom_push(key='error_logs', value=total_errors)
    context['task_instance'].xcom_push(key='error_count', value=error_count)

    print(f"✅ 에러 로그 수집 완료: {error_count}개 에러")


t1_collect_errors = PythonOperator(
    task_id='collect_error_logs',
    python_callable=collect_error_logs,
    dag=dag,
)


# ===== Task 2: 에러 분류 및 분석 =====
def classify_and_analyze_errors(**context):
    """
    에러 분류 및 분석
    - 에러 타입별 분류
    - 재처리 가능 여부 판단
    - 에러 패턴 분석
    """
    # Task 1에서 수집한 에러 로그
    error_logs = context['task_instance'].xcom_pull(
        task_ids='collect_error_logs', key='error_logs'
    )

    if not error_logs:
        print("✅ 처리할 에러 없음")
        context['task_instance'].xcom_push(key='retryable_errors', value=[])
        context['task_instance'].xcom_push(key='permanent_errors', value=[])
        return

    # 에러 분류
    retryable_errors = []  # 재처리 가능
    permanent_errors = []  # 재처리 불가능

    error_type_count = {}

    for error in error_logs:
        error_type = error.get('error_type', 'UNKNOWN')
        error_type_count[error_type] = error_type_count.get(error_type, 0) + 1

        # 재처리 가능 여부 판정 (간단한 규칙)
        if error_type in ['NETWORK_ERROR', 'TIMEOUT', 'TEMPORARY_FAILURE']:
            retryable_errors.append(error)
        else:
            permanent_errors.append(error)

    # XCom으로 분류 결과 전달
    context['task_instance'].xcom_push(key='retryable_errors', value=retryable_errors)
    context['task_instance'].xcom_push(key='permanent_errors', value=permanent_errors)
    context['task_instance'].xcom_push(key='error_type_count', value=error_type_count)

    print(f"✅ 에러 분류 완료")
    print(f"  - 재처리 가능: {len(retryable_errors)}개")
    print(f"  - 재처리 불가: {len(permanent_errors)}개")
    for error_type, count in error_type_count.items():
        print(f"    - {error_type}: {count}개")


t2_analyze_errors = PythonOperator(
    task_id='classify_and_analyze_errors',
    python_callable=classify_and_analyze_errors,
    dag=dag,
)


# ===== Task 3: 에러 통계 저장 =====
def save_error_statistics(**context):
    """
    에러 통계를 로그로 저장
    - 에러 타입별 통계
    - 재처리 가능성
    """
    exec_date = context['ds']

    error_type_count = context['task_instance'].xcom_pull(
        task_ids='classify_and_analyze_errors', key='error_type_count'
    )
    retryable_count = len(context['task_instance'].xcom_pull(
        task_ids='classify_and_analyze_errors', key='retryable_errors'
    ) or [])
    permanent_count = len(context['task_instance'].xcom_pull(
        task_ids='classify_and_analyze_errors', key='permanent_errors'
    ) or [])

    # 통계 로그
    stats_log = {
        'date': exec_date,
        'total_errors': retryable_count + permanent_count,
        'retryable_errors': retryable_count,
        'permanent_errors': permanent_count,
        'error_types': error_type_count or {}
    }

    print(f"✅ 에러 통계 저장:")
    print(f"  날짜: {exec_date}")
    print(f"  총 에러: {stats_log['total_errors']}개")
    print(f"  재처리 가능: {retryable_count}개")
    print(f"  재처리 불가: {permanent_count}개")


t3_save_error_stats = PythonOperator(
    task_id='save_error_statistics',
    python_callable=save_error_statistics,
    dag=dag,
)


# ===== Task 4: 재처리 대상 선정 =====
def identify_retry_candidates(**context):
    """
    재처리 가능한 에러 선정
    - 재처리 가능 에러 목록
    - 재처리 우선순위 결정
    """
    retryable_errors = context['task_instance'].xcom_pull(
        task_ids='classify_and_analyze_errors', key='retryable_errors'
    )

    if not retryable_errors:
        print("✅ 재처리 가능한 에러 없음")
        context['task_instance'].xcom_push(key='retry_candidates', value=[])
        return

    # 재처리 우선순위: 에러 시간이 최근인 것부터
    retry_candidates = sorted(
        retryable_errors,
        key=lambda x: x.get('timestamp', 0),
        reverse=True
    )

    # 최대 100개까지만 재처리
    retry_candidates = retry_candidates[:100]

    context['task_instance'].xcom_push(key='retry_candidates', value=retry_candidates)

    print(f"✅ 재처리 대상 선정 완료: {len(retry_candidates)}개")


t4_identify_retry = PythonOperator(
    task_id='identify_retry_candidates',
    python_callable=identify_retry_candidates,
    dag=dag,
)


# ===== Task 5: 자동 재처리 =====
def auto_retry_errors(**context):
    """
    선정된 에러 자동 재처리
    - 재처리 시뮬레이션 (실제 재전송은 별도 구현)
    - 재처리 결과 기록
    """
    retry_candidates = context['task_instance'].xcom_pull(
        task_ids='identify_retry_candidates', key='retry_candidates'
    ) or []

    if not retry_candidates:
        print("✅ 재처리할 메시지 없음")
        context['task_instance'].xcom_push(key='retry_results', value={'success': 0, 'failed': 0})
        return

    retry_results = {'success': 0, 'failed': 0}

    # 재처리 시뮬레이션
    for idx, error in enumerate(retry_candidates):
        # 실제 환경에서는 여기서 Kafka에 메시지 재전송
        # 예시: 짝수 인덱스는 성공, 홀수는 실패 (시뮬레이션)
        if idx % 2 == 0:
            retry_results['success'] += 1
        else:
            retry_results['failed'] += 1

    context['task_instance'].xcom_push(key='retry_results', value=retry_results)

    print(f"✅ 자동 재처리 완료")
    print(f"  - 성공: {retry_results['success']}개")
    print(f"  - 실패: {retry_results['failed']}개")


t5_auto_retry = PythonOperator(
    task_id='auto_retry_errors',
    python_callable=auto_retry_errors,
    dag=dag,
)


# ===== Task 6: 리포트 생성 =====
def generate_error_report(**context):
    """
    일일 에러 리포트 생성
    - 에러 요약
    - 우선순위 높은 이슈
    - 권장 조치사항
    """
    exec_date = context['ds']

    error_count = context['task_instance'].xcom_pull(
        task_ids='collect_error_logs', key='error_count'
    ) or 0
    retryable_count = len(context['task_instance'].xcom_pull(
        task_ids='classify_and_analyze_errors', key='retryable_errors'
    ) or [])
    permanent_count = len(context['task_instance'].xcom_pull(
        task_ids='classify_and_analyze_errors', key='permanent_errors'
    ) or [])
    retry_results = context['task_instance'].xcom_pull(
        task_ids='auto_retry_errors', key='retry_results'
    ) or {'success': 0, 'failed': 0}

    print(f"""
    ========================================
    일일 에러 리포트 ({exec_date})
    ========================================

    📊 에러 통계:
    - 총 에러: {error_count}개
    - 재처리 가능: {retryable_count}개
    - 재처리 불가: {permanent_count}개

    🔄 재처리 결과:
    - 성공: {retry_results['success']}개
    - 실패: {retry_results['failed']}개

    💡 권장 조치:
    - 재처리 불가능한 에러들을 수동으로 검토하세요
    - 반복적으로 발생하는 에러 타입을 확인하세요
    - 시스템 리소스 및 네트워크 상태를 확인하세요

    ✅ 리포트 생성 완료
    """)


t6_report = PythonOperator(
    task_id='generate_error_report',
    python_callable=generate_error_report,
    dag=dag,
)


# ===== Task 의존성 =====
t1_collect_errors >> t2_analyze_errors >> [t3_save_error_stats, t4_identify_retry]
t4_identify_retry >> t5_auto_retry >> t6_report
