"""
DLQ (Dead Letter Queue) 배치 처리 DAG

스케줄:
- 매일 오전 2시: 전날 에러 로그 처리
  1. 로그 파일 수집
  2. PostgreSQL 저장
  3. 에러 통계 집계
  4. 재처리 시도
  5. 일일 리포트
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import json
import glob
from pathlib import Path
import sys
import os

# 프로젝트 루트 경로 추가
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from config.settings import KAFKA_BROKERS, KAFKA_TOPIC

# 기본 설정
default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'start_date': datetime(2025, 11, 1),
    'email_on_failure': False,
    'email': ['admin@company.com'],
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'dlq_batch_processing',
    default_args=default_args,
    description='DLQ 배치 처리 - 에러 수집, 분석, 재처리',
    schedule_interval='0 2 * * *',  # 매일 오전 2시
    catchup=False,
    tags=['dlq', 'error-handling', 'kafka'],
)


# ===== Task 1: 로그 파일 수집 =====
def collect_failed_logs(**context):
    """
    어제 날짜의 로그 파일 읽기
    """
    from airflow.models import Variable

    execution_date = context['execution_date']
    # 어제 날짜
    yesterday = (execution_date - timedelta(days=1)).strftime("%Y-%m-%d")

    log_dir = project_root / "logs" / "failed_messages"
    log_file = log_dir / f"{yesterday}.jsonl"

    print(f"로그 파일 경로: {log_file}")

    if not log_file.exists():
        print(f"로그 파일 없음: {log_file}")
        return []

    # JSONL 파일 읽기
    failed_messages = []
    with open(log_file, 'r', encoding='utf-8') as f:
        for line in f:
            if line.strip():
                try:
                    failed_messages.append(json.loads(line))
                except json.JSONDecodeError as e:
                    print(f"JSON 파싱 에러: {e}, line: {line[:100]}")
                    continue

    print(f"수집된 에러: {len(failed_messages)}개")

    return failed_messages


t1_collect = PythonOperator(
    task_id='collect_failed_logs',
    python_callable=collect_failed_logs,
    dag=dag,
)


# ===== Task 2: PostgreSQL에 저장 =====
def save_to_postgres(**context):
    """
    수집한 에러를 PostgreSQL에 저장
    """
    # XCom에서 데이터 가져오기
    ti = context['task_instance']
    failed_messages = ti.xcom_pull(task_ids='collect_failed_logs')

    if not failed_messages:
        print("저장할 데이터 없음")
        return 0

    # PostgreSQL 연결
    postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
    conn = postgres_hook.get_conn()
    cursor = conn.cursor()

    inserted = 0
    for record in failed_messages:
        try:
            cursor.execute("""
                INSERT INTO failed_messages (
                    message_id,
                    partition_key,
                    original_data,
                    error_type,
                    error_message,
                    error_traceback,
                    failed_at,
                    status
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s
                )
                ON CONFLICT DO NOTHING
            """, (
                str(record.get('index')),
                record.get('partition_key'),
                json.dumps(record.get('message')),
                record['error'].get('type'),
                record['error'].get('message'),
                record['error'].get('traceback'),
                record.get('timestamp'),
                'pending'
            ))
            inserted += 1

        except Exception as e:
            print(f"저장 실패: {e}")
            continue

    conn.commit()
    cursor.close()
    conn.close()

    print(f"✅ PostgreSQL 저장 완료: {inserted}개")
    return inserted


t2_save = PythonOperator(
    task_id='save_to_postgres',
    python_callable=save_to_postgres,
    dag=dag,
)


# ===== Task 3: 에러 통계 집계 =====
t3_stats = PostgresOperator(
    task_id='aggregate_statistics',
    postgres_conn_id='postgres_default',
    sql="""
        INSERT INTO error_statistics (date, error_type, count)
        SELECT
            DATE(failed_at) as date,
            error_type,
            COUNT(*) as count
        FROM failed_messages
        WHERE DATE(failed_at) = CURRENT_DATE - INTERVAL '1 day'
        GROUP BY DATE(failed_at), error_type
        ON CONFLICT (date, error_type)
        DO UPDATE SET
            count = EXCLUDED.count,
            updated_at = CURRENT_TIMESTAMP;
    """,
    dag=dag,
)


# ===== Task 4: 재처리 시도 =====
def retry_failed_messages(**context):
    """
    pending 상태 메시지 재처리
    """
    from kafka import KafkaProducer

    # PostgreSQL 연결
    postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
    conn = postgres_hook.get_conn()
    cursor = conn.cursor()

    # 재처리 대상 조회 (24시간 경과, 재시도 3회 미만)
    cursor.execute("""
        SELECT
            id,
            partition_key,
            original_data
        FROM failed_messages
        WHERE status = 'pending'
          AND failed_at < NOW() - INTERVAL '24 hours'
          AND retry_attempt < 3
        ORDER BY failed_at
        LIMIT 100
    """)

    messages_to_retry = cursor.fetchall()

    if not messages_to_retry:
        print("재처리 대상 없음")
        cursor.close()
        conn.close()
        return 0

    print(f"재처리 대상: {len(messages_to_retry)}개")

    # Kafka Producer 생성
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        key_serializer=lambda k: k.encode('utf-8') if k else None,
        acks='all',
        retries=3
    )

    success_count = 0
    for msg_id, partition_key, original_data in messages_to_retry:
        try:
            # JSON 문자열을 dict로 변환
            if isinstance(original_data, str):
                message_data = json.loads(original_data)
            else:
                message_data = original_data

            # Kafka 재전송
            future = producer.send(
                KAFKA_TOPIC,
                key=partition_key,
                value=message_data
            )
            future.get(timeout=10)

            # 성공: resolved 상태로
            cursor.execute("""
                UPDATE failed_messages
                SET status = 'resolved',
                    resolved_at = NOW(),
                    resolved_by = 'airflow',
                    updated_at = NOW()
                WHERE id = %s
            """, (msg_id,))

            success_count += 1
            print(f"재처리 성공: ID={msg_id}")

        except Exception as e:
            # 실패: retry_attempt 증가
            cursor.execute("""
                UPDATE failed_messages
                SET retry_attempt = retry_attempt + 1,
                    updated_at = NOW()
                WHERE id = %s
            """, (msg_id,))

            print(f"재처리 실패: ID={msg_id}, Error={e}")

    conn.commit()

    # 3번 재시도 실패한 메시지는 ignored로
    cursor.execute("""
        UPDATE failed_messages
        SET status = 'ignored',
            notes = '3회 재시도 실패',
            updated_at = NOW()
        WHERE status = 'pending'
          AND retry_attempt >= 3
    """)
    ignored_count = cursor.rowcount
    conn.commit()

    cursor.close()
    conn.close()
    producer.close()

    print(f"✅ 재처리 성공: {success_count}개")
    if ignored_count > 0:
        print(f"⚠️ 포기 처리: {ignored_count}개 (3회 실패)")

    return success_count


t4_retry = PythonOperator(
    task_id='retry_failed_messages',
    python_callable=retry_failed_messages,
    dag=dag,
)


# ===== Task 5: 일일 리포트 =====
def send_daily_report(**context):
    """
    일일 리포트 생성
    """
    # PostgreSQL 연결
    postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
    conn = postgres_hook.get_conn()
    cursor = conn.cursor()

    # 어제 통계
    execution_date = context['execution_date']
    yesterday = (execution_date - timedelta(days=1)).strftime("%Y-%m-%d")

    cursor.execute("""
        SELECT
            COUNT(*) as total,
            COUNT(*) FILTER (WHERE status = 'resolved') as resolved,
            COUNT(*) FILTER (WHERE status = 'pending') as pending,
            COUNT(*) FILTER (WHERE status = 'ignored') as ignored
        FROM failed_messages
        WHERE DATE(failed_at) = %s
    """, (yesterday,))

    result = cursor.fetchone()

    if result is None or result[0] == 0:
        print("에러 없음 - 리포트 생성 스킵")
        cursor.close()
        conn.close()
        return

    total, resolved, pending, ignored = result

    # 에러 타입별 통계
    cursor.execute("""
        SELECT error_type, COUNT(*)
        FROM failed_messages
        WHERE DATE(failed_at) = %s
        GROUP BY error_type
        ORDER BY COUNT(*) DESC
        LIMIT 5
    """, (yesterday,))

    error_breakdown = cursor.fetchall()

    # 리포트 작성
    report = f"""
📊 DLQ 일일 리포트 ({yesterday})

전체 실패: {total}개
✅ 해결: {resolved}개 ({resolved/total*100:.1f}%)
⏳ 대기: {pending}개
⏭️ 무시: {ignored}개

에러 타입 TOP 5:
"""

    for error_type, count in error_breakdown:
        report += f"  • {error_type}: {count}개\n"

    if total > 0 and (total - ignored) > 0:
        success_rate = (resolved / (total - ignored)) * 100
        report += f"\n재처리 성공률: {success_rate:.1f}%"

    # 콘솔 출력
    print(report)

    # TODO: Slack 전송 (선택사항)
    # SLACK_WEBHOOK = Variable.get('slack_webhook_url', default_var=None)
    # if SLACK_WEBHOOK:
    #     import requests
    #     requests.post(SLACK_WEBHOOK, json={'text': report})

    cursor.close()
    conn.close()

    return report


t5_report = PythonOperator(
    task_id='send_daily_report',
    python_callable=send_daily_report,
    dag=dag,
)


# ===== Task 의존성 =====
t1_collect >> t2_save >> t3_stats >> t4_retry >> t5_report
