"""
Kafka Consumer for PostgreSQL
Kafka에서 메시지 받아서 PostgreSQL에 저장
"""

import sys
import os

# 상위 디렉토리 경로 추가 (src/consumer/ -> comerce-kafka/)
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from pathlib import Path
from datetime import datetime
import json
from config.settings import (
    KAFKA_BROKERS,
    KAFKA_TOPIC,
    CONSUMER_GROUP_ID,
    POSTGRES_HOST,
    POSTGRES_PORT,
    POSTGRES_DB,
    POSTGRES_USER,
    POSTGRES_PASSWORD
)
from src.utils.logger import get_logger
from src.utils.kafka_utils import create_kafka_consumer, create_kafka_producer
from src.utils.postgres_utils import get_postgres_connection, batch_insert, create_table_from_schema

# 로그 디렉토리 생성
log_dir = Path("logs/consumer")
log_dir.mkdir(parents=True, exist_ok=True)

# 로거 설정
logger = get_logger(__name__, log_file=str(log_dir / "consumer_postgres.log"))




def prepare_batch_data(batch_data):
    """배치 데이터를 튜플로 변환"""
    return [
        (
            data['timestamp'],  # Unix timestamp (밀리초) 그대로 저장
            data['visitorid'],
            data['event'],
            data['itemid'],
            data.get('transactionid')  # NaN일 수 있음
        )
        for data in batch_data
    ]


def send_error_to_kafka(error_msg, producer):
    """에러 정보를 Kafka error_data topic으로 전송"""
    try:
        producer.send('error_data', error_msg)
        logger.info("에러 데이터가 Kafka로 전송됨")
    except Exception as e:
        logger.error(f"에러 데이터 전송 실패: {e}")


def consume_messages(consumer, conn, producer):
    """메시지 수신 및 저장"""
    message_count = 0
    batch_data = []
    batch_size = 100  # 1000 → 100으로 변경 (테스트용)

    logger.info("메시지 대기 중...")
    logger.info("종료하려면 Ctrl+C 누르세요")

    try:
        for message in consumer:
            # Producer에서 이미 NaN → None 처리됨
            msg = message.value

            # 배치에 추가
            batch_data.append(msg)
            message_count += 1

            # 배치 크기 도달 시 저장
            if len(batch_data) >= batch_size:
                tuple_data = prepare_batch_data(batch_data)
                try:
                    inserted = batch_insert(
                        conn,
                        'raw_clickstream_events',
                        ['timestamp', 'visitorid', 'event', 'itemid', 'transactionid'],
                        tuple_data
                    )
                    logger.info(f"DB 저장: {message_count}개 (배치: {inserted}개)")
                except Exception as e:
                    # DB 저장 실패 시 error_data topic으로 전송
                    logger.error(f"배치 저장 실패: {e}", exc_info=False)
                    error_msg = {
                        "timestamp": datetime.now().isoformat(),
                        "error_type": type(e).__name__,
                        "message": str(e),
                        "module": "consumer_postgres",
                        "batch_count": len(batch_data),
                        "status": "pending_retry"
                    }
                    send_error_to_kafka(error_msg, producer)
                finally:
                    batch_data = []

    except KeyboardInterrupt:
        logger.warning("사용자가 중단했습니다")
    except Exception as e:
        logger.error(f"메시지 수신 에러: {e}", exc_info=True)
        # 치명적 에러를 error_data topic으로 전송
        error_msg = {
            "timestamp": datetime.now().isoformat(),
            "error_type": type(e).__name__,
            "message": str(e),
            "module": "consumer_postgres",
            "status": "critical"
        }
        send_error_to_kafka(error_msg, producer)
        raise
    finally:
        # 남은 데이터 저장
        if batch_data:
            tuple_data = prepare_batch_data(batch_data)
            try:
                inserted = batch_insert(
                    conn,
                    'raw_clickstream_events',
                    ['timestamp', 'visitorid', 'event', 'itemid', 'transactionid'],
                    tuple_data
                )
                logger.info(f"남은 데이터 저장: {inserted}개")
            except Exception as e:
                logger.error(f"남은 데이터 저장 실패: {e}", exc_info=False)
                error_msg = {
                    "timestamp": datetime.now().isoformat(),
                    "error_type": type(e).__name__,
                    "message": str(e),
                    "module": "consumer_postgres",
                    "batch_count": len(batch_data),
                    "status": "pending_retry"
                }
                send_error_to_kafka(error_msg, producer)

        logger.info(f"총 받은 메시지: {message_count}개")


def main():
    logger.info("=" * 60)
    logger.info("Kafka Consumer (PostgreSQL) 시작")
    logger.info("=" * 60)

    conn = None
    consumer = None
    producer = None

    try:
        # PostgreSQL 연결
        conn = get_postgres_connection(
            host=POSTGRES_HOST,
            port=POSTGRES_PORT,
            database=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD
        )

        # 데이터베이스 초기화 (선택 사항 - docker-compose에서 자동 실행)
        # schema_path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 'sql', 'schema.sql')
        # create_table_from_schema(conn, schema_path)

        # Consumer 생성 (utils 사용)
        consumer = create_kafka_consumer(
            topics=[KAFKA_TOPIC],
            bootstrap_servers=KAFKA_BROKERS,
            group_id=f"{CONSUMER_GROUP_ID}_postgres"
        )

        # 에러 처리용 Producer 생성
        producer = create_kafka_producer(
            bootstrap_servers=KAFKA_BROKERS
        )
        logger.info("✅ 에러 처리용 Producer 생성됨")

        # 메시지 수신
        consume_messages(consumer, conn, producer)

    except KeyboardInterrupt:
        logger.warning("사용자가 중단했습니다")
    except Exception as e:
        logger.error(f"에러 발생: {e}", exc_info=True)
        raise
    finally:
        # 종료 처리
        if consumer:
            consumer.close()
            logger.info("Consumer 종료")
        if producer:
            producer.close()
            logger.info("Producer 종료")
        if conn:
            conn.close()
            logger.info("PostgreSQL 연결 종료")

        logger.info("=" * 60)


if __name__ == '__main__':
    main()
