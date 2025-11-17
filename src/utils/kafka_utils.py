from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import KafkaError, NoBrokersAvailable
import json
import math
from typing import List, Optional, Callable
from .logger import get_logger

logger = get_logger(__name__)


def safe_json_serializer(v):
    def convert_nan(obj):
        if isinstance(obj, float) and math.isnan(obj):
            return None
        return obj

    if isinstance(v, dict):
        v = {k: convert_nan(val) for k, val in v.items()}

    return json.dumps(v, allow_nan=False).encode('utf-8')


def create_kafka_producer(
    bootstrap_servers: List[str],
    value_serializer: Optional[Callable] = None,
    key_serializer: Optional[Callable] = None,
    **kwargs
) -> KafkaProducer:
    if value_serializer is None:
        value_serializer = safe_json_serializer

    if key_serializer is None:
        key_serializer = lambda k: k.encode('utf-8') if k else None

    try:
        producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=value_serializer,
            key_serializer=key_serializer,
            acks='all',
            retries=3,
            **kwargs
        )
        logger.info(f"Kafka Producer 연결 성공: {bootstrap_servers}")
        return producer

    except NoBrokersAvailable as e:
        logger.error(f"Kafka 브로커를 찾을 수 없습니다: {e}")
        raise
    except Exception as e:
        logger.error(f"Kafka Producer 생성 실패: {e}")
        raise


def create_kafka_consumer(
    topics: List[str],
    bootstrap_servers: List[str],
    group_id: str,
    value_deserializer: Optional[Callable] = None,
    auto_offset_reset: str = 'earliest',
    enable_auto_commit: bool = True,
    **kwargs
) -> KafkaConsumer:
    if value_deserializer is None:
        value_deserializer = lambda m: json.loads(m.decode('utf-8'))

    try:
        consumer = KafkaConsumer(
            *topics,
            bootstrap_servers=bootstrap_servers,
            group_id=group_id,
            value_deserializer=value_deserializer,
            auto_offset_reset=auto_offset_reset,
            enable_auto_commit=enable_auto_commit,
            **kwargs
        )
        logger.info(f"Kafka Consumer 연결 성공: {bootstrap_servers}")
        logger.info(f"구독 토픽: {topics}")
        logger.info(f"Consumer Group ID: {group_id}")
        return consumer

    except NoBrokersAvailable as e:
        logger.error(f"Kafka 브로커를 찾을 수 없습니다: {e}")
        raise
    except Exception as e:
        logger.error(f"Kafka Consumer 생성 실패: {e}")
        raise


def test_kafka_connection(bootstrap_servers: List[str]) -> bool:
    try:
        producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            request_timeout_ms=5000,
            api_version_auto_timeout_ms=5000
        )
        producer.close()
        logger.info("Kafka 연결 테스트 성공")
        return True
    except Exception as e:
        logger.error(f"Kafka 연결 테스트 실패: {e}")
        return False


def send_message(
    producer: KafkaProducer,
    topic: str,
    message: dict,
    key: Optional[str] = None,
    timeout: int = 10
) -> bool:
    try:
        future = producer.send(topic, value=message, key=key)
        future.get(timeout=timeout)
        logger.debug(f"메시지 전송 성공: topic={topic}")
        return True
    except KafkaError as e:
        logger.error(f"Kafka 메시지 전송 실패: {e}")
        return False
    except Exception as e:
        logger.error(f"예상치 못한 에러: {e}")
        return False
