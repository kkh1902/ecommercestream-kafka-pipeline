"""
Kafka Producer
CSV 파일을 읽어서 Kafka로 전송
"""

import pandas as pd
import time
import argparse
from tqdm import tqdm
import sys
import os

# 상위 디렉토리 경로 추가
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from config.settings import (
    KAFKA_BROKERS,
    KAFKA_TOPIC,
    CSV_FILE_PATH,
    PRODUCER_INTERVAL
)
from src.utils.logger import get_logger
from src.utils.kafka_utils import create_kafka_producer
from pathlib import Path

# 로그 디렉토리 생성
log_dir = Path("logs/producer")
log_dir.mkdir(parents=True, exist_ok=True)

# 로거 설정
logger = get_logger(__name__, log_file=str(log_dir / "producer.log"))




def load_csv(file_path, sample=0):
    """CSV 파일 읽기"""
    try:
        if sample > 0:
            df = pd.read_csv(file_path, nrows=sample)
            logger.info(f"데이터 로드 완료: {len(df)}개 (샘플 모드)")
        else:
            df = pd.read_csv(file_path)
            logger.info(f"데이터 로드 완료: {len(df)}개")
        return df
    except FileNotFoundError:
        logger.error(f"파일을 찾을 수 없습니다: {file_path}")
        raise
    except Exception as e:
        logger.error(f"CSV 로드 실패: {e}")
        raise


def send_messages(producer, df, topic, interval=4, batch_mode=False):
    """메시지 전송"""
    success_count = 0
    send_error = 0

    logger.info(f"\n토픽 '{topic}'로 {len(df)}개 메시지 전송 시작")
    if batch_mode:
        logger.info("배치 모드: 빠른 전송")
    else:
        logger.info(f"일반 모드: {interval}초 간격 전송\n")

    # 데이터 전송
    for idx, row in tqdm(df.iterrows(), total=len(df), desc="전송 중"):
        try:
            # 딕셔너리로 변환 후 NaN을 None으로 변환  파이썬에서 None이 Null임
            message = {}
            for key, value in row.to_dict().items():
                if pd.isna(value):
                    message[key] = None
                else:
                    message[key] = value

            # 파티션 키 생성 (visitorid 기준)
            partition_key = str(message.get('visitorid', idx))

            # Kafka 전송 (자동 3번 재시도)
            future = producer.send(topic, key=partition_key, value=message)
            future.get(timeout=10)

            success_count += 1

            # 일반 모드일 때만 대기
            if not batch_mode and interval > 0:
                time.sleep(interval)

        except Exception as e:
            # Kafka가 3번 재시도 후에도 실패한 경우
            send_error += 1

            # 에러 로그 기록
            logger.error(f"전송 실패 [{idx}]: {type(e).__name__}: {e}", exc_info=False)

            continue

    # 결과 출력
    logger.info("=" * 50)
    logger.info(f"전송 성공: {success_count}개")
    if send_error > 0:
        logger.warning(f"전송 실패: {send_error}개")
        logger.warning(f"에러 로그 위치: logs/producer/producer.log")
    logger.info(f"전체: {len(df)}개")
    logger.info("=" * 50)

    return success_count, send_error


def main():
    """메인 실행 함수"""
    parser = argparse.ArgumentParser(description='Kafka Producer')
    parser.add_argument('--file', type=str, default=CSV_FILE_PATH,
                        help=f'CSV 파일 경로 (기본값: {CSV_FILE_PATH})')
    parser.add_argument('--topic', type=str, default=KAFKA_TOPIC,
                        help=f'Kafka 토픽 (기본값: {KAFKA_TOPIC})')
    parser.add_argument('--sample', type=int, default=0,
                        help='샘플 개수 (0=전체)')
    parser.add_argument('--interval', type=int, default=PRODUCER_INTERVAL,
                        help=f'전송 간격(초) (기본값: {PRODUCER_INTERVAL})')
    parser.add_argument('--batch', action='store_true',
                        help='배치 모드 (지연 없이 빠른 전송)')

    args = parser.parse_args()

    logger.info("=" * 60)
    logger.info("Kafka Producer 시작")
    logger.info("=" * 60)

    try:
        # 1. Kafka Producer 생성 (utils 사용)
        producer = create_kafka_producer(KAFKA_BROKERS)

        # 2. CSV 파일 읽기
        df = load_csv(args.file, args.sample)

        # 3. 메시지 전송
        send_messages(
            producer=producer,
            df=df,
            topic=args.topic,
            interval=args.interval,
            batch_mode=args.batch
        )

        # 4. 종료 처리
        logger.info("Producer 종료 중...")
        producer.flush()
        producer.close()

        logger.info("전송 완료!")
        logger.info("=" * 60)

    except KeyboardInterrupt:
        logger.warning("사용자가 중단했습니다")
        producer.close()
    except Exception as e:
        logger.error(f"에러 발생: {e}", exc_info=True)
        raise


if __name__ == '__main__':
    main()
