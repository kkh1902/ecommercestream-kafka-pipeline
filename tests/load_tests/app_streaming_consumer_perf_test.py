#!/usr/bin/env python3

import subprocess
import time
import argparse
from datetime import datetime
from pathlib import Path
import sys

sys.path.append(str(Path(__file__).parent.parent.parent))

from src.utils.postgres_utils import get_postgres_connection
from config.settings import (
    POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DB,
    POSTGRES_USER, POSTGRES_PASSWORD
)


class StreamingConsumerPerfTest:
    # 초기화
    def __init__(self):
        self.test_dir = Path(__file__).parent
        self.project_dir = Path(__file__).parent.parent.parent
        self.process = None
        self.start_time = None
        self.initial_count = 0
        self.final_count = 0

    # DB 레코드 초기화 clickstreams_events에 몇개의 레코드가 있는지 확인
    def get_table_record_count(self, table_name='clickstream_events'):
        try:
            conn = get_postgres_connection(
                host=POSTGRES_HOST,
                port=POSTGRES_PORT,
                database=POSTGRES_DB,
                user=POSTGRES_USER,
                password=POSTGRES_PASSWORD
            )
            cursor = conn.cursor()
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            count = cursor.fetchone()[0]
            cursor.close()
            conn.close()
            return count
        except Exception as e:
            print(f"DB 조회 실패: {e}")
            return 0

    # 메인 테스트
    def run_streaming_consumer_test(self, timeout=180):
        # 설정 출력 
        print("\n" + "="*70)
        print("Spark Streaming Consumer 성능 테스트")
        print("="*70)
        print(f"설정:")
        print(f"   - 타임아웃: {timeout}초")
        print(f"   - 컨슈머: streaming_consumer.py")
        print(f"   - 대상 테이블: clickstream_events")
        print(f"   - 시작 시간: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("="*70)

        print("\nclickstream_events 초기 레코드 수 확인 중...")
        self.initial_count = self.get_table_record_count('clickstream_events')
        print(f"   - 초기 레코드: {self.initial_count:,}")

        # process실행
        cmd = [
            "python",
            str(self.project_dir / "src/consumer/streaming_consumer.py"),
        ]

        print(f"\n실행: {' '.join(cmd)}")
        print(f"Streaming Consumer를 {timeout}초 동안 실행합니다...\n")

        self.start_time = time.time()

        # 비동기식 (동시 실행) time out  완료 될때 까지 run() 으로는 무한 대기 에러
        try:
            self.process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                cwd=str(self.project_dir),
                bufsize=1
            )

            import select
            
            # 실시간 모니터링 루프
            # while 루프 (30초마다 진행 상황 출력)
            # timeout 체크
            # 프로세스 상태 체크
            # 30초마다 DB 레코드 확인
            while True:
                elapsed = time.time() - self.start_time

                if elapsed > timeout:
                    print(f"\n타임아웃 ({timeout}s) 도달. Streaming Consumer 중지 중...")
                    self.process.terminate()
                    try:
                        self.process.wait(timeout=30)
                    except subprocess.TimeoutExpired:
                        print("강제 종료 중...")
                        self.process.kill()
                        self.process.wait()
                    break

                if self.process.poll() is not None:
                    print(f"\n프로세스가 조기 종료됨 (종료 코드: {self.process.returncode})")
                    break

                if int(elapsed) % 30 == 0 and elapsed > 0:
                    current_count = self.get_table_record_count('clickstream_events')
                    processed = current_count - self.initial_count
                    rate = processed / elapsed if elapsed > 0 else 0
                    print(f"   [{int(elapsed):3d}s] 레코드: {processed:,} | 처리량: {rate:.0f} msg/sec")

                time.sleep(1)

            elapsed_time = time.time() - self.start_time

        except Exception as e:
            print(f"Streaming Consumer 실행 실패: {e}")
            return None

        print("\nclickstream_events 최종 레코드 수 확인 중...")
        self.final_count = self.get_table_record_count('clickstream_events')
        print(f"   - 최종 레코드: {self.final_count:,}")

        # 성능 지표 계산
        processed_count = self.final_count - self.initial_count
        throughput = processed_count / elapsed_time if elapsed_time > 0 else 0

        metrics = {
            "initial_count": self.initial_count,
            "final_count": self.final_count,
            "processed_count": processed_count,
            "throughput": throughput,
            "elapsed_time": elapsed_time
        }

        print(f"\n총 실행 시간: {elapsed_time:.2f}초")
        print(f"처리된 레코드: {processed_count:,}")
        print(f"처리량: {throughput:.0f} msg/sec")

        if processed_count > 0:
            print(f"데이터 크기: {processed_count * 1024 / (1024*1024):.2f} MB")
            print(f"데이터 속도: {processed_count * 1024 / (1024*1024) / elapsed_time:.2f} MB/sec")

        return {
            "test": "streaming_consumer_app",
            "timeout": timeout,
            "status": "성공" if processed_count > 0 else "실패",
            "timestamp": datetime.now().isoformat(),
            "metrics": metrics
        }

    # 결과 요약출력
    def print_summary(self, result):
        if result is None:
            print("\n테스트 실패!")
            return

        print("\n" + "="*70)
        print("Spark Streaming Consumer 성능 요약")
        print("="*70)
        print(f"\n테스트: {result['test']}")
        print(f"   - 타임아웃: {result['timeout']}s")
        print(f"   - 상태: {result['status']}")

        metrics = result['metrics']
        print(f"\n지표:")
        print(f"   - 초기 DB 레코드: {metrics['initial_count']:,}")
        print(f"   - 최종 DB 레코드: {metrics['final_count']:,}")
        print(f"   - 처리된 레코드: {metrics['processed_count']:,}")
        print(f"   - 처리량: {metrics['throughput']:.0f} msg/sec")
        print(f"   - 실행 시간: {metrics['elapsed_time']:.2f}s")

        if metrics['processed_count'] > 0:
            print(f"\n데이터 통계:")
            print(f"   - 데이터 크기: {metrics['processed_count'] * 1024 / (1024*1024):.2f} MB")
            print(f"   - 데이터 속도: {metrics['processed_count'] * 1024 / (1024*1024) / metrics['elapsed_time']:.2f} MB/sec")

        print(f"\n일반 Consumer와 비교:")
        print(f"   - 일반 Consumer: 1,578 msg/sec")
        print(f"   - Streaming Consumer: {metrics['throughput']:.0f} msg/sec")
        if metrics['throughput'] > 0:
            ratio = metrics['throughput'] / 1578
            print(f"   - 성능 비율: {ratio:.1f}x")

        print("\n" + "="*70)

        return result


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--timeout", type=int, default=180)

    args = parser.parse_args()

    tester = StreamingConsumerPerfTest()

    result = tester.run_streaming_consumer_test(timeout=args.timeout)
    tester.print_summary(result)


if __name__ == "__main__":
    main()
