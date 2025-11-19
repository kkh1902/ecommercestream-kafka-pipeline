#!/usr/bin/env python3
"""
Real Consumer Application Performance Test

우리가 만든 consumer_postgres.py의 실제 성능을 측정합니다.
"""

import subprocess
import time
import argparse
import signal
from datetime import datetime
from pathlib import Path
import sys
import threading

# 프로젝트 경로 추가
sys.path.append(str(Path(__file__).parent.parent.parent))

from src.utils.postgres_utils import get_postgres_connection
from config.settings import (
    POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DB,
    POSTGRES_USER, POSTGRES_PASSWORD
)


class ConsumerPerfTest:
    """Consumer Application Performance Test"""

    def __init__(self):
        self.test_dir = Path(__file__).parent
        self.project_dir = Path(__file__).parent.parent.parent
        self.process = None
        self.start_time = None
        self.initial_count = 0
        self.final_count = 0

    def get_table_record_count(self, table_name='raw_clickstream_events'):
        """PostgreSQL 테이블의 레코드 수 조회"""
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
            print(f"❌ 데이터베이스 조회 실패: {e}")
            return 0

    def run_consumer_test(self, timeout=180):
        """
        Consumer 성능 테스트 실행

        Args:
            timeout: 테스트 실행 시간 (초)
        """
        print("\n" + "="*70)
        print("📥 Real Consumer Application Performance Test")
        print("="*70)
        print(f"📊 Configuration:")
        print(f"   - Timeout: {timeout} seconds")
        print(f"   - Start Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("="*70)

        # 초기 테이블 레코드 수 확인
        print("\n📌 Checking initial record count...")
        self.initial_count = self.get_table_record_count()
        print(f"   - Initial records in DB: {self.initial_count:,}")

        # Consumer 실행 명령어
        cmd = [
            "python",
            str(self.project_dir / "src/consumer/consumer_postgres.py"),
        ]

        print(f"\n📌 Running: {' '.join(cmd)}")
        print(f"⏳ Consumer will run for {timeout} seconds...\n")

        self.start_time = time.time()

        try:
            # Consumer 프로세스 시작
            self.process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                cwd=str(self.project_dir)
            )

            # Timeout까지 대기
            try:
                self.process.wait(timeout=timeout)
            except subprocess.TimeoutExpired:
                print(f"\n⏱️ Timeout ({timeout}s) reached. Stopping consumer...")
                self.process.terminate()
                try:
                    self.process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    self.process.kill()
                    self.process.wait()

            elapsed_time = time.time() - self.start_time

        except Exception as e:
            print(f"❌ Consumer 실행 실패: {e}")
            return None

        # 최종 테이블 레코드 수 확인
        print("\n📌 Checking final record count...")
        self.final_count = self.get_table_record_count()
        print(f"   - Final records in DB: {self.final_count:,}")

        # 처리된 레코드 수
        processed_count = self.final_count - self.initial_count
        throughput = processed_count / elapsed_time if elapsed_time > 0 else 0

        # 성능 지표 계산
        metrics = {
            "initial_count": self.initial_count,
            "final_count": self.final_count,
            "processed_count": processed_count,
            "throughput": throughput,  # msg/sec
            "elapsed_time": elapsed_time
        }

        print(f"\n⏱️  Total Execution Time: {elapsed_time:.2f} seconds")
        print(f"📊 Records Processed: {processed_count:,}")
        print(f"📈 Throughput: {throughput:.0f} msg/sec")

        # Consumer 출력 표시
        if self.process.stdout:
            print(f"\n{'='*70}")
            print("Consumer Output (최근 20줄):")
            print(f"{'='*70}")
            lines = self.process.stdout.readlines()
            for line in lines[-20:]:
                print(line, end='')

        return {
            "test": "consumer_app",
            "timeout": timeout,
            "status": "SUCCESS" if processed_count > 0 else "FAILED",
            "timestamp": datetime.now().isoformat(),
            "metrics": metrics
        }

    def print_summary(self, result):
        """테스트 결과 요약"""
        if result is None:
            print("\n❌ 테스트 실패!")
            return

        print("\n" + "="*70)
        print("📈 Consumer Application Performance Summary")
        print("="*70)
        print(f"\n✅ Test: {result['test']}")
        print(f"   - Timeout: {result['timeout']}s")
        print(f"   - Status: {result['status']}")

        metrics = result['metrics']
        print(f"\n📊 Metrics:")
        print(f"   - Initial DB Records: {metrics['initial_count']:,}")
        print(f"   - Final DB Records: {metrics['final_count']:,}")
        print(f"   - Records Processed: {metrics['processed_count']:,}")
        print(f"   - Throughput: {metrics['throughput']:.0f} msg/sec")
        print(f"   - Execution Time: {metrics['elapsed_time']:.2f}s")

        if metrics['processed_count'] > 0:
            print(f"\n💾 Data Statistics:")
            print(f"   - Data Size: {metrics['processed_count'] * 1024 / (1024*1024):.2f} MB")
            print(f"   - Data Rate: {metrics['processed_count'] * 1024 / (1024*1024) / metrics['elapsed_time']:.2f} MB/sec")

        print("\n" + "="*70)

        return result


def main():
    parser = argparse.ArgumentParser(
        description="Real Consumer Application Performance Test",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python app_consumer_perf_test.py                    # Default 180s timeout
  python app_consumer_perf_test.py --timeout 300      # 5 minutes
  python app_consumer_perf_test.py --timeout 60       # 1 minute
        """
    )

    parser.add_argument("--timeout", type=int, default=180,
                        help="Test timeout in seconds (default: 180)")

    args = parser.parse_args()

    tester = ConsumerPerfTest()

    # Consumer 테스트 실행
    result = tester.run_consumer_test(timeout=args.timeout)

    # 결과 요약 출력
    tester.print_summary(result)


if __name__ == "__main__":
    main()
