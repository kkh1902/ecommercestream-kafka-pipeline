#!/usr/bin/env python3
"""
Integrated Application Performance Test

Producer + Consumer 전체 파이프라인의 성능을 측정합니다.
"""

import subprocess
import time
import argparse
from datetime import datetime
from pathlib import Path
import sys
import threading

# 프로젝트 경로 추가
sys.path.append(str(Path(__file__).parent.parent.parent))

from config.settings import CSV_FILE_PATH
from src.utils.postgres_utils import get_postgres_connection
from config.settings import (
    POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DB,
    POSTGRES_USER, POSTGRES_PASSWORD
)


class IntegratedPerfTest:
    """Integrated Application Performance Test"""

    def __init__(self):
        self.test_dir = Path(__file__).parent
        self.project_dir = Path(__file__).parent.parent.parent
        self.results = []

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
            print(f"❌ Database query failed: {e}")
            return 0

    def run_producer(self, csv_file, sample=10000):
        """Producer 실행"""
        print("\n" + "="*70)
        print("1️⃣ PHASE 1: Running Producer")
        print("="*70)

        cmd = [
            "python",
            str(self.project_dir / "src/producer/producer.py"),
            "--file", str(csv_file),
            "--batch",
        ]

        if sample > 0:
            cmd.extend(["--sample", str(sample)])

        start_time = time.time()
        result = subprocess.run(cmd, capture_output=True, text=True, cwd=str(self.project_dir))
        elapsed_time = time.time() - start_time

        print(result.stdout)

        return {
            "phase": "producer",
            "success": result.returncode == 0,
            "elapsed_time": elapsed_time,
            "sample_size": sample
        }

    def run_consumer(self, timeout=120):
        """Consumer 실행"""
        print("\n" + "="*70)
        print("2️⃣ PHASE 2: Running Consumer")
        print("="*70)

        initial_count = self.get_table_record_count()
        print(f"📌 Initial DB records: {initial_count:,}")

        cmd = [
            "python",
            str(self.project_dir / "src/consumer/consumer_postgres.py"),
        ]

        start_time = time.time()

        try:
            process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                cwd=str(self.project_dir)
            )

            try:
                process.wait(timeout=timeout)
            except subprocess.TimeoutExpired:
                print(f"\n⏱️ Timeout ({timeout}s) reached. Stopping consumer...")
                process.terminate()
                try:
                    process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()

            elapsed_time = time.time() - start_time

        except Exception as e:
            print(f"❌ Consumer failed: {e}")
            return None

        final_count = self.get_table_record_count()
        print(f"📌 Final DB records: {final_count:,}")

        processed_count = final_count - initial_count

        return {
            "phase": "consumer",
            "success": processed_count > 0,
            "elapsed_time": elapsed_time,
            "initial_count": initial_count,
            "final_count": final_count,
            "processed_count": processed_count,
            "throughput": processed_count / elapsed_time if elapsed_time > 0 else 0
        }

    def run_integrated_test(self, csv_file, sample=10000, consumer_timeout=180):
        """전체 통합 테스트 실행"""
        print("\n" + "="*70)
        print("🔄 Integrated Application Performance Test")
        print("="*70)
        print(f"📊 Configuration:")
        print(f"   - CSV File: {csv_file}")
        print(f"   - Sample Size: {sample}" if sample > 0 else "   - Sample Size: ALL")
        print(f"   - Consumer Timeout: {consumer_timeout}s")
        print(f"   - Start Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("="*70)

        total_start = time.time()

        # Phase 1: Producer
        producer_result = self.run_producer(csv_file, sample)
        self.results.append(producer_result)

        if not producer_result["success"]:
            print("\n❌ Producer failed! Aborting test.")
            return None

        # 잠시 대기 (Kafka에 데이터가 안정화되도록)
        print("\n⏳ Waiting 5 seconds for data stabilization...")
        time.sleep(5)

        # Phase 2: Consumer
        consumer_result = self.run_consumer(consumer_timeout)
        self.results.append(consumer_result)

        if consumer_result is None or not consumer_result["success"]:
            print("\n❌ Consumer failed!")
            return None

        total_elapsed = time.time() - total_start

        return {
            "test": "integrated",
            "status": "SUCCESS",
            "timestamp": datetime.now().isoformat(),
            "producer": producer_result,
            "consumer": consumer_result,
            "total_time": total_elapsed
        }

    def print_summary(self, result):
        """테스트 결과 요약"""
        if result is None:
            print("\n" + "="*70)
            print("❌ INTEGRATED TEST FAILED")
            print("="*70)
            return

        print("\n" + "="*70)
        print("📊 INTEGRATED TEST SUMMARY")
        print("="*70)

        producer = result["producer"]
        consumer = result["consumer"]

        print(f"\n✅ Phase 1: Producer")
        print(f"   - Status: {'SUCCESS' if producer['success'] else 'FAILED'}")
        print(f"   - Sample Size: {producer['sample_size']:,}")
        print(f"   - Execution Time: {producer['elapsed_time']:.2f}s")

        if producer['sample_size'] > 0:
            producer_throughput = producer['sample_size'] / producer['elapsed_time']
            print(f"   - Throughput: {producer_throughput:.0f} msg/sec")

        print(f"\n✅ Phase 2: Consumer")
        print(f"   - Status: {'SUCCESS' if consumer['success'] else 'FAILED'}")
        print(f"   - Initial DB Records: {consumer['initial_count']:,}")
        print(f"   - Final DB Records: {consumer['final_count']:,}")
        print(f"   - Processed Records: {consumer['processed_count']:,}")
        print(f"   - Execution Time: {consumer['elapsed_time']:.2f}s")
        print(f"   - Consumer Throughput: {consumer['throughput']:.0f} msg/sec")

        print(f"\n📈 Overall Statistics:")
        print(f"   - Total Time: {result['total_time']:.2f}s")
        print(f"   - Total Records: {consumer['processed_count']:,}")
        print(f"   - Average Time per Record: {result['total_time']/consumer['processed_count']*1000:.2f}ms")

        # 병목 분석
        print(f"\n🔍 Bottleneck Analysis:")
        producer_rate = producer['sample_size'] / producer['elapsed_time']
        consumer_rate = consumer['throughput']
        print(f"   - Producer Rate: {producer_rate:.0f} msg/sec")
        print(f"   - Consumer Rate: {consumer_rate:.0f} msg/sec")

        if consumer_rate < producer_rate:
            ratio = producer_rate / consumer_rate
            print(f"   ⚠️ Consumer is {ratio:.1f}x slower than Producer!")
            print(f"   → Bottleneck: Database insertion")
        else:
            print(f"   ✅ Consumer keeping up with Producer")

        print("\n" + "="*70)


def main():
    parser = argparse.ArgumentParser(
        description="Integrated Application Performance Test",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python run_app_perf_tests.py                    # Default 10k records, 180s consumer
  python run_app_perf_tests.py --sample 50000     # 50k records
  python run_app_perf_tests.py --consumer-timeout 300  # 5 minutes
        """
    )

    parser.add_argument("--csv", type=str, default=CSV_FILE_PATH,
                        help=f"CSV file path (default: {CSV_FILE_PATH})")
    parser.add_argument("--sample", type=int, default=10000,
                        help="Sample size in records (default: 10000)")
    parser.add_argument("--consumer-timeout", type=int, default=180,
                        help="Consumer timeout in seconds (default: 180)")

    args = parser.parse_args()

    tester = IntegratedPerfTest()

    # 통합 테스트 실행
    result = tester.run_integrated_test(
        csv_file=args.csv,
        sample=args.sample,
        consumer_timeout=args.consumer_timeout
    )

    # 결과 요약 출력
    tester.print_summary(result)


if __name__ == "__main__":
    main()
