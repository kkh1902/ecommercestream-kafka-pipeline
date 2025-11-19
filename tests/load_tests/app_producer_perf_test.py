#!/usr/bin/env python3
"""
Real Producer Application Performance Test

우리가 만든 producer.py의 실제 성능을 측정합니다.
"""

import subprocess
import time
import argparse
from datetime import datetime
from pathlib import Path
import sys
import os

# 프로젝트 경로 추가
sys.path.append(str(Path(__file__).parent.parent.parent))

from config.settings import CSV_FILE_PATH


class ProducerPerfTest:
    """Producer Application Performance Test"""

    def __init__(self):
        self.test_dir = Path(__file__).parent
        self.project_dir = Path(__file__).parent.parent.parent

    def run_producer_test(self, csv_file, sample=10000, batch_mode=True):
        """
        Producer 성능 테스트 실행

        Args:
            csv_file: CSV 파일 경로
            sample: 테스트할 샘플 크기 (0=전체)
            batch_mode: 배치 모드 여부
        """
        print("\n" + "="*70)
        print("🚀 Real Producer Application Performance Test")
        print("="*70)
        print(f"📊 Configuration:")
        print(f"   - CSV File: {csv_file}")
        print(f"   - Sample Size: {sample} records" if sample > 0 else "   - Sample Size: ALL")
        print(f"   - Batch Mode: {batch_mode}")
        print(f"   - Start Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("="*70)

        # Producer 실행 명령어
        cmd = [
            "python",
            str(self.project_dir / "src/producer/producer.py"),
            "--file", str(csv_file),
            "--batch",  # 배치 모드 활성화
        ]

        if sample > 0:
            cmd.extend(["--sample", str(sample)])

        print(f"\n📌 Running: {' '.join(cmd)}\n")

        start_time = time.time()
        result = subprocess.run(cmd, capture_output=True, text=True, cwd=str(self.project_dir))
        elapsed_time = time.time() - start_time

        # 출력 표시
        print(result.stdout)
        if result.stderr:
            print("[STDERR]", result.stderr)

        # 성능 지표 계산
        metrics = self._parse_producer_output(result.stdout, sample, elapsed_time)

        print(f"\n⏱️  Total Execution Time: {elapsed_time:.2f} seconds")
        print(f"📈 Throughput: {metrics['throughput']:.0f} msg/sec")
        print(f"💾 Data Size: {metrics['data_size']:.2f} MB")

        return {
            "test": "producer_app",
            "csv_file": csv_file,
            "sample_size": sample,
            "batch_mode": batch_mode,
            "status": "SUCCESS" if result.returncode == 0 else "FAILED",
            "timestamp": datetime.now().isoformat(),
            "execution_time": elapsed_time,
            "metrics": metrics
        }

    def _parse_producer_output(self, output, sample, elapsed_time):
        """Producer 출력에서 성능 지표 추출"""
        import re

        # 로그에서 성공한 메시지 수 찾기
        match = re.search(r"전송 성공: (\d+)개", output)
        if match:
            success_count = int(match.group(1))
        else:
            success_count = sample if sample > 0 else 0

        # 처리량 계산 (msg/sec)
        throughput = success_count / elapsed_time if elapsed_time > 0 else 0

        # 데이터 크기 추정 (1KB per message)
        data_size = (success_count * 1024) / (1024 * 1024)  # MB

        return {
            "success_count": success_count,
            "throughput": throughput,
            "data_size": data_size,
            "elapsed_time": elapsed_time
        }

    def print_summary(self, result):
        """테스트 결과 요약"""
        print("\n" + "="*70)
        print("📈 Producer Application Performance Summary")
        print("="*70)
        print(f"\n✅ Test: {result['test']}")
        print(f"   - CSV File: {result['csv_file']}")
        print(f"   - Sample Size: {result['sample_size']}")
        print(f"   - Status: {result['status']}")
        print(f"   - Execution Time: {result['execution_time']:.2f}s")

        metrics = result['metrics']
        print(f"\n📊 Metrics:")
        print(f"   - Messages Sent: {metrics['success_count']:,}")
        print(f"   - Throughput: {metrics['throughput']:.0f} msg/sec")
        print(f"   - Data Size: {metrics['data_size']:.2f} MB")
        print(f"   - Data Rate: {metrics['data_size']/result['execution_time']:.2f} MB/sec")

        print("\n" + "="*70)

        return result


def main():
    parser = argparse.ArgumentParser(
        description="Real Producer Application Performance Test",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python app_producer_perf_test.py                    # Default 10k records
  python app_producer_perf_test.py --sample 50000     # 50k records
  python app_producer_perf_test.py --csv data/raw/events.csv --sample 100000
        """
    )

    parser.add_argument("--csv", type=str, default=CSV_FILE_PATH,
                        help=f"CSV file path (default: {CSV_FILE_PATH})")
    parser.add_argument("--sample", type=int, default=10000,
                        help="Sample size in records (default: 10000, 0 for all)")

    args = parser.parse_args()

    tester = ProducerPerfTest()

    # Producer 테스트 실행
    result = tester.run_producer_test(
        csv_file=args.csv,
        sample=args.sample,
        batch_mode=True
    )

    # 결과 요약 출력
    tester.print_summary(result)


if __name__ == "__main__":
    main()
