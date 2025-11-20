#!/usr/bin/env python3

import subprocess
import time
import argparse
import sys
from datetime import datetime
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent.parent))
from config.settings import CSV_FILE_PATH


class ProducerPerfTest:
    def __init__(self):
        self.project_dir = Path(__file__).parent.parent.parent

    def run_test(self, csv_file, sample=5000):
        # 1. 설정 출력
        print("\n" + "="*60)
        print("Producer 성능 테스트")
        print("="*60)
        print(f"설정 :")
        print(f"   - CSV 파일: {csv_file}")
        print(f"   - 샘플 크기: {sample:,}개" if sample > 0 else "   - 샘플 크기: 전체")
        print(f"   - 시작 시간: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("="*60 + "\n")

        # 2. Producer 명령어 구성
        cmd = [
            "python",
            str(self.project_dir / "src/producer/producer.py"), # producer 실행
            "--file", str(csv_file),
            "--batch",
        ]

        if sample > 0:
            cmd.extend(["--sample", str(sample)])

        # 3. 시간 측정하며 실행     
        start_time = time.time()
        # 코드 안에서 터미널 명령 실행 모듈 subprocess
        result = subprocess.run(cmd, capture_output=True, text=True, cwd=str(self.project_dir))
        elapsed_time = time.time() - start_time

        print(result.stdout)
        if result.stderr:
            print("[에러]", result.stderr)

         # 5. 성능 지표 계산
        success_count = sample if sample > 0 else 0  # 5,000개
        throughput = success_count / elapsed_time if elapsed_time > 0 else 0  # msg/sec
        data_size = (success_count * 1024) / (1024 * 1024)  # MB data 크기 
        data_rate = data_size / elapsed_time if elapsed_time > 0 else 0 # MB/sec

        print(f"\n실행 시간: {elapsed_time:.2f}초")
        print(f"처리량: {throughput:.0f} msg/sec ({data_rate:.2f} MB/sec)")

        return {
            "success_count": success_count,
            "throughput": throughput,
            "data_size": data_size,
            "elapsed_time": elapsed_time,
            "status": "성공" if result.returncode == 0 else "실패"
        }

    def print_summary(self, metrics):
        print("\n" + "="*60)
        print("테스트 결과 요약")
        print("="*60)
        print(f"상태: {metrics['status']}")
        print(f"전송 메시지: {metrics['success_count']:,}개")
        print(f"처리량: {metrics['throughput']:.0f} msg/sec")
        print(f"데이터 크기: {metrics['data_size']:.2f} MB")
        print(f"전송 속도: {metrics['data_size']/metrics['elapsed_time']:.2f} MB/sec")
        print(f"실행 시간: {metrics['elapsed_time']:.2f}초")
        print("="*60 + "\n")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--csv", type=str, default=CSV_FILE_PATH)
    parser.add_argument("--sample", type=int, default=5000)

    args = parser.parse_args()
    tester = ProducerPerfTest()

    metrics = tester.run_test(csv_file=args.csv, sample=args.sample)
    tester.print_summary(metrics)


if __name__ == "__main__":
    main()
