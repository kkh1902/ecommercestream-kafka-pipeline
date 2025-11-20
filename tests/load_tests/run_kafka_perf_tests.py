"""
Kafka 성능 테스트

목적: Kafka Producer와 Consumer의 성능을 측정합니다.

사용 예시:
    python run_kafka_perf_tests.py --producer
    python run_kafka_perf_tests.py --producer --num-records 100000 --record-size 1024 --acks all
    python run_kafka_perf_tests.py --consumer
    python run_kafka_perf_tests.py --consumer --num-messages 100000 --timeout 120000
"""

import subprocess
import argparse
import re
import time
from datetime import datetime


class KafkaPerfTest:
    """Kafka 성능 테스트"""

    def producer_test(self, num_records=100000, record_size=1024, acks="all"):
        """Kafka Producer 성능 테스트 실행"""
        print("\n" + "="*60)
        print("Kafka Producer 성능 테스트")
        print("="*60)
        print(f"설정:")
        print(f"   - 메시지 개수: {num_records:,}개")
        print(f"   - 메시지 크기: {record_size} bytes")
        print(f"   - Acks 설정: {acks}")
        print(f"   - 시작 시간: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("="*60)

        cmd = [
            "docker", "exec", "kafka-broker-1",
            "kafka-producer-perf-test",
            "--topic", "perf-test",
            "--num-records", str(num_records),
            "--record-size", str(record_size),
            "--throughput", "-1",
            "--producer-props",
            f"bootstrap.servers=kafka-broker-1:19092,kafka-broker-2:19093,kafka-broker-3:19094",
            f"acks={acks}"
        ]

        start_time = time.time()
        result = subprocess.run(cmd, capture_output=True, text=True)
        elapsed_time = time.time() - start_time

        print(result.stdout)
        if result.stderr:
            print("에러", result.stderr)

        # 결과를 표 형식으로 파싱해서 출력
        self._parse_and_print_producer_result(result.stdout, elapsed_time)

    def consumer_test(self, num_messages=100000, timeout_ms=120000):
        """Kafka Consumer 성능 테스트 실행"""
        print("\n" + "="*60)
        print("Kafka Consumer 성능 테스트")
        print("="*60)
        print(f"설정:")
        print(f"   - 메시지 개수: {num_messages:,}개")
        print(f"   - 타임아웃: {timeout_ms} ms")
        print(f"   - 시작 시간: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("="*60)
        print("\nProducer 테스트가 완료되었는지 확인하세요!")
        print("Consumer가 Topic에 있는 데이터를 읽어야 합니다.\n")

        cmd = [
            "docker", "exec", "kafka-broker-1",
            "kafka-consumer-perf-test",
            "--broker-list", "kafka-broker-1:19092,kafka-broker-2:19093,kafka-broker-3:19094",
            "--topic", "perf-test",
            "--messages", str(num_messages),
            "--timeout", str(timeout_ms)
        ]

        start_time = time.time()
        result = subprocess.run(cmd, capture_output=True, text=True)
        elapsed_time = time.time() - start_time

        print(result.stdout)
        if result.stderr:
            print("[에러]", result.stderr)

        # 결과를 표 형식으로 파싱해서 출력
        self._parse_and_print_consumer_result(result.stdout, elapsed_time)

    def _parse_and_print_producer_result(self, output, elapsed_time):
        """Producer 결과를 표 형식으로 파싱 및 출력"""
        # 결과 줄 찾기
        result_line = None
        for line in output.strip().split('\n'):
            if 'records sent' in line:
                result_line = line
                break

        if not result_line:
            return

        print("\n" + "="*55)
        print("Kafka Producer 성능 분석")
        print("="*55)
        print("항목                       값")
        print("─"*55)

        try:
            # 패턴 매칭으로 각 값 추출
            records_match = re.search(r'(\d+) records sent', result_line)
            if records_match:
                print(f"{'전송된 메시지':<27} {int(records_match.group(1)):,}")

            records_sec_match = re.search(r'([\d.]+) records/sec', result_line)
            if records_sec_match:
                print(f"{'처리 속도(msg/sec)':<27} {float(records_sec_match.group(1)):,.2f}")

            mb_sec_match = re.search(r'\(([\d.]+) MB/sec\)', result_line)
            if mb_sec_match:
                print(f"{'처리량(MB/sec)':<27} {float(mb_sec_match.group(1)):,.2f}")

            avg_latency_match = re.search(r'([\d.]+) ms avg latency', result_line)
            if avg_latency_match:
                print(f"{'평균 지연(ms)':<27} {float(avg_latency_match.group(1)):,.2f}")

            max_latency_match = re.search(r'([\d.]+) ms max latency', result_line)
            if max_latency_match:
                print(f"{'최대 지연(ms)':<27} {float(max_latency_match.group(1)):,.2f}")

            # 실행 시간 추가
            print(f"{'실행 시간':<27} {elapsed_time:.2f}초")

        except Exception as e:
            print(f"파싱 에러: {e}")

        print("="*55)

    def _parse_and_print_consumer_result(self, output, elapsed_time):
        """Consumer 결과를 표 형식으로 파싱 및 출력"""
        lines = output.strip().split('\n')

        # 마지막 라인이 결과 데이터
        if len(lines) < 2:
            return

        result_line = lines[-1]

        headers = [
            "시작 시간",
            "종료 시간",
            "소비 데이터(MB)",
            "처리량(MB/sec)",
            "소비 메시지 수",
            "처리 속도(msg/sec)",
            "Rebalance(ms)",
            "Fetch(ms)",
            "Fetch 처리량(MB/sec)",
            "Fetch 속도(msg/sec)"
        ]

        values = result_line.split(", ")

        if len(values) != len(headers):
            return

        print("\n" + "="*55)
        print("Kafka Consumer 성능 분석")
        print("="*55)
        print("항목                       값")
        print("─"*55)

        for header, value in zip(headers, values):
            # 숫자 포맷팅
            try:
                num_value = float(value)
                if num_value > 1000:
                    formatted = f"{num_value:,.2f}" if "." in value else f"{int(num_value):,}"
                else:
                    formatted = f"{num_value:.2f}" if "." in value else value
            except:
                formatted = value

            print(f"{header:<27} {formatted}")

        # 실행 시간 추가
        print(f"{'실행 시간':<27} {elapsed_time:.2f}초")

        print("="*55)


def main():
    parser = argparse.ArgumentParser(add_help=False)

    parser.add_argument("--producer", action="store_true")
    parser.add_argument("--num-records", type=int, default=100000)
    parser.add_argument("--record-size", type=int, default=1024)
    parser.add_argument("--acks", type=str, default="all")

    parser.add_argument("--consumer", action="store_true")
    parser.add_argument("--num-messages", type=int, default=100000)
    parser.add_argument("--timeout", type=int, default=120000)

    args = parser.parse_args()

    tester = KafkaPerfTest()

    if args.producer:
        tester.producer_test(
            num_records=args.num_records,
            record_size=args.record_size,
            acks=args.acks
        )

    elif args.consumer:
        tester.consumer_test(
            num_messages=args.num_messages,
            timeout_ms=args.timeout
        )


if __name__ == "__main__":
    main()
