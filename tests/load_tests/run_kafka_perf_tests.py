#!/usr/bin/env python3
"""
Kafka Performance Test Suite

용도: Kafka Producer와 Consumer의 성능을 측정합니다.

사용법:
    python run_kafka_perf_tests.py --producer --num-records 100000
    python run_kafka_perf_tests.py --consumer --num-messages 100000
    python run_kafka_perf_tests.py --all
"""

import subprocess
import argparse
import time
from datetime import datetime
from pathlib import Path


class KafkaPerfTest:
    """Kafka Performance Test Runner"""

    def __init__(self):
        self.results = []
        self.test_dir = Path(__file__).parent

    def run_producer_test(self, num_records=100000, record_size=1024, acks="all"):
        """Run Kafka Producer Performance Test"""
        print("\n" + "="*60)
        print("🚀 Kafka Producer Performance Test")
        print("="*60)
        print(f"📊 Configuration:")
        print(f"   - Records: {num_records:,}")
        print(f"   - Record Size: {record_size} bytes")
        print(f"   - Acks: {acks}")
        print(f"   - Start Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
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
            print("[STDERR]", result.stderr)

        print(f"\n⏱️  Total Execution Time: {elapsed_time:.2f} seconds")

        return {
            "test": "producer",
            "num_records": num_records,
            "record_size": record_size,
            "acks": acks,
            "status": "SUCCESS" if result.returncode == 0 else "FAILED",
            "timestamp": datetime.now().isoformat(),
            "execution_time": elapsed_time
        }

    def run_consumer_test(self, num_messages=100000, timeout_ms=120000):
        """Run Kafka Consumer Performance Test"""
        print("\n" + "="*60)
        print("📥 Kafka Consumer Performance Test")
        print("="*60)
        print(f"📊 Configuration:")
        print(f"   - Messages: {num_messages:,}")
        print(f"   - Timeout: {timeout_ms} ms")
        print(f"   - Start Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("="*60)
        print("\n⚠️  Make sure Producer test has completed!")
        print("    Consumer needs data in the topic to read.\n")

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
            print("[STDERR]", result.stderr)

        print(f"\n⏱️  Total Execution Time: {elapsed_time:.2f} seconds")

        return {
            "test": "consumer",
            "num_messages": num_messages,
            "timeout_ms": timeout_ms,
            "status": "SUCCESS" if result.returncode == 0 else "FAILED",
            "timestamp": datetime.now().isoformat(),
            "execution_time": elapsed_time
        }

    def print_summary(self):
        """Print test summary"""
        if not self.results:
            print("\n❌ No test results to summarize.")
            return

        print("\n" + "="*60)
        print("📈 Test Summary")
        print("="*60)

        for result in self.results:
            if result["test"] == "producer":
                print(f"\n✅ Producer Test")
                print(f"   - Records: {result['num_records']:,}")
                print(f"   - Record Size: {result['record_size']} bytes")
                print(f"   - Status: {result['status']}")
                print(f"   - Execution Time: {result['execution_time']:.2f}s")

            elif result["test"] == "consumer":
                print(f"\n✅ Consumer Test")
                print(f"   - Messages: {result['num_messages']:,}")
                print(f"   - Status: {result['status']}")
                print(f"   - Execution Time: {result['execution_time']:.2f}s")

        print("\n" + "="*60)

    def run_all_tests(self):
        """Run all performance tests"""
        print("\n" + "="*60)
        print("🔄 Running All Kafka Performance Tests")
        print("="*60)

        # Producer test
        producer_result = self.run_producer_test(
            num_records=100000,
            record_size=1024,
            acks="all"
        )
        self.results.append(producer_result)

        # Wait a bit before consumer test
        print("\n⏳ Waiting 10 seconds before Consumer test...")
        time.sleep(10)

        # Consumer test
        consumer_result = self.run_consumer_test(
            num_messages=100000,
            timeout_ms=120000
        )
        self.results.append(consumer_result)

        self.print_summary()


def main():
    parser = argparse.ArgumentParser(
        description="Kafka Performance Test Suite",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python run_kafka_perf_tests.py --producer
  python run_kafka_perf_tests.py --consumer
  python run_kafka_perf_tests.py --all
  python run_kafka_perf_tests.py --producer --num-records 500000 --record-size 2048
        """
    )

    parser.add_argument("--producer", action="store_true",
                        help="Run Producer performance test")
    parser.add_argument("--consumer", action="store_true",
                        help="Run Consumer performance test")
    parser.add_argument("--all", action="store_true",
                        help="Run all tests (Producer + Consumer)")

    parser.add_argument("--num-records", type=int, default=100000,
                        help="Number of records for producer test (default: 100000)")
    parser.add_argument("--record-size", type=int, default=1024,
                        help="Record size in bytes (default: 1024)")
    parser.add_argument("--acks", type=str, default="all",
                        help="Producer acks setting (default: all)")

    parser.add_argument("--num-messages", type=int, default=100000,
                        help="Number of messages for consumer test (default: 100000)")
    parser.add_argument("--timeout", type=int, default=120000,
                        help="Consumer timeout in ms (default: 120000)")

    args = parser.parse_args()

    tester = KafkaPerfTest()

    if args.producer:
        result = tester.run_producer_test(
            num_records=args.num_records,
            record_size=args.record_size,
            acks=args.acks
        )
        tester.results.append(result)
        tester.print_summary()

    elif args.consumer:
        result = tester.run_consumer_test(
            num_messages=args.num_messages,
            timeout_ms=args.timeout
        )
        tester.results.append(result)
        tester.print_summary()

    elif args.all:
        tester.run_all_tests()

    else:
        parser.print_help()


if __name__ == "__main__":
    main()
