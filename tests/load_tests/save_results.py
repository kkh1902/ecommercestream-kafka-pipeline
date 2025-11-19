#!/usr/bin/env python3
"""
Kafka Performance Test Results Saver

테스트 결과를 JSON 파일로 저장합니다.
"""

import json
import subprocess
import re
from datetime import datetime
from pathlib import Path


def parse_producer_output(output):
    """Parse producer test output"""
    # 예: 100000 records sent, 47258.979206 records/sec (46.15 MB/sec)...
    match = re.search(
        r'(\d+)\s+records sent,\s+([\d.]+)\s+records/sec\s+\(([\d.]+)\s+MB/sec\),\s+'
        r'([\d.]+)\s+ms avg latency,\s+([\d.]+)\s+ms max latency,\s+'
        r'([\d.]+)\s+ms 50th,\s+([\d.]+)\s+ms 95th,\s+([\d.]+)\s+ms 99th,\s+([\d.]+)\s+ms 99.9th',
        output
    )

    if match:
        return {
            "records_sent": int(match.group(1)),
            "records_per_sec": float(match.group(2)),
            "mb_per_sec": float(match.group(3)),
            "avg_latency_ms": float(match.group(4)),
            "max_latency_ms": float(match.group(5)),
            "p50_latency_ms": float(match.group(6)),
            "p95_latency_ms": float(match.group(7)),
            "p99_latency_ms": float(match.group(8)),
            "p999_latency_ms": float(match.group(9))
        }
    return None


def save_results(test_name, metrics, config):
    """Save test results to JSON file"""
    results_dir = Path(__file__).parent / "results"
    results_dir.mkdir(exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = results_dir / f"{test_name}_{timestamp}.json"

    result = {
        "test_name": test_name,
        "timestamp": datetime.now().isoformat(),
        "configuration": config,
        "metrics": metrics
    }

    with open(filename, 'w') as f:
        json.dump(result, f, indent=2)

    print(f"\n✅ Results saved: {filename}")
    return str(filename)


def run_and_save_producer_test(num_records=100000, record_size=1024, acks="all"):
    """Run producer test and save results"""
    print("\n" + "="*60)
    print("Running Producer Test & Saving Results...")
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

    result = subprocess.run(cmd, capture_output=True, text=True)
    metrics = parse_producer_output(result.stdout + result.stderr)

    config = {
        "num_records": num_records,
        "record_size": record_size,
        "acks": acks,
        "brokers": "kafka-broker-1, kafka-broker-2, kafka-broker-3"
    }

    if metrics:
        print("\n📊 Metrics:")
        print(f"   - Records/sec: {metrics['records_per_sec']:,.2f}")
        print(f"   - MB/sec: {metrics['mb_per_sec']:.2f}")
        print(f"   - Avg Latency: {metrics['avg_latency_ms']:.2f} ms")
        print(f"   - P95 Latency: {metrics['p95_latency_ms']:.2f} ms")
        print(f"   - P99 Latency: {metrics['p99_latency_ms']:.2f} ms")

        filename = save_results("producer", metrics, config)
        return filename
    else:
        print("\n❌ Failed to parse producer output")
        return None


if __name__ == "__main__":
    # Example usage
    filename = run_and_save_producer_test(
        num_records=100000,
        record_size=1024,
        acks="all"
    )

    if filename:
        print(f"\n📁 Result file: {filename}")
