#!/usr/bin/env python3
"""
설정값별 성능 비교 분석
여러 설정 조합으로 처리 시간을 비교하여 최적 설정을 찾는다
"""

import subprocess
import time
import json
from datetime import datetime
from pathlib import Path
from typing import List, Dict
import sys

sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from config.settings import POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD
import psycopg2


class PerformanceComparison:
    def __init__(self):
        self.project_dir = Path(__file__).parent.parent.parent.parent
        self.results = []
        self.results_dir = Path("tests/load_tests/results")
        self.results_dir.mkdir(parents=True, exist_ok=True)

    def get_db_row_count(self):
        """PostgreSQL 테이블 행 수 조회"""
        try:
            conn = psycopg2.connect(
                host=POSTGRES_HOST,
                port=POSTGRES_PORT,
                database=POSTGRES_DB,
                user=POSTGRES_USER,
                password=POSTGRES_PASSWORD
            )
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM raw_clickstream_events")
            count = cursor.fetchone()[0]
            cursor.close()
            conn.close()
            return count
        except Exception as e:
            print(f"❌ DB 조회 실패: {e}")
            return 0

    def run_baseline_test(self, sample_size: int, duration: int, test_name: str) -> Dict:
        """Baseline 테스트 실행"""
        print(f"\n{'='*70}")
        print(f"📊 테스트: {test_name}")
        print(f"{'='*70}")
        print(f"Producer 샘플: {sample_size:,}개")
        print(f"Consumer 측정 시간: {duration}초")

        initial_count = self.get_db_row_count()
        print(f"초기 DB 레코드: {initial_count:,}개")

        # Producer 테스트
        print(f"\n📤 Producer 실행 중...")
        start_time = time.time()

        try:
            import pandas as pd
            from kafka import KafkaProducer

            df = pd.read_csv(self.project_dir / "data/raw/events.csv", nrows=sample_size)

            producer = KafkaProducer(
                bootstrap_servers=['localhost:9092', 'localhost:9093', 'localhost:9094'],
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                batch_size=32768,
                linger_ms=10,
                acks='all',
                retries=3
            )

            for idx, row in df.iterrows():
                message = row.to_dict()
                partition_key = str(int(message.get('visitorid', idx)))
                producer.send('clickstream',
                            key=partition_key.encode('utf-8'),
                            value=message)

            producer.flush(timeout=30)
            producer.close()

            producer_duration = time.time() - start_time
            producer_throughput = sample_size / producer_duration

        except Exception as e:
            print(f"❌ Producer 실패: {e}")
            return None

        print(f"   전송 완료: {sample_size:,}개 ({producer_duration:.2f}초)")
        print(f"   처리량: {producer_throughput:.0f} events/sec")

        # Consumer 측정 (대기)
        print(f"\n⏳ Consumer 대기 중...")
        time.sleep(10)  # Consumer가 처리할 시간

        initial_consumer_count = self.get_db_row_count()
        print(f"측정 시작 DB 레코드: {initial_consumer_count:,}개")

        print(f"⏱️  {duration}초 동안 처리 모니터링...")
        start_time = time.time()

        while time.time() - start_time < duration:
            time.sleep(10)

        final_count = self.get_db_row_count()
        consumer_duration = time.time() - start_time
        processed_count = final_count - initial_consumer_count

        consumer_throughput = processed_count / consumer_duration if consumer_duration > 0 else 0

        print(f"\n📊 결과:")
        print(f"   처리된 이벤트: {processed_count:,}개")
        print(f"   소요 시간: {consumer_duration:.2f}초")
        print(f"   Consumer 처리량: {consumer_throughput:.0f} events/sec")

        result = {
            "test_name": test_name,
            "timestamp": datetime.now().isoformat(),
            "producer": {
                "sample_size": sample_size,
                "duration_seconds": producer_duration,
                "throughput_events_per_sec": producer_throughput
            },
            "consumer": {
                "initial_count": initial_consumer_count,
                "final_count": final_count,
                "processed_count": processed_count,
                "duration_seconds": consumer_duration,
                "throughput_events_per_sec": consumer_throughput
            },
            "total_processed": processed_count,
            "total_time_seconds": producer_duration + consumer_duration,
            "overall_throughput": (processed_count) / (producer_duration + consumer_duration)
        }

        self.results.append(result)
        return result

    def generate_comparison_report(self):
        """비교 리포트 생성"""
        if not self.results:
            print("❌ 테스트 결과 없음")
            return

        print(f"\n\n{'='*100}")
        print("📊 성능 비교 분석 리포트")
        print(f"{'='*100}\n")

        # 테이블 헤더
        print(f"{'테스트명':<25} {'Producer':<15} {'Consumer':<15} {'총 시간':<12} {'최적화':<10}")
        print(f"{'':25} {'(events/sec)':<15} {'(events/sec)':<15} {'(초)':<12} {'효과':<10}")
        print("-" * 100)

        # 기본값 (첫 번째 결과)
        baseline = self.results[0] if self.results else None

        for result in self.results:
            test_name = result['test_name']
            producer_throughput = result['producer']['throughput_events_per_sec']
            consumer_throughput = result['consumer']['throughput_events_per_sec']
            total_time = result['total_time_seconds']

            # 개선도 계산
            if baseline:
                baseline_time = baseline['total_time_seconds']
                improvement = ((baseline_time - total_time) / baseline_time * 100) if baseline_time > 0 else 0
                improvement_text = f"{improvement:+.1f}%"
            else:
                improvement_text = "-"

            print(f"{test_name:<25} {producer_throughput:>14.0f} {consumer_throughput:>14.0f} {total_time:>11.2f} {improvement_text:>9}")

        print("\n" + "="*100)

        # 최고 성능 테스트
        best_by_consumer = max(self.results, key=lambda x: x['consumer']['throughput_events_per_sec'])
        best_by_time = min(self.results, key=lambda x: x['total_time_seconds'])

        print(f"\n🏆 최고 성능:")
        print(f"   Consumer 처리량 최고: {best_by_consumer['test_name']}")
        print(f"   └─ {best_by_consumer['consumer']['throughput_events_per_sec']:.0f} events/sec")
        print(f"\n⚡ 최단 시간:")
        print(f"   {best_by_time['test_name']}")
        print(f"   └─ 총 {best_by_time['total_time_seconds']:.2f}초 ({best_by_time['total_processed']:,}개 처리)")

        # 개선도
        if len(self.results) > 1:
            baseline = self.results[0]
            best = min(self.results[1:], key=lambda x: x['total_time_seconds'])
            time_saved = baseline['total_time_seconds'] - best['total_time_seconds']
            improvement_percent = (time_saved / baseline['total_time_seconds'] * 100)

            print(f"\n📈 최적화 효과:")
            print(f"   기본값 대비: {time_saved:.2f}초 단축 ({improvement_percent:.1f}% 개선)")
            print(f"   기본값: {baseline['total_time_seconds']:.2f}초 → 최적값: {best['total_time_seconds']:.2f}초")

        print("\n" + "="*100)

        # 결과 저장
        self.save_results()

    def save_results(self):
        """결과 JSON 저장"""
        results_file = self.results_dir / "performance_comparison.json"
        with open(results_file, 'w', encoding='utf-8') as f:
            json.dump(self.results, f, indent=2, ensure_ascii=False)
        print(f"✅ 결과 저장: {results_file}")

    def run_all_tests(self):
        """모든 테스트 실행"""
        print("\n" + "="*70)
        print("🚀 설정값별 성능 비교 분석 시작")
        print("="*70)

        test_configs = [
            {
                "name": "Test 1: 기본 설정 (기준선)",
                "sample": 100000,
                "duration": 120
            },
            {
                "name": "Test 2: 큰 샘플 (150K)",
                "sample": 150000,
                "duration": 150
            },
            {
                "name": "Test 3: 매우 큰 샘플 (200K)",
                "sample": 200000,
                "duration": 180
            },
        ]

        for config in test_configs:
            try:
                self.run_baseline_test(
                    sample_size=config['sample'],
                    duration=config['duration'],
                    test_name=config['name']
                )
                time.sleep(5)  # 테스트 간 대기
            except Exception as e:
                print(f"❌ 테스트 실패: {e}")
                continue

        self.generate_comparison_report()


def main():
    comparator = PerformanceComparison()
    comparator.run_all_tests()


if __name__ == "__main__":
    main()
