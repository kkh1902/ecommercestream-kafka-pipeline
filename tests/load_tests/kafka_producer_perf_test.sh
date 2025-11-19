#!/bin/bash
# Kafka Producer Performance Test
#
# 용도: Kafka Producer의 처리량, 지연시간, 대역폭을 측정
#
# 사용법:
#   ./kafka_producer_perf_test.sh [num_records] [record_size] [acks]
#
# 예시:
#   ./kafka_producer_perf_test.sh 100000 1024 all
#   ./kafka_producer_perf_test.sh 500000 512 1
#

NUM_RECORDS=${1:-100000}      # 기본값: 100,000
RECORD_SIZE=${2:-1024}        # 기본값: 1KB
ACKS=${3:-all}                # 기본값: all (acks=all)
TOPIC="perf-test"

echo "=========================================="
echo "Kafka Producer Performance Test"
echo "=========================================="
echo "Topic: $TOPIC"
echo "Number of Records: $NUM_RECORDS"
echo "Record Size: $RECORD_SIZE bytes"
echo "Acks: $ACKS"
echo "=========================================="
echo ""

# Producer 테스트 실행
docker exec kafka-broker-1 kafka-producer-perf-test \
  --topic "$TOPIC" \
  --num-records "$NUM_RECORDS" \
  --record-size "$RECORD_SIZE" \
  --throughput -1 \
  --producer-props bootstrap.servers=kafka-broker-1:19092,kafka-broker-2:19093,kafka-broker-3:19094 \
  acks="$ACKS"

echo ""
echo "=========================================="
echo "Test Completed"
echo "=========================================="
