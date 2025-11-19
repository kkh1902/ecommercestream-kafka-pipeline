#!/bin/bash
# Kafka Consumer Performance Test
#
# 용도: Kafka Consumer의 처리량, 지연시간을 측정
#
# 사용법:
#   ./kafka_consumer_perf_test.sh [num_messages] [timeout_ms]
#
# 예시:
#   ./kafka_consumer_perf_test.sh 100000 120000
#   ./kafka_consumer_perf_test.sh 500000 300000
#
# 주의: Consumer 테스트를 하기 전에 Producer 테스트가 완료되어야 합니다.
#      Kafka에 메시지가 있어야 Consumer가 읽을 수 있습니다.
#

NUM_MESSAGES=${1:-100000}      # 기본값: 100,000
TIMEOUT_MS=${2:-120000}        # 기본값: 120초
TOPIC="perf-test"

echo "=========================================="
echo "Kafka Consumer Performance Test"
echo "=========================================="
echo "Topic: $TOPIC"
echo "Number of Messages: $NUM_MESSAGES"
echo "Timeout: $TIMEOUT_MS ms"
echo "=========================================="
echo ""
echo "📌 주의: 이 테스트를 실행하기 전에"
echo "         Producer 테스트가 완료되어야 합니다!"
echo ""

# Consumer 테스트 실행
docker exec kafka-broker-1 kafka-consumer-perf-test \
  --broker-list kafka-broker-1:19092,kafka-broker-2:19093,kafka-broker-3:19094 \
  --topic "$TOPIC" \
  --messages "$NUM_MESSAGES" \
  --timeout "$TIMEOUT_MS"

echo ""
echo "=========================================="
echo "Test Completed"
echo "=========================================="
