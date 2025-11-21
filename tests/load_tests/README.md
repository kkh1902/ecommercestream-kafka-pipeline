# 도커 확인
docker ps
# Kafka produce 성능 테스트
python tests/load_tests/run_kafka_perf_tests.py --producer --num-records 100000 --record-size 1024 --acks all
# Kafka Consumer성능 테스트
python tests/load_tests/run_kafka_perf_tests.py --consumer --num-messages 100000 --timeout 120000

# Topic 삭제
docker exec kafka-broker-1 kafka-topics --delete --topic perf-test --bootstrap-server kafka-broker-1:9092
# perf-test가 없음 (성공적으로 삭제됨)

# Topic 확인 
docker exec kafka-broker-1 kafka-topics --list --bootstrap-server kafka-broker-1:9092
# 3초후 
sleep3

# Topic 생성
docker exec kafka-broker-1 kafka-topics --create --topic perf-test --partitions 3 --replication-factor 3 --bootstrap-server kafka-broker-1:9092

# app_producer 테스트
python tests/load_tests/app_producer_perf_test.py  #  5000개

# app_streaming_consumer_perf_test  테스트
python tests/load_tests/app_streaming_consumer_perf_test.py --timeout 120




# Topic 초기화 (필수!)
docker exec kafka-broker-1 kafka-topics --delete --topic perf-test --bootstrap-server kafka-broker-1:9092 && \
sleep 3 && \
docker exec kafka-broker-1 kafka-topics --create --topic perf-test --partitions 3 --replication-factor 3 --bootstrap-server kafka-broker-1:9092

# Producer 실행 (필수!)
python tests/load_tests/run_kafka_perf_tests.py --producer

# Streaming Consumer 실행
    # 1 DB 초기화
    docker exec postgres psql -U admin -d ecommerce -c "TRUNCATE TABLE clickstream_events RESTART IDENTITY;"

    # Topic 초기화 (삭제 + 생성)
    docker exec kafka-broker-1 kafka-topics --delete --topic perf-test --bootstrap-server kafka-broker-1:9092 && \
    sleep 3 && \
    docker exec kafka-broker-1 kafka-topics --create --topic perf-test --partitions 3 --replication-factor 3 --bootstrap-server kafka-broker-1:9092

    # 3 Producer 실행 (100,000개 메시지 생성)
    python tests/load_tests/run_kafka_perf_tests.py --producer

    # 4️ Streaming Consumer 실행 (120초)
    python tests/load_tests/app_streaming_consumer_perf_test.py --timeout 120