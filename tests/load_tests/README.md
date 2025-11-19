# Performance Test Suite

## 두 가지 성능 테스트

### 1️⃣ Kafka 성능 테스트 (kafka-perf-test)
Kafka Broker 자체의 성능을 측정합니다.
- Producer 처리량: 50,000~70,000 msg/sec
- Consumer 처리량: 30,000 msg/sec
- 순수 Kafka만 테스트

### 2️⃣ 애플리케이션 성능 테스트 (Real App)
우리가 만든 producer.py와 consumer_postgres.py의 실제 성능을 측정합니다.
- CSV 읽기 → Kafka 전송 (producer.py)
- Kafka 수신 → PostgreSQL 저장 (consumer_postgres.py)
- 전체 파이프라인 성능 (E2E)

## 핵심 차이

```
Kafka-perf-test:
  "Kafka 자체가 얼마나 빨리 메시지를 주고받을 수 있는가?"

Application Performance Test:
  "우리 애플리케이션이 실제로 얼마나 빨리 처리할 수 있는가?"
```

## 📁 구조

```
tests/load_tests/
├── README.md (이 파일)
├── kafka_producer_perf_test.sh    # Bash 스크립트 (Producer)
├── kafka_consumer_perf_test.sh    # Bash 스크립트 (Consumer)
├── run_kafka_perf_tests.py        # Python 래퍼 (추천)
└── results/                       # 테스트 결과 저장 위치 (생성됨)
```

---

## 🚀 사용법

### 1️⃣ **Python 스크립트 사용 (권장)**

#### Producer 테스트만
```bash
python run_kafka_perf_tests.py --producer
```

#### Consumer 테스트만
```bash
python run_kafka_perf_tests.py --consumer
```

#### 전체 테스트 (Producer + Consumer)
```bash
python run_kafka_perf_tests.py --all
```

#### 커스텀 설정으로 테스트
```bash
# Producer: 50만 개 메시지, 2KB 크기, acks=1
python run_kafka_perf_tests.py --producer \
  --num-records 500000 \
  --record-size 2048 \
  --acks 1

# Consumer: 50만 개 메시지, 5분 timeout
python run_kafka_perf_tests.py --consumer \
  --num-messages 500000 \
  --timeout 300000
```

### 2️⃣ **Bash 스크립트 사용**

```bash
# Producer 테스트 (기본값)
bash kafka_producer_perf_test.sh

# Producer 테스트 (커스텀)
bash kafka_producer_perf_test.sh 500000 2048 all

# Consumer 테스트 (기본값)
bash kafka_consumer_perf_test.sh

# Consumer 테스트 (커스텀)
bash kafka_consumer_perf_test.sh 500000 120000
```

---

## 📊 결과 해석

### Producer 성능 지표

```
100000 records sent, 47258.979206 records/sec (46.15 MB/sec),
461.95 ms avg latency, 820.00 ms max latency,
400 ms 50th, 766 ms 95th, 809 ms 99th, 819 ms 99.9th
```

| 항목 | 의미 | 목표 | 현재 |
|------|------|------|------|
| **records/sec** | 초당 메시지 처리량 | 높을수록 좋음 | 47,258 ✅ |
| **MB/sec** | 초당 처리 대역폭 | 높을수록 좋음 | 46.15 ✅ |
| **avg latency** | 평균 지연시간(ms) | 낮을수록 좋음 | 461.95 ⚠️ |
| **max latency** | 최대 지연시간(ms) | 낮을수록 좋음 | 820 ⚠️ |
| **50th percentile** | 중앙값 | 낮을수록 좋음 | 400 ✅ |
| **95th percentile** | 95% 이하 | 낮을수록 좋음 | 766 ⚠️ |
| **99th percentile** | 99% 이하 | 낮을수록 좋음 | 809 ⚠️ |
| **99.9th percentile** | 99.9% 이하 | 낮을수록 좋음 | 819 ⚠️ |

#### 해석 가이드

- ✅ **records/sec > 10,000**: 매우 빠름
- ✅ **records/sec 5,000-10,000**: 양호
- ⚠️ **records/sec 1,000-5,000**: 개선 필요
- ❌ **records/sec < 1,000**: 성능 문제

### Consumer 성능 지표

```
start.time, end.time, data.consumed.in.MB, MB.sec, data.consumed.in.nMsg,
nMsg.sec, rebalance.time.ms, fetch.time.ms, fetch.MB.sec, fetch.nMsg.sec
```

| 항목 | 의미 |
|------|------|
| **data.consumed.in.MB** | 소비한 데이터 크기 |
| **MB.sec** | 초당 처리 대역폭 |
| **nMsg.sec** | 초당 메시지 처리량 |
| **fetch.time.ms** | 실제 fetch 시간 |
| **fetch.nMsg.sec** | fetch 처리량 |

---

## 🧪 테스트 시나리오

### Scenario 1: 기본 성능 테스트
```bash
# 100,000 * 1KB = ~100MB
python run_kafka_perf_tests.py --producer --num-records 100000 --record-size 1024
```

### Scenario 2: 대용량 테스트
```bash
# 1,000,000 * 1KB = ~1GB
python run_kafka_perf_tests.py --producer --num-records 1000000 --record-size 1024
```

### Scenario 3: 큰 메시지 테스트
```bash
# 100,000 * 10KB = ~1GB
python run_kafka_perf_tests.py --producer --num-records 100000 --record-size 10240
```

### Scenario 4: 안정성 테스트 (acks=all)
```bash
# 모든 브로커가 수신 확인할 때까지 대기 (느리지만 안전)
python run_kafka_perf_tests.py --producer --num-records 100000 --acks all
```

### Scenario 5: 성능 테스트 (acks=1)
```bash
# Leader만 수신 확인하면 완료 (빠르지만 위험)
python run_kafka_perf_tests.py --producer --num-records 100000 --acks 1
```

---

## 📈 권장 테스트 순서

```
1️⃣ Producer 기본 테스트 (100k)
   └─ baseline 설정

2️⃣ Producer acks 비교 테스트
   ├─ acks=all (안전)
   └─ acks=1 (빠름)

3️⃣ Producer 대용량 테스트
   └─ 1,000,000+ 메시지

4️⃣ Consumer 테스트 (Producer 완료 후)
   └─ 처리량 확인

5️⃣ End-to-End 통합 테스트
   └─ Producer + Consumer 동시 실행
```

---

## ⚠️ 주의사항

### Producer 테스트
- Kafka 브로커가 모두 healthy 상태인지 확인
- 충분한 디스크 공간 필요 (최소 1GB)
- 테스트 중 다른 작업 피하기

### Consumer 테스트
- **반드시 Producer 테스트를 먼저 실행해야 함**
- Consumer가 읽을 메시지가 Kafka에 있어야 함
- Timeout 설정을 충분히 크게 (기본값: 120초)

### Docker 환경
```bash
# Docker 상태 확인
docker-compose -f docker/docker-compose.yml ps

# 모든 브로커가 healthy 상태여야 함
# kafka-broker-1, kafka-broker-2, kafka-broker-3: Up (healthy)
```

---

## 🔧 문제 해결

### "No such container: kafka-broker-1" 에러
```bash
# Docker 컨테이너 확인
docker ps | grep kafka

# 모든 컨테이너 재시작
docker-compose -f docker/docker-compose.yml restart
```

### Consumer 타임아웃 에러
```bash
# Timeout을 더 길게 설정
python run_kafka_perf_tests.py --consumer --timeout 300000  # 5분
```

### "LEADER_NOT_AVAILABLE" 경고
- Kafka 브로커가 완전히 시작되지 않은 상태
- 30초 정도 기다린 후 다시 실행

---

## 📝 현재 성능 기준선 (2025-11-19)

### Producer 성능 (100,000 records, 1KB)
```
처리량: 47,258.98 msg/sec
대역폭: 46.15 MB/sec
평균 지연시간: 461.95 ms
95th 지연시간: 766 ms
99th 지연시간: 809 ms
```

**결론**: ✅ Kafka 자체는 매우 빠름
**병목**: 실제 Producer/Consumer 애플리케이션 코드

### Consumer 성능
아직 측정 대기 중...

---

## 📖 다음 단계

1. **Producer 애플리케이션 성능 측정**
   - `src/producer/producer.py`의 실제 처리량 측정
   - CSV 읽기 최적화

2. **Consumer 애플리케이션 성능 측정**
   - `src/consumer/consumer_postgres.py`의 실제 처리량
   - 배치 크기 최적화 (현재 100 → ?)

3. **병목 지점 분석**
   - Kafka vs Producer/Consumer 성능 비교
   - DB insert 속도 최적화

4. **Spark 배치 작업 성능**
   - `src/spark/jobs/spark_jobs/batch_statistics.py` 성능 측정

---

## 📚 참고 자료

- [Apache Kafka Performance Tuning](https://kafka.apache.org/documentation/#performance)
- [Confluent Kafka Performance Tuning](https://docs.confluent.io/kafka/operations-tools/monitoring/performance-tuning.html)
