# 📊 Kafka + Spark Streaming 모니터링 스택

Prometheus + Grafana를 사용한 Kafka와 Spark Streaming의 실시간 메트릭 모니터링

## **🚀 빠른 시작**

### **1️⃣ 모니터링 스택 시작**

```bash
# 모니터링 서비스 실행
docker-compose -f monitoring/docker-compose.monitoring.yml up -d
```

### **2️⃣ 접근 주소**

| 서비스 | URL | 설명 |
|--------|-----|------|
| **Grafana** | http://localhost:3000 | 대시보드 (admin/admin123) |
| **Prometheus** | http://localhost:9090 | 메트릭 저장소 |
| **Spark UI** | http://localhost:4040 | Spark 실시간 UI |
| **Kafka UI** | http://localhost:8090 | Kafka 관리 UI |

---

## **📊 대시보드 설명**

### **Dashboard 1: Kafka 모니터링**

```
주요 메트릭:
├─ Kafka Brokers: 브로커 상태 (3개 모두 정상?)
├─ Kafka Topics: 현재 Topic 개수
├─ Consumer Group Lag: Consumer가 처리하지 못한 메시지 수
├─ Clickstream Topic Messages: 초당 메시지 처리량
└─ Bytes In/Out Rate: 네트워크 처리량
```

**언제 확인해야 하나?**
- Consumer Lag이 계속 증가 → Spark가 처리하지 못하는 상황
- 특정 Broker가 Down → Kafka 문제

---

### **Dashboard 2: Spark Streaming 모니터링**

```
주요 메트릭:
├─ Total Records Processed: 누적 처리 레코드 수
├─ Active Batches: 현재 실행 중인 배치 수
├─ Batches Submitted: 초당 제출된 배치 수
├─ Records Input/Output Rate: 입력/출력 속도 (records/sec)
├─ Batch Processing Time: 배치당 소요 시간
└─ Total Delay: 전체 처리 지연 시간
```

**언제 확인해야 하나?**
- Processing Time 급증 → 처리 속도 저하
- Total Delay 증가 → 실시간 처리가 밀리고 있음
- Input Rate > Output Rate → 병목 상황

---

## **🔧 구조**

```
monitoring/
├─ prometheus.yml              # Prometheus 설정
├─ docker-compose.monitoring.yml
├─ grafana/
│  └─ provisioning/
│     ├─ datasources/
│     │  └─ prometheus.yaml    # Prometheus 연결 설정
│     └─ dashboards/
│        ├─ kafka.json         # Kafka 대시보드
│        ├─ spark.json         # Spark Streaming 대시보드
│        └─ dashboards.yaml    # 대시보드 자동 로드 설정
└─ README.md
```

---

## **📈 메트릭 이해하기**

### **Kafka 메트릭**

```
Consumer Lag (중요!) = Producer Offset - Consumer Offset

예:
- Producer가 100,000번째 메시지 작성
- Consumer가 95,000번째 메시지까지만 처리
- Lag = 5,000 (처리 못 한 메시지)

→ Lag이 계속 증가하면? Consumer(Spark)가 처리하지 못하는 상황
```

### **Spark Streaming 메트릭**

```
Batch Processing Time = 한 배치 처리에 걸리는 시간

예:
- Batch interval: 2초
- Processing time: 1.5초 → 정상 (시간 내 처리)
- Processing time: 2.5초 → 지연 (다음 배치와 겹침)

Total Delay = Batch생성시간 - 처리완료시간
→ 클수록 실시간성이 떨어짐
```

---

## **⚠️ 모니터링 팁**

### **1️⃣ 정상 상황**
- Consumer Lag: 안정적 (증가 안 함)
- Batch Processing Time: 일정
- Records Input ≈ Records Output

### **2️⃣ 문제 상황 감지**
- **Kafka 병목**: Consumer Lag 급증
- **Spark 병목**: Processing Time 급증
- **Network 병목**: Bytes In/Out Rate 부족

### **3️⃣ 대응 방안**
| 문제 | 확인할 메트릭 | 해결 방법 |
|------|-------------|---------|
| Lag 증가 | Consumer Lag | Spark executor 수 증가 |
| Processing 느림 | Batch Processing Time | Batch interval 늘리기 |
| Broker Down | Kafka Brokers | Kafka 재시작 |

---

## **🛑 서비스 중지**

```bash
# 모니터링 스택 중지
docker-compose -f monitoring/docker-compose.monitoring.yml down

# 모든 데이터 삭제
docker-compose -f monitoring/docker-compose.monitoring.yml down -v
```

---

## **📝 Prometheus 쿼리 예제**

Prometheus http://localhost:9090 에서 직접 쿼리 실행:

```promql
# 최근 5분간 초당 Kafka 메시지 처리량
rate(kafka_topic_partition_current_offset[5m])

# Spark 누적 처리 레코드 수
spark_streaming_streaming_total_records_processed

# Consumer Lag (Consumer가 처리하지 못한 메시지)
kafka_consumergroup_lag

# Spark 배치 처리 시간 (ms)
spark_streaming_streaming_batch_duration_total
```

---

## **🔗 관련 링크**

- [Prometheus 공식 문서](https://prometheus.io/docs/)
- [Grafana 공식 문서](https://grafana.com/docs/)
- [Kafka Exporter](https://github.com/danielqsj/kafka-exporter)
- [Spark Metrics](https://spark.apache.org/docs/latest/monitoring.html)

---

**마지막 업데이트**: 2025-11-20
