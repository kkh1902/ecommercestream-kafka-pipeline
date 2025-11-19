# E-commerce Clickstream Data Pipeline

**Kafka 기반 실시간 데이터 파이프라인 | CSV → Kafka → PostgreSQL → Spark**

---

## 📊 프로젝트 개요

270만 개의 E-commerce 클릭스트림 데이터를 Kafka를 통해 처리하고 PostgreSQL에 저장하는 **엔드-투-엔드 데이터 파이프라인**입니다.

**핵심:**
- CSV 파일 → Kafka로 스트리밍 (Producer)
- Kafka 메시지 → PostgreSQL 저장 (Consumer)
- 일일 통계 생성 (Spark Batch)
- 실시간 모니터링 (Kafka UI)

---

## 🏗️ 시스템 아키텍처

```
CSV Data (2.7M rows)
    ↓
[Producer] → Kafka Cluster (3 brokers)
    ↓
[Consumer] → PostgreSQL (raw_clickstream_events)
    ↓
[Spark Batch] → daily_statistics (매일 새벽 2시)

모니터링: Kafka UI (http://localhost:8090)
에러 처리: error_data 토픽
```

---

## 🚀 빠른 시작

### 1. 모든 서비스 시작

```bash
cd docker
docker-compose up -d
```

### 2. 서비스 상태 확인

```bash
docker-compose ps
```

**예상 결과:**
- ✅ producer (실행 중)
- ✅ consumer-postgres (실행 중)
- ✅ spark-streaming (실행 완료)
- ✅ kafka-broker-1,2,3 (healthy)
- ✅ postgres (healthy)

### 3. 데이터 확인

```bash
# PostgreSQL 접속
docker exec -it postgres psql -U admin -d ecommerce

# 저장된 데이터 확인
SELECT COUNT(*) FROM clickstream_events;
```

### 4. 로그 확인

```bash
# Producer 로그
docker-compose logs -f producer

# Consumer 로그
docker-compose logs -f consumer-postgres

# Spark 로그
docker-compose logs -f spark-streaming
```

---

## 📁 폴더 구조

```
comerce-kafka/
├── docker/                 # Docker 설정
│   ├── docker-compose.yml
│   ├── Dockerfile.producer
│   ├── Dockerfile.consumer
│   └── Dockerfile.spark
├── src/
│   ├── producer/           # CSV → Kafka
│   ├── consumer/           # Kafka → PostgreSQL
│   │   ├── consumer_postgres.py    (사용 중)
│   │   └── streaming_consumer.py   (미사용)
│   └── spark/              # 일일 통계 배치
│       └── jobs/batch_statistics.py
├── config/                 # 설정 (Kafka, DB 등)
├── airflow/                # Airflow DAG (개발 중)
├── sql/                    # PostgreSQL 스키마
└── README.md
```

---

## 🛠️ 기술 스택

| 레이어 | 기술 |
|--------|------|
| **메시지 브로커** | Apache Kafka 7.5.0 (3-broker 클러스터) |
| **스트림 처리** | Apache Spark 3.5.0 (local[*] 모드) |
| **데이터베이스** | PostgreSQL 16 |
| **컨테이너** | Docker & Docker Compose |
| **프로그래밍** | Python 3.10 |
| **주요 라이브러리** | kafka-python, pyspark, pandas, psycopg2 |

---

## 🎯 설계 결정사항

### Q1: 왜 Kafka?
- **고가용성**: 3-broker 클러스터로 안정성 보장
- **확장성**: 수백만 건 데이터 처리 가능
- **중앙 집중식 에러 관리**: error_data 토픽

### Q2: 왜 Kafka Consumer (Spark Streaming X)?
- **단순성**: Kafka Consumer로 충분 (배치 100개 단위)
- **안정성**: 검증된 패턴 (실무 표준)
- **오버킬 방지**: Spark Streaming은 복잡도만 증가

### Q3: 왜 배치는 Spark?
- **SQL 활용**: 복잡한 통계 계산 가능
- **성능**: 1000만 건 데이터 처리 빠름
- **유연성**: PySpark로 원하는 형태로 가공

### Q4: 에러 처리는?
- **중앙 관리**: error_data 토픽에 모든 에러 저장
- **운영 중심**: 사람의 개입 최소화
- **확장성**: Slack 알림 추가 가능 (선택사항)

---

## 📊 데이터 흐름

### Producer (CSV → Kafka)
```
src/producer/producer.py
├─ CSV 파일 읽기
├─ 데이터 검증 (필수값, 타입 확인)
├─ Kafka로 전송 (batch 모드 또는 일반 모드)
└─ 로그 기록 (logs/producer/producer.log)
```

### Consumer (Kafka → PostgreSQL)
```
src/consumer/consumer_postgres.py
├─ Kafka 메시지 수신 (계속 도는 무한 루프)
├─ 배치 단위 처리 (100개씩)
├─ PostgreSQL 저장
├─ 실패 시 error_data 토픽에 저장
└─ 로그 기록 (logs/consumer/consumer_postgres.log)
```

### Spark Batch (일일 통계)
```
src/spark/jobs/spark_jobs/batch_statistics.py
├─ 일일 2시에 Airflow DAG에서 실행
├─ PostgreSQL에서 데이터 읽기
├─ 일별 통계 계산 (매출, 이벤트, 방문자)
├─ 결과를 daily_statistics에 저장
└─ 검증 (값 범위, 논리적 오류 확인)
```

---

## 💾 데이터베이스

### 주요 테이블

| 테이블 | 설명 | 행 수 |
|--------|------|-------|
| `clickstream_events` | Consumer가 저장한 원본 이벤트 | 270만+ |
| `daily_statistics` | Spark가 생성한 일일 통계 | 계속 증가 |

---

## 📈 모니터링

### Kafka UI
```
http://localhost:8090
```
- 토픽 확인
- 메시지 흐름 추적
- Consumer lag 모니터링

### 로그 확인
```bash
docker-compose logs -f producer
docker-compose logs -f consumer-postgres
docker-compose logs -f spark-streaming
```

### PostgreSQL 데이터 확인
```bash
docker exec -it postgres psql -U admin -d ecommerce

SELECT COUNT(*) FROM clickstream_events;
SELECT COUNT(*) FROM daily_statistics;
```

---

## 🔧 트러블슈팅

| 문제 | 해결 |
|------|------|
| **Consumer가 메시지를 못 읽음** | `docker-compose logs consumer-postgres` 확인 |
| **Spark 배치 실패** | `docker-compose logs spark-streaming` 확인 |
| **PostgreSQL 연결 실패** | `docker-compose ps` 확인 (postgres가 healthy인지) |
| **Kafka UI 안 열림** | `http://localhost:8090` 재시도 (1-2분 대기) |

---

## 📚 배운 점

1. **Kafka 아키텍처**: 고가용성 설계 (3-broker 클러스터)
2. **에러 처리**: 중앙 집중식 error_data 토픽 관리
3. **기술 선택**: Trade-off 판단 (Spark vs Kafka Consumer)
4. **배치 처리**: 데이터 검증과 원자성 보장
5. **운영**: 모니터링과 자동 재시작의 중요성

---

## 🚀 다음 단계

- [ ] README 작성 (완료)
- [ ] 테스트 코드 추가 (선택)
- [ ] Airflow DAG 실행 (선택)
- [ ] Slack 알림 통합 (선택)

---

## 👤 작성자

**작성일**: 2025년 11월
**프로젝트 기간**: 약 2-3개월 (설계 + 구현)

---

**이 프로젝트는 포트폴리오용 DE 시스템입니다. 실무 학습 및 기술 역량 증명에 목적이 있습니다.**
