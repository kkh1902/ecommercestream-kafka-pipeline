# E-commerce Data Pipeline Architecture

## 📊 전체 아키텍처

```mermaid
graph TB
    subgraph Ingestion["📥 Data Ingestion"]
        P["Producer<br/>(producer.py)<br/>CSV → Kafka"]
    end

    subgraph Messaging["📨 Message Broker"]
        T["Apache Kafka<br/>Topic: clickstream"]
    end

    subgraph Streaming["🔄 Real-time Streaming"]
        C1["Consumer 1<br/>(consumer_postgres.py)<br/>원본 저장"]
        C2["Consumer 2<br/>(consumer_spark_streaming.py)<br/>통계 처리"]
    end

    subgraph Storage["💾 Data Storage - PostgreSQL"]
        RAW["raw_events<br/>(원본)"]
        STATS["statistics_events<br/>(통계/BI용)"]
    end

    subgraph BatchML["⚙️ Batch Processing - Airflow"]
        DAG1["data_quality_management<br/>(매일 새벽 1시)<br/>데이터 품질 검증"]
        DAG2["daily_error_handling<br/>(매일 새벽 2시)<br/>에러 처리"]
        DAG3["daily_statistics_management<br/>(매일 새벽 3시)<br/>통계 집계"]
        DAG4["ml_training_pipeline<br/>(매주 월요일 새벽 4시)<br/>추천 모델 학습"]
        DAG5["dlq_batch_processing<br/>(매일 새벽 2시)<br/>DLQ 처리"]
    end

    subgraph ML["🤖 Machine Learning"]
        MLDATA["recommendation_data.py<br/>데이터 준비"]
        MLMODEL["recommendation_model.py<br/>XGBoost 학습"]
        MLEVAL["recommendation_metrics.py<br/>평가 지표"]
        MLENGINE["recommendation_engine.py<br/>추천 생성"]
    end

    subgraph Analytics["📈 Analytics & BI"]
        DASH["BI Dashboard<br/>통계 시각화"]
    end

    subgraph ErrorHandling["⚠️ Error Handling"]
        DLQ["failed_messages<br/>DLQ 테이블"]
    end

    P -->|Kafka Protocol| T
    T -->|Subscribe| C1
    T -->|Subscribe| C2

    C1 -->|Batch Insert| RAW
    C2 -->|특성 추가| STATS

    RAW --> DAG1
    RAW --> DAG3
    RAW --> DAG4

    DAG4 -->|raw_events 읽기| MLDATA
    MLDATA -->|특성 엔지니어링| MLMODEL
    MLMODEL -->|평가| MLEVAL
    MLEVAL -->|확률 계산| MLENGINE

    STATS --> DASH

    DAG5 -->|에러 처리| DLQ

    style P fill:#e1f5ff
    style C1 fill:#e1f5ff
    style C2 fill:#fff9c4
    style RAW fill:#f0f0f0
    style STATS fill:#f0f0f0
    style DAG1 fill:#ffe0b2
    style DAG2 fill:#ffe0b2
    style DAG3 fill:#ffe0b2
    style DAG4 fill:#c8e6c9
    style DAG5 fill:#ffe0b2
    style MLDATA fill:#c8e6c9
    style MLMODEL fill:#c8e6c9
    style MLEVAL fill:#c8e6c9
    style MLENGINE fill:#c8e6c9
```

---

## 🔄 데이터 플로우

### 1️⃣ 실시간 데이터 수집 (Consumer 1)

```mermaid
sequenceDiagram
    participant P as Producer
    participant K as Kafka
    participant C1 as Consumer 1
    participant DB as PostgreSQL

    P->>K: Send Event (clickstream)
    K->>C1: Poll Events
    C1->>C1: Batch Accumulation
    C1->>DB: Batch Insert
    DB-->>C1: Success

    Note over DB: raw_events<br/>(원본 저장)
```

### 2️⃣ 실시간 통계 처리 (Consumer 2 - Spark Streaming)

```mermaid
sequenceDiagram
    participant K as Kafka
    participant C2 as Consumer 2<br/>Spark Streaming
    participant CSV as CSV Files<br/>item_properties
    participant DB as PostgreSQL

    K->>C2: Stream Events
    CSV->>C2: Load Categories
    C2->>C2: JOIN itemid → categoryid
    C2->>C2: Add Features<br/>(event_date, hour, day_of_week)
    C2->>DB: Write

    Note over DB: statistics_events<br/>(통계/BI용)
```

### 3️⃣ 배치 ML 학습 (Airflow - 주 1회)

```mermaid
sequenceDiagram
    participant AIRFLOW as Airflow
    participant DB as PostgreSQL<br/>raw_events
    participant PYTHON as Python<br/>recommendation_data.py
    participant XGBOOST as XGBoost<br/>recommendation_model.py
    participant EVAL as Evaluation<br/>recommendation_metrics.py

    AIRFLOW->>PYTHON: Trigger
    PYTHON->>DB: SELECT raw_events
    PYTHON->>PYTHON: 정제 + 특성 추가
    PYTHON->>PYTHON: Train/Test 분할 (80/20)
    PYTHON->>XGBOOST: 데이터 전달
    XGBOOST->>XGBOOST: 모델 학습
    XGBOOST->>EVAL: 평가
    EVAL-->>AIRFLOW: Precision@10, NDCG@10
    XGBOOST->>XGBOOST: 모델 저장 (.pkl)
```

### 4️⃣ 데이터 품질 검증 (Airflow - 매일)

```mermaid
flowchart TB
    A["quality_management_dag<br/>매일 새벽 1시"]
    A --> B["check_raw_events_quality<br/>NULL/timestamp 검증"]
    A --> C["check_statistics_events_quality<br/>특성 완성도"]
    A --> D["check_ml_events_quality<br/>정제 효율성"]
    B --> E["generate_quality_report<br/>품질 보고서"]
    C --> E
    D --> E

    style A fill:#ffe0b2
    style B fill:#fff9c4
    style C fill:#fff9c4
    style D fill:#fff9c4
    style E fill:#c8e6c9
```

---

## 📋 주요 컴포넌트

### Consumer 1 (Python - 동기식)
- **파일**: `src/consumer/consumer_postgres.py`
- **역할**: 원본 데이터 저장
- **특징**:
  - 실시간 메시지 구독
  - 배치 삽입 (100건씩)
  - 에러 추적
- **저장 테이블**: `raw_events` (원본 그대로)

### Consumer 2 (Spark Streaming - 비동기식)
- **파일**: `src/consumer/consumer_spark_streaming.py`
- **역할**: 통계용 데이터 처리
- **특징**:
  - Kafka 스트림 읽기 (최신 오프셋)
  - item_properties와 JOIN
  - 시간 특성 추가 (hour, day_of_week, date)
  - Primary Key 생성 (MD5 해시)
- **저장 테이블**: `statistics_events` (통계/BI용)

### ML 파이프라인 (Airflow - 주 1회)
- **DAG**: `ml_training_pipeline.py` (매주 월도일 새벽 4시)
- **모듈**:
  1. `recommendation_data.py`: 데이터 준비 (정제 + 특성)
  2. `recommendation_model.py`: XGBoost 학습
  3. `recommendation_metrics.py`: 평가 (Precision@10, NDCG@10)
  4. `recommendation_engine.py`: 추천 생성
- **평가 방식**: 오프라인 (과거 데이터로 검증)

### Airflow DAGs

#### 1. data_quality_management (매일 새벽 1시)
- raw_events 품질 검증
- statistics_events 특성 완성도
- ml_prepared_events 정제 효율성

#### 2. daily_error_handling (매일 새벽 2시)
- 에러 로그 수집
- 에러 분류 및 분석
- 자동 재처리

#### 3. daily_statistics_management (매일 새벽 3시)
- 사용자 통계 계산
- 상품 통계 계산
- 매출 통계 계산
- 전환율 분석

#### 4. ml_training_pipeline (매주 월요일 새벽 4시)
- raw_events 읽기
- 정제 + 특성 추가
- 모델 학습
- 평가 + 특성 중요도

#### 5. dlq_batch_processing (매일 새벽 2시)
- 실패 메시지 수집
- 에러 분석
- 자동 재처리

---

## 📊 데이터 테이블 구조

### raw_events (Consumer 1에서 저장)
```
id (BIGINT PK)
├─ timestamp (BIGINT) - Unix milliseconds
├─ visitorid (INTEGER)
├─ event (VARCHAR) - view, click, purchase
├─ itemid (INTEGER)
├─ transactionid (INTEGER)
└─ created_at (TIMESTAMP)
```

### statistics_events (Consumer 2에서 저장)
```
id (BIGINT PK)
├─ timestamp, visitorid, itemid, categoryid
├─ event, transactionid
├─ event_date (YYYY-MM-DD)
├─ hour_of_day (0-23)
├─ day_of_week (1-7)
├─ is_purchase (0/1)
└─ created_at (TIMESTAMP)
```

### ML 학습 데이터 (Python 메모리에서 처리)
```
Source: raw_events (PostgreSQL)
  ↓
정제: NULL 제거, timestamp 유효성
  ↓
특성: event_hour, event_dow, event_month, ...
  ↓
분할: Train 80% / Test 20% (시간순)
  ↓
학습: XGBoost (구매 확률 예측)
  ↓
평가: Precision@10, NDCG@10
```

---

## 🔄 아키텍처 흐름 요약

```mermaid
graph LR
    A["Kafka Events<br/>실시간"] -->|C1| B["raw_events<br/>원본"]
    A -->|C2| C["statistics_events<br/>통계"]

    B -->|daily| D["Airflow<br/>배치 처리"]
    D -->|ML Train| E["XGBoost<br/>모델"]
    E -->|저장| F["models/<br/>추천 모델"]

    C -->|BI| G["Dashboard<br/>경영진 분석"]

    D -->|품질 검증| H["Quality Report<br/>데이터 품질"]
    D -->|에러 처리| I["Error Log<br/>재처리"]

    style A fill:#ffe1e1
    style B fill:#f0f0f0
    style C fill:#f0f0f0
    style E fill:#c8e6c9
    style G fill:#e1bee7
    style H fill:#ffe0b2
    style I fill:#ffccbc
```

---

## ⏰ 스케줄

```
00:00 ─ [idle]
01:00 ─ data_quality_management (품질 검증)
02:00 ─ dlq_batch_processing (에러 처리)
02:00 ─ daily_error_handling (에러 분석)
03:00 ─ daily_statistics_management (통계 집계)
04:00 ─ ml_training_pipeline (ML 학습) [월요일만]
```

---

## 📈 성능 지표

### Consumer 1 (Python)
- 배치 크기: 100건
- 처리 속도: ~1000 msg/sec

### Consumer 2 (Spark Streaming)
- 배치 크기: 10,000건/trigger
- 처리 속도: ~100,000 msg/sec (분산 처리)

### ML 모델
- 학습 데이터: raw_events 전체
- 평가: Precision@10, NDCG@10
- 목표: Precision@10 > 0.25, NDCG@10 > 0.5

---

## 🔐 데이터 보안

### Error Handling
- 실패 메시지: `logs/failed_messages/YYYY-MM-DD.jsonl`
- DLQ 테이블: `failed_messages`
- 재처리: 자동 + 수동 검토

### Data Quality
- 원본 보존: `raw_events` (모든 데이터)
- 통계 기반: `statistics_events` (정제된 특성)
- ML 학습: Python 메모리 (추가 정제)

---

## 🚀 확장성

### 수평 확장
- **Producer**: 여러 인스턴스 실행 가능
- **Consumer 1**: Consumer Group으로 파티션 분배
- **Consumer 2**: Spark 클러스터 모드

### 향후 개선
1. 실시간 추천 API 제공 (recommendation_engine)
2. 온라인 평가 (A/B 테스트)
3. ML 모델 자동 갱신
4. 상품 추천 배치 처리 (권장 상품 사전 계산)
