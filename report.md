# 프로젝트 설정 종합 분석 리포트
## Commerce Kafka 프로젝트 - Spark 및 DB 연결 설정

**작성일**: 2025-11-21
**프로젝트**: comerce-kafka
**분석 범위**: Spark 설정, 데이터베이스 연결, 환경 관리, 실무 모범 사례

---

## 📋 목차
1. [현재 프로젝트 설정 분석](#1-현재-프로젝트-설정-분석)
2. [Spark 설정 상세 분석](#2-spark-설정-상세-분석)
3. [데이터베이스 연결 설정](#3-데이터베이스-연결-설정)
4. [환경별 설정 비교](#4-환경별-설정-비교)
5. [실무 모범 사례](#5-실무-모범-사례)
6. [현재 프로젝트의 강점과 개선 사항](#6-현재-프로젝트의-강점과-개선-사항)
7. [설정 보고 체계](#7-설정-보고-체계)

---

## 1. 현재 프로젝트 설정 분석

### 1.1 프로젝트 구조

```
comerce-kafka/
├── .env                          # 환경 변수 (Git 제외)
├── config/settings.py            # 설정 파일 (중앙 집중식)
├── docker/
│   ├── docker-compose.yml        # 전체 인프라 정의
│   ├── spark/                    # Spark Streaming 컨테이너
│   ├── spark-master/             # Spark Master 노드
│   ├── spark-worker/             # Spark Worker 노드 (×3)
│   ├── monitoring/               # Prometheus + Grafana
│   └── airflow/                  # Apache Airflow
├── src/
│   ├── consumer/streaming_consumer.py    # Spark Streaming Job
│   ├── spark/jobs/batch_statistics.py    # 배치 작업
│   └── utils/postgres_utils.py           # DB 연결 헬퍼
└── airflow/dags/                 # DAG 정의
```

### 1.2 핵심 설정 파일 3개

| 파일명 | 역할 | 내용 |
|--------|------|------|
| `.env` | 환경 변수 저장 | Kafka, PostgreSQL, Slack, Email 설정 |
| `config/settings.py` | 설정 로더 | dotenv 로드 및 타입 변환 |
| `docker-compose.yml` | 인프라 정의 | 전체 서비스 컨테이너화 |

---

## 2. Spark 설정 상세 분석

### 2.1 Spark 실행 환경별 설정

#### 환경 1: 로컬 개발 환경 (Standalone)
**파일**: `src/consumer/streaming_consumer.py`

```python
# Spark 마스터 설정
.master("local[*]")  # 모든 CPU 코어 사용

# 주요 설정
.config("spark.sql.streaming.checkpointLocation", "/tmp/spark-checkpoint")
.config("spark.sql.adaptive.enabled", "true")  # 적응형 쿼리 실행 (3.2+)
.config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0")
```

**특징**:
- ✅ 개발 환경에 최적화
- ✅ 추가 설정 없이 로컬에서 즉시 실행 가능
- ⚠️ 단일 머신 리소스로 제한됨
- ⚠️ 프로덕션 부하 테스트 불가

**사용 케이스**:
- 로컬 개발 및 디버깅
- 프로토타이핑
- 단위 테스트

---

#### 환경 2: Docker 컨테이너 클러스터
**파일**: `docker/docker-compose.yml`

```yaml
# Spark Master
spark-master:
  image: bitnami/spark:3.4.2-debian-11-r19
  environment:
    SPARK_MODE: master
    SPARK_RPC_AUTHENTICATION_ENABLED: no
  ports:
    - "7077:7077"   # Spark Master 포트
    - "8080:8080"   # Web UI

# Spark Worker (×3)
spark-worker-[1-3]:
  image: bitnami/spark:3.4.2-debian-11-r19
  environment:
    SPARK_MODE: worker
    SPARK_MASTER_URL: spark://spark-master:7077
    SPARK_WORKER_CORES: 2
    SPARK_WORKER_MEMORY: 2G
```

**Spark Streaming 제출**:
```bash
spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,\
            org.postgresql:postgresql:42.6.0 \
  /app/src/consumer/streaming_consumer.py
```

**클러스터 구성**:
- Master 1개: 7077 포트 (클러스터 조정)
- Worker 3개: 각각 2 코어, 2GB 메모리
- 총 리소스: 6 코어, 6GB 메모리

**특징**:
- ✅ 분산 처리 가능
- ✅ 컨테이너 기반으로 환경 일관성 보장
- ✅ 수평 확장 용이 (Worker 추가)
- ⚠️ 오버헤드: 마스터-워커 통신 비용

**사용 케이스**:
- 개발 환경 검증
- 성능 테스트
- 초기 프로덕션 배포

---

#### 환경 3: Airflow 오케스트레이션
**파일**: `airflow/dags/daily_statistics_batch.py`

```python
# Spark 공통 설정
spark_conf = {
    'spark.driver.memory': '2g',          # Driver 프로세스 메모리
    'spark.executor.memory': '2g',        # 각 Executor 메모리
    'spark.executor.cores': '2',          # 각 Executor의 CPU 코어 수
    'spark.network.timeout': '600s',      # 네트워크 타임아웃 (10분)
    'spark.task.maxFailures': '2'         # 태스크 재시도 횟수
}

# SparkSubmitOperator 실행
SparkSubmitOperator(
    task_id='collect_daily_statistics',
    application=f'{PROJECT_ROOT}/src/spark/jobs/spark_jobs/batch_statistics.py',
    conf=spark_conf,
    packages='org.postgresql:postgresql:42.6.0',
    jars=f'{PROJECT_ROOT}/src/spark/jars/postgresql-42.7.3.jar',
    num_executors=1,
    executor_memory='2g',
    executor_cores=2,
    driver_memory='2g'
)
```

**DAG 설정**:
```python
default_args = {
    'owner': 'analytics-team',
    'depends_on_past': False,        # 어제 실패해도 독립적으로 실행
    'start_date': datetime(2025, 11, 1),
    'retries': 2,                   # 2회 자동 재시도
    'retry_delay': timedelta(minutes=5)  # 5분 간격 재시도
}

# 스케줄: 매일 새벽 2시 (UTC 기준)
schedule_interval='0 2 * * *'
```

**특징**:
- ✅ 스케줄링 자동화
- ✅ 실패 시 자동 재시도
- ✅ 의존성 관리 가능
- ✅ 실행 이력 추적
- ✅ 웹 UI에서 모니터링

**사용 케이스**:
- 프로덕션 배치 작업
- 정기적인 데이터 파이프라인
- 복잡한 워크플로우

---

### 2.2 Spark 설정 요약 표

| 항목 | 로컬 개발 | Docker 클러스터 | Airflow |
|------|---------|-----------------|---------|
| **마스터 URL** | local[*] | spark://spark-master:7077 | local[*] (기본) |
| **메모리 (Driver)** | 시스템 기본 | - | 2GB |
| **메모리 (Executor)** | 시스템 기본 | 2GB/워커 | 2GB |
| **코어 (CPU)** | 모든 코어 | 2/워커 × 3 = 6 | 2 |
| **총 병렬성** | 제한 없음 | 6 | 2 |
| **배포 모드** | client | client | client |
| **JAR/Package** | 로컬 경로 | 마운트 + 다운로드 | Airflow 서버에서 제출 |
| **체크포인트** | /tmp/spark-checkpoint | 컨테이너 내부 | DAG 실행 경로 |
| **타임아웃** | 기본값 | 기본값 | 600초 (10분) |

---

## 3. 데이터베이스 연결 설정

### 3.1 PostgreSQL 연결 구성

**설정 로드 경로**:
```
.env → config/settings.py → 각 모듈에서 import
```

**설정 파일**: `config/settings.py`
```python
POSTGRES_HOST = os.getenv('POSTGRES_HOST', 'localhost')
POSTGRES_PORT = int(os.getenv('POSTGRES_PORT', 5432))
POSTGRES_DB = os.getenv('POSTGRES_DB', 'ecommerce')
POSTGRES_USER = os.getenv('POSTGRES_USER', 'admin')
POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD', 'admin123')
```

### 3.2 환경별 DB 연결

#### 개발 환경 (로컬)
```
Host: localhost:5432
Database: ecommerce
User: admin
Password: admin123
```

**연결 방식**:
- Python: `psycopg2` 직접 연결
- Spark: JDBC URL via `postgresql:42.7.3.jar`
  ```
  jdbc:postgresql://localhost:5432/ecommerce
  ```

#### Docker 환경
```yaml
postgres:
  image: postgres:16
  environment:
    POSTGRES_DB: ecommerce
    POSTGRES_USER: admin
    POSTGRES_PASSWORD: admin123
  ports:
    - "5432:5432"
  volumes:
    - postgres-volume:/var/lib/postgresql/data
    - ../sql/schema.sql:/docker-entrypoint-initdb.d/schema.sql
```

**컨테이너 간 연결**:
```
Host: postgres          # Docker 네트워크 이름 해석
Port: 5432
```

#### Airflow 메타 DB (별도)
```yaml
postgres-airflow:
  image: postgres:16
  environment:
    POSTGRES_DB: airflow
    POSTGRES_USER: airflow
    POSTGRES_PASSWORD: airflow123
  ports:
    - "5433:5432"
```

**용도**: Airflow DAG 메타데이터, 작업 실행 이력 저장

---

### 3.3 JDBC 연결 설정 (Spark)

**Spark에서 PostgreSQL 연결**:

```python
jdbc_url = f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
jdbc_properties = {
    "user": POSTGRES_USER,
    "password": POSTGRES_PASSWORD,
    "driver": "org.postgresql.Driver"
}

# 데이터 쓰기
df.write \
    .format("jdbc") \
    .mode("append") \
    .option("url", jdbc_url) \
    .option("dbtable", "daily_statistics") \
    .options(**jdbc_properties) \
    .save()
```

**필수 JAR 파일**:
- `postgresql-42.7.3.jar` (1.09 MB)
- 위치: `src/spark/jars/`

---

### 3.4 연결 풀 관리

**Python 클라이언트** (`src/utils/postgres_utils.py`):
```python
def get_postgres_connection():
    conn = psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        database=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD
    )
    return conn

def batch_insert(table, records, page_size=1000):
    # 배치 삽입으로 네트워크 오버헤드 감소
    # 1000개 단위로 INSERT 실행
    pass
```

**특징**:
- ✅ 연결당 하나의 SQL 트랜잭션 관리
- ⚠️ 연결 풀링 미구현 (향후 개선 필요)
- ⚠️ 동시성 제한됨

---

## 4. 환경별 설정 비교

### 4.1 3가지 환경 설정 매트릭스

| 항목 | 로컬 개발 | Docker | Airflow | 프로덕션* |
|------|---------|--------|---------|----------|
| **Kafka Broker** | localhost:9092 | kafka-broker-1:19092 | 컨테이너 네트워크 | 관리형 서비스 |
| **PostgreSQL** | localhost:5432 | postgres:5432 | postgres:5432 | AWS RDS/GCP Cloud SQL |
| **Spark Master** | local[*] | spark://spark-master:7077 | local[*] | YARN/K8s |
| **메모리 (Driver)** | 시스템 기본 | 미지정 | 2GB | 환경별 |
| **메모리 (Executor)** | 시스템 기본 | 2GB/워커 | 2GB | 환경별 |
| **로그 저장** | 로컬 파일 | 컨테이너 내부 | Airflow 로그 | 중앙 로깅 (ELK, CloudWatch) |
| **모니터링** | 없음 | Prometheus + Grafana | Airflow UI | 전사 모니터링 |

*프로덕션 권장사항

---

### 4.2 환경 변수 관리

**계층 구조**:
```
개발자 PC
├── .env.example (Git 커밋)
│   ├─→ 신입 가이드 역할
│   └─→ 설정 구조 문서화
└── .env (Git 제외)
    ├─→ 실제 값 저장
    └─→ 보안 위험: 자격증명 노출

Docker 환경
├── 컨테이너 .env 마운트
│   └─→ docker run -e 또는 env_file로 주입
└── docker-compose.yml
    └─→ environment 섹션에서 정의

Airflow
├── airflow.cfg (Airflow 설정)
├── SparkSubmitOperator conf 파라미터
│   └─→ 런타임 설정 오버라이드
└── /opt/airflow/.env (별도)
```

---

## 5. 실무 모범 사례

### 5.1 Spark 설정 관리

#### 1️⃣ 환경별 독립적인 설정 파일

**권장 구조**:
```
config/
├── __init__.py
├── base.py          # 공통 설정
├── dev.py           # 개발 환경
├── docker.py        # Docker 환경
├── airflow.py       # Airflow 환경
└── prod.py          # 프로덕션
```

**예시** (`config/base.py`):
```python
class BaseConfig:
    SPARK_VERSION = "3.5.0"
    NETWORK_TIMEOUT = "600s"
    TASK_MAX_FAILURES = 2
```

**예시** (`config/docker.py`):
```python
from .base import BaseConfig

class DockerConfig(BaseConfig):
    SPARK_MASTER = "spark://spark-master:7077"
    EXECUTOR_MEMORY = "2g"
    EXECUTOR_CORES = 2
    NUM_EXECUTORS = 3
```

#### 2️⃣ 메모리 설정 가이드라인

**Driver 메모리** (`spark.driver.memory`):
```
개발: 512MB - 1GB
프로덕션: 2GB - 4GB (대용량 수집, 복잡한 변환)
```

**Executor 메모리** (`spark.executor.memory`):
```
경험 공식: (총 노드 메모리 - 1GB 오버헤드) / executor_개수

예: 8GB 노드, 2개 Executor
= (8 - 1) / 2 = 3.5GB/Executor
```

**코어 수** (`spark.executor.cores`):
```
권장: 2-4 (과도한 스레드 경합 피하기)
일반적: 2
CPU 집약: 4
IO 집약: 6-8
```

#### 3️⃣ 타임아웃 설정

```python
# 네트워크 타임아웃 (Spark 간 통신)
'spark.network.timeout': '600s'  # 기본 120s, 대량 데이터 전송 시 증가

# Task 타임아웃
'spark.executor.heartbeatInterval': '60s'  # 기본값
```

#### 4️⃣ 적응형 쿼리 실행 (AQE)

Spark 3.2+에서 권장:
```python
.config("spark.sql.adaptive.enabled", "true")
.config("spark.sql.adaptive.skewJoin.enabled", "true")
.config("spark.sql.adaptive.coalescePartitions.enabled", "true")
```

**이점**:
- 파티션 크기 자동 조정
- Skew 데이터 자동 감지
- 동적 쿼리 최적화

---

### 5.2 데이터베이스 연결 관리

#### 1️⃣ 연결 풀링 (필수)

**문제**: 현재 프로젝트는 연결당 1개씩 생성
```python
# ❌ 현재 방식 (비효율)
conn = psycopg2.connect(...)  # 매번 새로운 연결
```

**해결책**: `psycopg2-pool` 또는 `SQLAlchemy`
```python
from psycopg2 import pool

# ✅ 개선 방식
connection_pool = psycopg2.pool.SimpleConnectionPool(
    min_connections=5,
    max_connections=20,
    host=POSTGRES_HOST,
    user=POSTGRES_USER,
    password=POSTGRES_PASSWORD,
    database=POSTGRES_DB
)

def get_connection():
    return connection_pool.getconn()

def put_connection(conn):
    connection_pool.putconn(conn)
```

#### 2️⃣ Spark JDBC 최적화

```python
df.read \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", table_name) \
    .option("numPartitions", 4)  # 병렬 읽기 파티션
    .option("partitionColumn", "id")  # 파티션 기준 열
    .option("lowerBound", 1)
    .option("upperBound", 1000000)
    .option("fetchsize", 10000)  # 배치 크기
    .load()
```

#### 3️⃣ JDBC 드라이버 관리

**현재 상태**:
```
src/spark/jars/
├── postgresql-42.6.0.jar    (1.08 MB)
├── postgresql-42.7.3.jar    (1.09 MB)  ← 중복
└── ...
```

**문제**: 버전 혼용, 중복

**해결책**:
```
jars/
├── VERSIONS.md           # 버전 관리 문서
├── postgresql-42.7.3.jar # 최신 버전만 유지
└── kafka-clients-3.5.1.jar
```

#### 4️⃣ 보안: 자격증명 관리

**현재 상태** (❌ 위험):
```python
# .env 파일에 평문 저장
POSTGRES_PASSWORD=admin123
```

**개선 방식** (✅ 권장):

**옵션 1: 환경 변수 (개발)**
```bash
export POSTGRES_PASSWORD="$(aws secretsmanager get-secret-value --secret-id postgres-password)"
```

**옵션 2: AWS Secrets Manager (프로덕션)**
```python
import boto3

secrets_client = boto3.client('secretsmanager')
secret = secrets_client.get_secret_value(SecretId='postgres-password')
password = json.loads(secret['SecretString'])['password']
```

**옵션 3: HashiCorp Vault**
```python
from hvac import Client

client = Client(url='http://vault:8200')
secret = client.secrets.kv.read_secret_version(path='postgres-creds')
password = secret['data']['data']['password']
```

---

### 5.3 Airflow DAG 설정 모범 사례

**현재 상태**:
```python
# ✅ 좋은 점
default_args = {
    'owner': 'analytics-team',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5)
}
schedule_interval='0 2 * * *'  # 매일 새벽 2시
```

**개선 권장사항**:

```python
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'analytics-team',
    'depends_on_past': False,
    'start_date': days_ago(1),          # 어제부터 시작
    'email_on_failure': True,            # 실패 시 알림
    'email': ['alerts@company.com'],
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=2),  # 최대 실행 시간
}

dag = DAG(
    'daily_statistics_batch',
    default_args=default_args,
    description='일일 통계 수집',
    schedule_interval='0 2 * * *',
    catchup=False,
    tags=['statistics', 'spark', 'prod'],
    max_active_runs=1,  # 동시 실행 제한
    doc_md=__doc__,     # DAG 문서화
)

# Task 간 의존성 명시
prepare_data >> compute_stats >> notify_completion
```

---

### 5.4 로깅 및 모니터링

**현재 상태**:
```
logs/
├── producer/
├── consumer/
└── spark/
```

**문제**: 로컬 파일 저장, 중앙 집중식 관리 없음

**개선 방식** (ELK Stack):
```python
import logging.config

LOGGING_CONFIG = {
    'version': 1,
    'handlers': {
        'elasticsearch': {
            'class': 'pythonjsonlogger.jsonlogger.JsonFormatter',
            'formatter': 'json',
        }
    },
    'loggers': {
        'spark': {
            'handlers': ['elasticsearch'],
            'level': 'INFO'
        }
    }
}
```

**모니터링 메트릭**:
```
Spark:
- 작업 실행 시간
- Executor 메모리 사용량
- Task 실패율
- 데이터 처리량 (records/sec)

PostgreSQL:
- 연결 수
- 쿼리 응답 시간
- 테이블 크기
- 인덱스 효율

Kafka:
- 메시지 처리량
- Consumer lag
- 토픽 파티션 크기
```

---

## 6. 현재 프로젝트의 강점과 개선 사항

### 6.1 강점 ✅

| 항목 | 상태 | 설명 |
|------|------|------|
| **중앙 집중식 설정** | ✅ | `config/settings.py`로 모든 설정 관리 |
| **환경 변수 분리** | ✅ | `.env` 파일로 자격증명 분리 |
| **다중 환경 지원** | ✅ | 로컬, Docker, Airflow 환경 모두 지원 |
| **Docker 활용** | ✅ | 전체 스택 컨테이너화 |
| **Airflow 통합** | ✅ | 스케줄링 및 의존성 관리 |
| **모니터링** | ✅ | Prometheus + Grafana 스택 구성 |
| **재시도 정책** | ✅ | 자동 재시도 설정 (2회) |

### 6.2 개선 필요 사항 ⚠️

| 항목 | 현재 상태 | 권장 개선 |
|------|---------|---------|
| **연결 풀링** | ❌ 미구현 | psycopg2-pool 또는 SQLAlchemy 도입 |
| **자격증명 관리** | ❌ 평문 저장 | AWS Secrets Manager / Vault |
| **환경별 설정** | ⚠️ 단일 파일 | `config/dev.py`, `config/prod.py` 분리 |
| **메모리 튜닝** | ⚠️ 하드코딩 | 메모리 계산 자동화 스크립트 |
| **JAR 버전** | ⚠️ 중복 | 최신 버전만 유지 |
| **Spark 로깅** | ⚠️ 로컬 파일 | ELK Stack으로 중앙화 |
| **JDBC 최적화** | ⚠️ 기본 설정 | numPartitions, fetchSize 튜닝 |
| **SLA 관리** | ❌ 미흡 | execution_timeout 명시 |
| **알림 설정** | ❌ 미흡 | Slack, PagerDuty 통합 |

---

## 7. 설정 보고 체계

### 7.1 실무에서의 설정 관리 프로세스

#### 1️⃣ 개발 환경 (Developer)
```
로컬 설정 변경
↓
git commit (.env 제외)
↓
코드 리뷰 (config/settings.py)
↓
PR 승인
```

#### 2️⃣ 스테이징 환경 (DevOps/Engineering)
```
Docker Compose 설정 검증
↓
환경 변수 주입 테스트
↓
Spark 클러스터 성능 테스트
↓
배포 자동화 검증
```

#### 3️⃣ 프로덕션 환경 (DevOps)
```
변수 암호화 (AWS KMS, Vault)
↓
보안 감사
↓
Blue-Green 배포 검증
↓
모니터링 알림 설정
↓
운영팀 문서화
```

---

### 7.2 설정 문서화 체크리스트

**각 배포 전에 확인사항**:

```markdown
## 배포 전 설정 검증 체크리스트

### Spark 설정
- [ ] Master URL 확인 (local[*] / spark://...)
- [ ] 메모리 할당 충분한가 (Driver, Executor)
- [ ] CPU 코어 수 적절한가
- [ ] 타임아웃 설정 확인
- [ ] Checkpoint 경로 유효한가

### Database 설정
- [ ] PostgreSQL 호스트/포트 연결 가능한가
- [ ] 자격증명 암호화 되어있는가
- [ ] 연결 풀 크기 적절한가
- [ ] 접근 권한 최소화 (Principle of Least Privilege)

### Kafka 설정
- [ ] Broker 목록 최신인가
- [ ] Topic 파티션 수 적절한가
- [ ] Consumer Group 이름 명확한가
- [ ] Replication Factor 설정 확인

### Airflow 설정
- [ ] DAG 스케줄 시간대 확인
- [ ] 재시도 정책 적절한가
- [ ] 의존성 모든 Task 명시
- [ ] Timeout 시간 설정
- [ ] 실패 알림 활성화

### 모니터링
- [ ] Prometheus targets 등록
- [ ] Grafana 대시보드 설정
- [ ] 알림 규칙 테스트
- [ ] 로그 중앙화 확인
```

---

### 7.3 월간 설정 리뷰 보고서 템플릿

```markdown
# 월간 설정 리뷰 보고서
**기간**: 2025-10-01 ~ 2025-10-31
**검토자**: DevOps Team

## 1. 설정 변경 이력

| 날짜 | 항목 | 변경 내용 | 영향도 | 담당자 |
|------|------|---------|--------|--------|
| 2025-10-05 | Spark Memory | Driver 1G→2G | Medium | John |
| 2025-10-12 | PostgreSQL | 연결 풀 추가 | High | Jane |
| 2025-10-20 | Airflow DAG | 실행 시간 조정 | Low | Bob |

## 2. 성능 메트릭

| 메트릭 | 지난달 | 이번달 | 변화 |
|--------|--------|--------|------|
| 평균 작업 시간 | 45분 | 38분 | ↓ 15% |
| Peak 메모리 | 6.2GB | 5.8GB | ↓ 6% |
| 에러율 | 2.1% | 1.2% | ↓ 43% |
| 평균 응답시간 | 120ms | 95ms | ↓ 21% |

## 3. 보안 감사

- [ ] 자격증명 노출 검사
- [ ] IAM 권한 검토
- [ ] 암호화 상태 확인
- [ ] 감사 로그 검토

## 4. 개선 사항

1. **완료**: 연결 풀링 도입 (10월 12일)
2. **진행 중**: ELK Stack 구축
3. **계획 중**: Kubernetes 마이그레이션

## 5. 다음 달 예정

- Spark 3.6 업그레이드 테스트
- PostgreSQL 14 → 15 마이그레이션
- 재해 복구(DR) 테스트
```

---

### 7.4 설정 관련 문서화 위치

**프로젝트 내 문서화 기준**:

```
docs/
├── SETUP.md              # 초기 설정 가이드
├── CONFIGURATION.md      # 상세 설정 문서
├── DEPLOYMENT.md         # 배포 절차
├── TROUBLESHOOTING.md    # 문제 해결 가이드
└── MONITORING.md         # 모니터링 설정

코드 내 문서화:
├── config/settings.py    # 설정 파일에 주석
├── airflow/dags/*.py     # DAG 파일에 docstring
└── src/spark/jobs/*.py   # Spark 작업에 docstring
```

---

### 7.5 설정 버전 관리

```yaml
# VERSION.yml
version: 1.0.0
updated: 2025-11-21

components:
  spark:
    version: 3.5.0
    config_hash: abc123...
    last_review: 2025-11-20

  postgresql:
    version: 16
    config_hash: def456...
    last_review: 2025-11-15

  kafka:
    version: 7.5.0
    config_hash: ghi789...
    last_review: 2025-11-10

changes:
  - date: 2025-11-20
    component: spark
    change: Driver memory increased to 2G
    reason: OOM issues in production
    reviewed_by: DevOps Team
```

---

## 8. 결론 및 권장사항

### 8.1 즉시 실행 항목 (1주일 내)

1. **`.env` 보안 강화**
   - 자격증명 암호화 또는 AWS Secrets Manager 도입

2. **연결 풀링 구현**
   - `psycopg2-pool` 또는 SQLAlchemy 도입
   - 동시 연결 제한으로 리소스 절감

3. **환경별 설정 파일 분리**
   - `config/prod.py`, `config/dev.py` 생성
   - 각 환경의 메모리/코어 명시

---

### 8.2 단기 실행 항목 (1개월 내)

1. **JAR 파일 정리**
   - 중복 버전 제거
   - VERSIONS.md 작성으로 추적성 확보

2. **모니터링 강화**
   - Prometheus 커스텀 메트릭 추가
   - Grafana 알림 규칙 설정

3. **Spark 성능 튜닝**
   - JDBC fetchSize, numPartitions 최적화
   - AQE 활성화 검증

---

### 8.3 중기 실행 항목 (3개월 내)

1. **ELK Stack 도입**
   - 중앙 집중식 로깅
   - 실시간 로그 분석

2. **Kubernetes 준비**
   - Helm 차트 작성
   - YAML 설정 준비

3. **SLA 수립**
   - 배치 최대 실행 시간 정의
   - 알람 임계값 설정

---

## 📎 부록: 설정 파일 요약

### 파일 1: `.env.example`
```bash
# Kafka
KAFKA_BROKERS=localhost:9092
KAFKA_TOPIC=clickstream

# PostgreSQL
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=ecommerce
POSTGRES_USER=admin
POSTGRES_PASSWORD=CHANGE_ME

# Airflow
AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql://airflow:airflow@localhost:5433/airflow
```

### 파일 2: `config/settings.py`
```python
import os
from dotenv import load_dotenv

load_dotenv()

KAFKA_BROKERS = os.getenv('KAFKA_BROKERS', 'localhost:9092').split(',')
POSTGRES_HOST = os.getenv('POSTGRES_HOST', 'localhost')
# ... 나머지 설정
```

### 파일 3: `docker-compose.yml`
- Kafka 클러스터 (3 Brokers)
- PostgreSQL (ecommerce + airflow)
- Spark Master/Workers
- Airflow
- Prometheus + Grafana

---

## 📊 최종 요약

| 항목 | 현재 | 평가 | 개선 우선순위 |
|------|------|------|-------------|
| Spark 설정 관리 | 다중 환경 지원 | ⭐⭐⭐⭐ | 메모리 자동화 |
| DB 연결 관리 | 기본 수준 | ⭐⭐⭐ | 연결 풀링 필수 |
| 자격증명 관리 | 평문 저장 | ⭐ | 암호화 긴급 |
| 환경 분리 | 단일 파일 | ⭐⭐ | 파일 분리 |
| 모니터링 | Prometheus 구성 | ⭐⭐⭐⭐ | 커스텀 메트릭 |
| 문서화 | 최소 수준 | ⭐⭐ | 상세 문서화 |

---

**작성**: 2025-11-21
**검토 예정**: 2025-12-21
**다음 갱신**: 2026-01-21
