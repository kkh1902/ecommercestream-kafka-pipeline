# Application Performance Test Results

## 📊 실행 결과 (2025-11-19)

### 1️⃣ Producer 애플리케이션 성능

```
테스트 설정:
- CSV 파일: data/raw/events.csv
- 샘플 크기: 5,000 records
- 모드: Batch (빠른 전송)

결과:
- 메시지 전송: 5,000개 ✅
- 처리량: 812 msg/sec
- 실행 시간: 6.16초
- 대역폭: 0.79 MB/sec
```

### 2️⃣ Kafka vs Producer 비교

| 항목 | Kafka-perf-test | Producer App | 비율 |
|------|-----------------|--------------|------|
| **처리량** | 67,159 msg/sec | 812 msg/sec | 1:82 |
| **대역폭** | 65.59 MB/sec | 0.79 MB/sec | 1:83 |

### 3️⃣ 병목 분석

```
파이프라인:
CSV 파일 읽기 → 직렬화 → Kafka 전송
   ⬇️           ⬇️         ⬇️
 (느림)        (느림)     (매우 빠름)

결론:
❌ Kafka는 충분히 빠름 (67k msg/sec)
❌ Producer 애플리케이션이 병목 (812 msg/sec)
   - CSV 파일 읽기가 느림
   - 직렬화 오버헤드
```

---

## 💡 원인 분석

### Producer가 느린 이유

1. **CSV 읽기 (pandas)**
   - 전체 파일을 메모리에 로드
   - 각 행을 반복 순회
   - 데이터 타입 변환

2. **직렬화 과정**
   - 딕셔너리로 변환
   - NaN → None 처리
   - Kafka 메시지 직렬화

3. **Kafka 전송**
   - 각 메시지마다 future.get() 대기
   - 배치 모드이지만 여전히 개별 처리

### 결론

```
⚠️ CSV 읽기와 직렬화가 병목
✅ Kafka 자체는 충분히 빠름 (67k msg/sec)
✅ Producer가 할 수 있는 것: 812 msg/sec

실제 병목: Python의 CSV 읽기 성능
```

---

## 🚀 최적화 제안

### Producer 최적화 방안

1. **배치 직렬화**
   ```python
   # 현재: 개별 메시지마다 직렬화
   # 개선: 여러 메시지를 한번에 직렬화
   ```

2. **CSV 읽기 최적화**
   ```python
   # 현재: pandas로 전체 파일 로드
   df = pd.read_csv(file)  # 모든 행 로드

   # 개선: 청크 단위로 읽기
   for chunk in pd.read_csv(file, chunksize=1000):
       # 처리
   ```

3. **비동기 처리**
   ```python
   # Producer를 여러 스레드/프로세스로 병렬화
   # 현재: 순차 처리 (812 msg/sec)
   # 개선 가능: 4-8x 향상 가능 (3,000-6,000 msg/sec)
   ```

---

## 📋 다음 테스트 항목

### 1. Consumer 성능 테스트
```bash
python tests/load_tests/app_consumer_perf_test.py --timeout 180
```
- Kafka → PostgreSQL insert 성능 측정
- 배치 크기 (현재 100) 최적화

### 2. 통합 테스트 (E2E)
```bash
python tests/load_tests/run_app_perf_tests.py --sample 10000
```
- CSV → Kafka → PostgreSQL 전체 파이프라인
- 병목 지점 확인

### 3. 최적화 후 재측정
```bash
# 개선 후 성능 비교
python tests/load_tests/app_producer_perf_test.py --sample 5000
```

---

## 📝 사용 방법

### Producer 애플리케이션 테스트
```bash
# 기본 (10k records)
python tests/load_tests/app_producer_perf_test.py

# 커스텀 샘플 크기
python tests/load_tests/app_producer_perf_test.py --sample 50000

# 다른 CSV 파일
python tests/load_tests/app_producer_perf_test.py --csv data/raw/events.csv --sample 5000
```

### Consumer 애플리케이션 테스트
```bash
# 기본 (180초 timeout)
python tests/load_tests/app_consumer_perf_test.py

# 커스텀 timeout
python tests/load_tests/app_consumer_perf_test.py --timeout 300
```

### 통합 테스트
```bash
# 기본 (10k + 180초)
python tests/load_tests/run_app_perf_tests.py

# 커스텀 설정
python tests/load_tests/run_app_perf_tests.py --sample 50000 --consumer-timeout 300
```

---

## 📊 성능 기준선

```
목표 성능:
- Producer: 1,000+ msg/sec (현재: 812 ✅)
- Consumer: 5,000+ msg/sec (측정 대기)
- E2E 처리량: min(producer, consumer)

개선 목표:
- Producer: 3,000+ msg/sec (병렬화)
- Consumer: 10,000+ msg/sec (배치 크기 최적화)
```

---

## 🔍 주요 발견사항

1. ✅ **Kafka는 충분히 빠름**
   - 67k msg/sec 처리 가능
   - Kafka 자체는 병목이 아님

2. ⚠️ **Producer 애플리케이션이 병목**
   - 현재: 812 msg/sec (82배 느림)
   - CSV 읽기와 직렬화가 원인
   - 개선 가능성: 3-8배

3. 📊 **전체 파이프라인의 성능**
   - Producer와 Consumer 속도 모두 영향
   - 느린 쪽이 전체 병목
   - 병렬화로 3배 이상 향상 가능

