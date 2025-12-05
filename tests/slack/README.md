# Slack 알람 기능 테스트

이 디렉토리는 DAG의 Slack 알림 기능을 테스트합니다.

## 📁 구조

```
tests/slack/
├── __init__.py                 # 모듈 초기화
├── conftest.py                 # Pytest 설정 및 픽스처
├── test_notifications.py       # 메인 테스트
└── README.md                   # 이 파일
```

## 📋 테스트 항목

### 1. TestSlackNotifications (웹훅 연결 테스트)
- `test_slack_webhook_connectivity`: 실제 Slack 웹훅 연결 테스트
- `test_daily_statistics_success_notification`: 통계 DAG 성공 알림
- `test_daily_statistics_failure_notification`: 통계 DAG 실패 알림
- `test_ml_features_success_notification`: ML DAG 성공 알림
- `test_ml_features_failure_notification`: ML DAG 실패 알림

### 2. TestSlackMessageFormatting (메시지 포맷 테스트)
- `test_success_message_format`: 성공 메시지 포맷 검증
- `test_failure_message_format`: 실패 메시지 포맷 검증
- `test_message_payload_structure`: Payload 구조 검증

### 3. TestSlackWebhookConfiguration (설정 테스트)
- `test_webhook_url_format`: 웹훅 URL 포맷 검증
- `test_both_webhooks_configured`: 모든 웹훅 설정 확인

## 🚀 실행 방법

### 전체 테스트 실행
```bash
pytest tests/slack/test_notifications.py -v -s
```

### 특정 테스트 클래스만 실행
```bash
# 웹훅 연결 테스트만
pytest tests/slack/test_notifications.py::TestSlackNotifications -v -s

# 메시지 포맷 테스트만
pytest tests/slack/test_notifications.py::TestSlackMessageFormatting -v -s

# 설정 테스트만
pytest tests/slack/test_notifications.py::TestSlackWebhookConfiguration -v -s
```

### 특정 테스트만 실행
```bash
pytest tests/slack/test_notifications.py::TestSlackNotifications::test_daily_statistics_success_notification -v -s
```

### Python에서 직접 실행
```bash
python tests/slack/test_notifications.py
```

## 📊 기대 결과

### ✅ 성공 알림 예시

**일일 통계 DAG:**
```
✅ Task: collect_daily_statistics
처리 완료: 어제 데이터 (1일)

📊 처리 결과:
  • 입력 이벤트: 1,250개

📈 생성된 통계:
  • Daily Statistics: 1개
  • Daily Event Stats: 5개
  • Daily Product Stats: 250개
```

**ML 피처 생성 DAG:**
```
✅ Task: collect_ml_features_stats
ML 피처 생성 완료: 어제 데이터 (1일)

🤖 생성 결과:
  • 사용자별 피처: 2,850개
  • 피처 종류: 8개
```

## 🔧 설정

### Slack 웹훅 URL

Slack 웹훅 URL은 `conftest.py`에 정의되어 있습니다:

```python
SLACK_DAILY_STATS_WEBHOOK = "SLACK_WEBHOOK_REMOVED"
SLACK_ML_FEATURES_WEBHOOK = "SLACK_WEBHOOK_REMOVED"
```

### pytest 픽스처

`conftest.py`에서 제공하는 픽스처:

#### `slack_webhooks`
모든 Slack 웹훅 URL을 딕셔너리로 제공:
```python
@pytest.fixture
def slack_webhooks():
    return {
        'daily_stats': SLACK_DAILY_STATS_WEBHOOK,
        'ml_features': SLACK_ML_FEATURES_WEBHOOK
    }
```

#### `slack_test_payload`
테스트용 메시지 Payload 샘플 제공:
```python
@pytest.fixture
def slack_test_payload():
    return {
        'test_connection': {...},
        'daily_stats_success': {...},
        'ml_features_success': {...}
    }
```

## ⚠️ 주의사항

1. **실제 Slack 메시지 전송**
   - 이 테스트는 실제 Slack 웹훅으로 메시지를 전송합니다
   - Slack 채널에 테스트 메시지가 나타날 것입니다

2. **네트워크 연결 필요**
   - Slack 서버에 연결 가능해야 합니다
   - 방화벽/프록시 설정을 확인하세요

3. **웹훅 유효성**
   - 웹훅 URL이 유효해야 합니다
   - 웹훅이 만료되었을 수 있습니다

## 📝 테스트 결과 해석

### 성공 (PASSED)
```
test_slack_webhook_connectivity PASSED
  ✓ Slack 웹훅 연결 성공
  ✓ 응답 코드: 200
```

### 실패 (FAILED)
```
test_slack_webhook_connectivity FAILED
  Slack 웹훅 연결 실패: [Errno -2] Name or service not known
```

**원인:**
- 네트워크 연결 오류
- 웹훅 URL이 잘못되거나 만료됨
- Slack 서버 다운

## 🔗 관련 파일

- [daily_statistics_batch.py](../../airflow/dags/daily_statistics_batch.py) - 통계 DAG
- [daily_ml_features_batch.py](../../airflow/dags/daily_ml_features_batch.py) - ML DAG
- [docker-compose.yml](../../docker/docker-compose.yml) - Slack 웹훅 설정

## 📚 참고

- [Slack Incoming Webhooks](https://api.slack.com/messaging/webhooks)
- [Pytest Documentation](https://docs.pytest.org/)
