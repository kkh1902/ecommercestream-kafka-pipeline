"""
Slack 테스트 설정 (conftest.py)

Slack 웹훅 URL과 공용 픽스처를 정의합니다.
"""

import pytest
import sys
from pathlib import Path

# 프로젝트 경로 설정
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))


# ===== Slack 웹훅 설정 =====

# 환경 변수에서 읽기
import os

# 통계 DAG Slack 웹훅
SLACK_DAILY_STATS_WEBHOOK = os.getenv("SLACK_DAILY_STATS_WEBHOOK", "")

# ML 피처 DAG Slack 웹훅
SLACK_ML_FEATURES_WEBHOOK = os.getenv("SLACK_ML_FEATURES_WEBHOOK", "")


# ===== Pytest 픽스처 =====

@pytest.fixture
def slack_webhooks():
    """Slack 웹훅 URL 제공"""
    return {
        'daily_stats': SLACK_DAILY_STATS_WEBHOOK,
        'ml_features': SLACK_ML_FEATURES_WEBHOOK
    }


@pytest.fixture
def slack_test_payload():
    """테스트용 Slack 메시지 Payload 샘플"""
    from datetime import datetime

    return {
        'test_connection': {
            "text": "🧪 Slack 연결 테스트 메시지",
            "attachments": [{
                "color": "#0099ff",
                "title": "테스트 메시지",
                "text": "Slack 웹훅이 정상적으로 작동합니다.",
                "footer": "Test Suite",
                "ts": int(datetime.now().timestamp())
            }]
        },
        'daily_stats_success': {
            "attachments": [{
                "color": "#00FF00",
                "title": "📊 일일 통계 DAG - SUCCESS",
                "text": "✅ Task: collect_daily_statistics\n처리 완료: 어제 데이터 (1일)\n\n📊 처리 결과:\n  • 입력 이벤트: 1,250개\n\n📈 생성된 통계:\n  • Daily Statistics: 1개\n  • Daily Event Stats: 5개\n  • Daily Product Stats: 250개",
                "footer": "Analytics Team",
                "fields": [
                    {"title": "DAG", "value": "daily_statistics_batch", "short": True},
                    {"title": "상태", "value": "성공", "short": True},
                    {"title": "시간", "value": datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "short": False}
                ],
                "ts": int(datetime.now().timestamp())
            }]
        },
        'ml_features_success': {
            "attachments": [{
                "color": "#00FF00",
                "title": "🤖 ML 피처 생성 DAG - SUCCESS",
                "text": "✅ Task: collect_ml_features_stats\nML 피처 생성 완료: 어제 데이터 (1일)\n\n🤖 생성 결과:\n  • 사용자별 피처: 2,850개\n  • 피처 종류: 8개 (session, events, items, purchase, addtocart, view, conversion_rate)",
                "footer": "ML Team",
                "fields": [
                    {"title": "DAG", "value": "daily_ml_features_batch", "short": True},
                    {"title": "상태", "value": "성공", "short": True},
                    {"title": "생성된 피처", "value": "8개", "short": True},
                    {"title": "시간", "value": datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "short": False}
                ],
                "ts": int(datetime.now().timestamp())
            }]
        }
    }
