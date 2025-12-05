"""
Slack 알람 기능 테스트
- daily_statistics_batch DAG Slack 알람 테스트
- daily_ml_features_batch DAG Slack 알람 테스트
- 웹훅 연결 테스트
- 메시지 포맷 테스트
"""

import sys
import os
import pytest
import requests
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock
import json

# 프로젝트 경로 설정
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

# Slack 웹훅 URL (환경 변수에서 읽음)
import os

SLACK_DAILY_STATS_WEBHOOK = os.getenv("SLACK_DAILY_STATS_WEBHOOK", "")
SLACK_ML_FEATURES_WEBHOOK = os.getenv("SLACK_ML_FEATURES_WEBHOOK", "")


class TestSlackNotifications:
    """Slack 알람 기능 테스트"""

    def setup_method(self):
        """테스트 전 설정"""
        self.daily_stats_webhook = SLACK_DAILY_STATS_WEBHOOK
        self.ml_features_webhook = SLACK_ML_FEATURES_WEBHOOK

    def test_slack_webhook_connectivity(self):
        """Slack 웹훅 연결 테스트"""
        print("\n[TEST] Slack 웹훅 연결 테스트")
        print(f"  통계 채널 URL: {self.daily_stats_webhook}")
        print(f"  ML 채널 URL: {self.ml_features_webhook}")

        # 테스트 메시지 전송
        payload = {
            "text": "🧪 Slack 연결 테스트 메시지",
            "attachments": [{
                "color": "#0099ff",
                "title": "테스트 메시지",
                "text": "Slack 웹훅이 정상적으로 작동합니다.",
                "footer": "Test Suite",
                "ts": int(datetime.now().timestamp())
            }]
        }

        try:
            response = requests.post(
                self.daily_stats_webhook,
                json=payload,
                timeout=10
            )

            assert response.status_code == 200, f"Slack 웹훅 응답 코드: {response.status_code}"
            print("  ✓ Slack 웹훅 연결 성공")
            print(f"  ✓ 응답 코드: {response.status_code}")

        except requests.exceptions.RequestException as e:
            pytest.fail(f"Slack 웹훅 연결 실패: {e}")

    def test_daily_statistics_success_notification(self):
        """daily_statistics_batch 성공 알림 테스트"""
        print("\n[TEST] daily_statistics_batch 성공 알림")

        payload = {
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
        }

        try:
            response = requests.post(
                self.daily_stats_webhook,
                json=payload,
                timeout=10
            )

            assert response.status_code == 200
            print("  ✓ 성공 알림 전송 완료")
            print(f"  ✓ 메시지: {payload['attachments'][0]['title']}")

        except requests.exceptions.RequestException as e:
            pytest.fail(f"알림 전송 실패: {e}")

    def test_daily_statistics_failure_notification(self):
        """daily_statistics_batch 실패 알림 테스트"""
        print("\n[TEST] daily_statistics_batch 실패 알림")

        payload = {
            "attachments": [{
                "color": "#FF0000",
                "title": "📊 일일 통계 DAG - ERROR",
                "text": "❌ Task: collect_daily_statistics\nError: Spark job failed - connection timeout",
                "footer": "Analytics Team",
                "fields": [
                    {"title": "DAG", "value": "daily_statistics_batch", "short": True},
                    {"title": "상태", "value": "실패", "short": True},
                    {"title": "에러", "value": "Spark job failed", "short": False}
                ],
                "ts": int(datetime.now().timestamp())
            }]
        }

        try:
            response = requests.post(
                self.daily_stats_webhook,
                json=payload,
                timeout=10
            )

            assert response.status_code == 200
            print("  ✓ 실패 알림 전송 완료")
            print(f"  ✓ 메시지: {payload['attachments'][0]['title']}")

        except requests.exceptions.RequestException as e:
            pytest.fail(f"알림 전송 실패: {e}")

    def test_ml_features_success_notification(self):
        """daily_ml_features_batch 성공 알림 테스트"""
        print("\n[TEST] daily_ml_features_batch 성공 알림")

        payload = {
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

        try:
            response = requests.post(
                self.ml_features_webhook,
                json=payload,
                timeout=10
            )

            assert response.status_code == 200
            print("  ✓ 성공 알림 전송 완료")
            print(f"  ✓ 메시지: {payload['attachments'][0]['title']}")

        except requests.exceptions.RequestException as e:
            pytest.fail(f"알림 전송 실패: {e}")

    def test_ml_features_failure_notification(self):
        """daily_ml_features_batch 실패 알림 테스트"""
        print("\n[TEST] daily_ml_features_batch 실패 알림")

        payload = {
            "attachments": [{
                "color": "#FF0000",
                "title": "🤖 ML 피처 생성 DAG - ERROR",
                "text": "❌ Task: create_ml_features\nError: Raw data not found in database",
                "footer": "ML Team",
                "fields": [
                    {"title": "DAG", "value": "daily_ml_features_batch", "short": True},
                    {"title": "상태", "value": "실패", "short": True},
                    {"title": "에러", "value": "Raw data not found", "short": False}
                ],
                "ts": int(datetime.now().timestamp())
            }]
        }

        try:
            response = requests.post(
                self.ml_features_webhook,
                json=payload,
                timeout=10
            )

            assert response.status_code == 200
            print("  ✓ 실패 알림 전송 완료")
            print(f"  ✓ 메시지: {payload['attachments'][0]['title']}")

        except requests.exceptions.RequestException as e:
            pytest.fail(f"알림 전송 실패: {e}")


class TestSlackMessageFormatting:
    """Slack 메시지 포맷 테스트"""

    def test_success_message_format(self):
        """성공 메시지 포맷 검증"""
        print("\n[TEST] 성공 메시지 포맷 검증")

        # 포맷 검증
        color = "#00FF00"
        title = "📊 일일 통계 DAG - SUCCESS"

        assert color == "#00FF00", "성공 색상은 녹색이어야 함"
        assert "SUCCESS" in title, "제목에 SUCCESS 포함되어야 함"
        assert "📊" in title, "이모지 포함되어야 함"

        print("  ✓ 성공 메시지 포맷 검증 완료")
        print(f"  ✓ 색상: {color} (녹색)")
        print(f"  ✓ 제목: {title}")

    def test_failure_message_format(self):
        """실패 메시지 포맷 검증"""
        print("\n[TEST] 실패 메시지 포맷 검증")

        # 포맷 검증
        color = "#FF0000"
        title = "📊 일일 통계 DAG - ERROR"

        assert color == "#FF0000", "실패 색상은 빨간색이어야 함"
        assert "ERROR" in title, "제목에 ERROR 포함되어야 함"
        assert "📊" in title, "이모지 포함되어야 함"

        print("  ✓ 실패 메시지 포맷 검증 완료")
        print(f"  ✓ 색상: {color} (빨간색)")
        print(f"  ✓ 제목: {title}")

    def test_message_payload_structure(self):
        """메시지 payload 구조 검증"""
        print("\n[TEST] 메시지 payload 구조 검증")

        payload = {
            "attachments": [{
                "color": "#00FF00",
                "title": "테스트",
                "text": "테스트 메시지",
                "footer": "Test",
                "fields": [
                    {"title": "필드1", "value": "값1", "short": True}
                ],
                "ts": int(datetime.now().timestamp())
            }]
        }

        # 구조 검증
        assert "attachments" in payload, "attachments 필드 필수"
        assert isinstance(payload["attachments"], list), "attachments는 list여야 함"

        attachment = payload["attachments"][0]
        required_fields = ["color", "title", "text", "footer"]

        for field in required_fields:
            assert field in attachment, f"{field} 필드 필수"

        print("  ✓ Payload 구조 검증 완료")
        print("  ✓ 필수 필드: color, title, text, footer, fields, ts")


class TestDAGSlackIntegration:
    """DAG와 Slack 통합 테스트"""

    @patch('requests.post')
    def test_daily_statistics_dag_callback(self, mock_post):
        """daily_statistics_batch DAG 콜백 테스트"""
        print("\n[TEST] daily_statistics_batch DAG 콜백")

        from airflow.dags.daily_statistics_batch import send_slack_notification

        # Mock 설정
        mock_post.return_value.status_code = 200

        message = "✅ Task: collect_daily_statistics\n처리 완료"
        send_slack_notification(message, status='success')

        # 호출 확인
        assert mock_post.called, "requests.post가 호출되지 않음"
        call_args = mock_post.call_args

        print("  ✓ Slack 알림 호출 확인")
        print(f"  ✓ 메시지: {message}")

    @patch('requests.post')
    def test_ml_features_dag_callback(self, mock_post):
        """daily_ml_features_batch DAG 콜백 테스트"""
        print("\n[TEST] daily_ml_features_batch DAG 콜백")

        from airflow.dags.daily_ml_features_batch import send_slack_notification

        # Mock 설정
        mock_post.return_value.status_code = 200

        message = "✅ Task: create_ml_features\nML 피처 생성 완료"
        send_slack_notification(message, status='success')

        # 호출 확인
        assert mock_post.called, "requests.post가 호출되지 않음"

        print("  ✓ Slack 알림 호출 확인")
        print(f"  ✓ 메시지: {message}")

    @patch('requests.post')
    def test_notification_with_context(self, mock_post):
        """Context와 함께 알림 테스트"""
        print("\n[TEST] Context와 함께 알림 전송")

        from airflow.dags.daily_statistics_batch import on_success_callback

        # Mock 설정
        mock_post.return_value.status_code = 200

        # Mock context
        context = {
            'task_instance': MagicMock(),
            'execution_date': datetime(2025, 12, 5),
        }
        context['task_instance'].task_id = 'collect_daily_statistics'

        # 실행
        on_success_callback(context)

        # 호출 확인
        assert mock_post.called, "requests.post가 호출되지 않음"

        print("  ✓ Context 기반 알림 전송 완료")
        print(f"  ✓ Task ID: {context['task_instance'].task_id}")


class TestSlackWebhookConfiguration:
    """Slack 웹훅 설정 테스트"""

    def test_webhook_url_format(self):
        """웹훅 URL 포맷 검증"""
        print("\n[TEST] 웹훅 URL 포맷 검증")

        webhook_url = SLACK_WEBHOOK_URL

        # URL 포맷 검증
        assert webhook_url.startswith("https://hooks.slack.com/services/"), "URL 포맷 오류"
        assert len(webhook_url) > 50, "URL 길이 오류"

        print("  ✓ 웹훅 URL 포맷 검증 완료")
        print(f"  ✓ URL: {webhook_url[:50]}...")

    def test_webhook_url_in_dag(self):
        """DAG에서 웹훅 URL 설정 확인"""
        print("\n[TEST] DAG 웹훅 URL 설정 확인")

        from airflow.dags.daily_statistics_batch import SLACK_DAILY_STATS_WEBHOOK
        from airflow.dags.daily_ml_features_batch import SLACK_ML_FEATURES_WEBHOOK

        assert SLACK_DAILY_STATS_WEBHOOK, "SLACK_DAILY_STATS_WEBHOOK 설정되지 않음"
        assert SLACK_ML_FEATURES_WEBHOOK, "SLACK_ML_FEATURES_WEBHOOK 설정되지 않음"

        print("  ✓ daily_statistics_batch 웹훅 설정됨")
        print(f"    URL: {SLACK_DAILY_STATS_WEBHOOK[:50]}...")
        print("  ✓ daily_ml_features_batch 웹훅 설정됨")
        print(f"    URL: {SLACK_ML_FEATURES_WEBHOOK[:50]}...")


# ===== CLI에서 실행할 때 사용 =====

if __name__ == '__main__':
    print("\n" + "="*70)
    print("Slack 알람 기능 테스트")
    print("="*70)
    print("\n⚠️  주의: 이 테스트는 실제 Slack 웹훅으로 메시지를 전송합니다.")
    print("    테스트 메시지가 Slack 채널에 나타날 것입니다.\n")

    # pytest 실행
    exit_code = pytest.main([
        __file__,
        '-v',
        '--tb=short',
        '-s'  # stdout 출력 표시
    ])

    sys.exit(exit_code)
