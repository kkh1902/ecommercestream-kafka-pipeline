import os
import sys
import json
from datetime import datetime
from typing import Optional, Dict, Any
import requests

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from config.settings import (
    SLACK_WEBHOOK_URL,
    SLACK_ALERT_ENABLED
)


class SlackNotifier:
    def __init__(self, webhook_url: Optional[str] = None, enabled: bool = True):
        self.webhook_url = webhook_url or SLACK_WEBHOOK_URL
        self.enabled = enabled and SLACK_ALERT_ENABLED

    def send(self, message: str, level: str = "INFO", context: Optional[Dict[str, Any]] = None) -> bool:
        if not self.enabled:
            print(f"[Slack Disabled] {level}: {message}")
            return False

        if not self.webhook_url:
            print("[Slack] Webhook URL이 설정되지 않았습니다.")
            return False

        try:
            level_config = {
                "INFO": {"emoji": "ℹ️", "color": "#36a64f"},
                "WARNING": {"emoji": "⚠️", "color": "#ff9900"},
                "ERROR": {"emoji": "❌", "color": "#ff0000"},
                "CRITICAL": {"emoji": "🚨", "color": "#cc0000"}
            }

            config = level_config.get(level.upper(), level_config["INFO"])

            payload = {
                "attachments": [
                    {
                        "color": config["color"],
                        "title": f"{config['emoji']} [{level.upper()}] E-Commerce Kafka Alert",
                        "text": message,
                        "fields": [],
                        "footer": "E-Commerce Kafka Pipeline",
                        "ts": int(datetime.now().timestamp())
                    }
                ]
            }

            if context:
                for key, value in context.items():
                    payload["attachments"][0]["fields"].append({
                        "title": key,
                        "value": str(value),
                        "short": True
                    })

            response = requests.post(
                self.webhook_url,
                json=payload,
                headers={"Content-Type": "application/json"},
                timeout=10
            )

            if response.status_code == 200:
                print(f"[Slack] 알림 전송 성공: {level}")
                return True
            else:
                print(f"[Slack] 알림 전송 실패: {response.status_code} - {response.text}")
                return False

        except Exception as e:
            print(f"[Slack] 알림 전송 중 에러: {e}")
            return False

    def send_info(self, message: str, context: Optional[Dict[str, Any]] = None) -> bool:
        return self.send(message, "INFO", context)

    def send_warning(self, message: str, context: Optional[Dict[str, Any]] = None) -> bool:
        return self.send(message, "WARNING", context)

    def send_error(self, message: str, context: Optional[Dict[str, Any]] = None) -> bool:
        return self.send(message, "ERROR", context)

    def send_critical(self, message: str, context: Optional[Dict[str, Any]] = None) -> bool:
        return self.send(message, "CRITICAL", context)


class Notifier:
    def __init__(self):
        self.slack = SlackNotifier()

    def notify(
        self,
        message: str,
        level: str = "INFO",
        context: Optional[Dict[str, Any]] = None
    ) -> bool:
        return self.slack.send(message, level, context)

    def notify_info(self, message: str, context: Optional[Dict[str, Any]] = None) -> bool:
        return self.notify(message, "INFO", context)

    def notify_warning(self, message: str, context: Optional[Dict[str, Any]] = None) -> bool:
        return self.notify(message, "WARNING", context)

    def notify_error(self, message: str, context: Optional[Dict[str, Any]] = None) -> bool:
        return self.notify(message, "ERROR", context)

    def notify_critical(self, message: str, context: Optional[Dict[str, Any]] = None) -> bool:
        return self.notify(message, "CRITICAL", context)


_notifier = None

def get_notifier() -> Notifier:
    global _notifier
    if _notifier is None:
        _notifier = Notifier()
    return _notifier


def send_alert(message: str, level: str = "INFO", context: Optional[Dict[str, Any]] = None) -> bool:
    return get_notifier().slack.send(message, level, context)
