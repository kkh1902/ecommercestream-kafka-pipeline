"""
에러 알림 모듈 (Slack, Email 지원)
error_logs 데이터를 바탕으로 실시간 알림 전송

사용 예:
    notifier = ErrorNotifier(webhook_url="https://hooks.slack.com/...")
    notifier.send_critical_alert(error_count=5, error_types=['DatabaseError', 'KafkaError'])
"""

import json
import logging
from typing import Dict, List, Optional
from datetime import datetime
import requests

logger = logging.getLogger(__name__)


class ErrorNotifier:
    """에러 알림 발송"""

    def __init__(self, slack_webhook_url: Optional[str] = None,
                 email_config: Optional[Dict] = None):
        """
        알림 발송자 초기화

        Args:
            slack_webhook_url: Slack Webhook URL
            email_config: Email 설정
                {
                    'smtp_host': 'smtp.gmail.com',
                    'smtp_port': 587,
                    'from': 'sender@gmail.com',
                    'password': 'app_password',
                    'to': ['recipient@gmail.com']
                }
        """
        self.slack_webhook_url = slack_webhook_url
        self.email_config = email_config

    def send_critical_alert(self, critical_count: int,
                           error_types: List[str],
                           additional_info: Optional[Dict] = None,
                           channel: str = '#kafka-error') -> bool:
        """
        Critical 에러 알림 발송 (🔴 빨강)

        Args:
            critical_count: Critical 에러 개수
            error_types: 에러 타입 리스트
            additional_info: 추가 정보
            channel: Slack 채널명 (기본값: #kafka-error)

        Returns:
            bool: 성공 여부
        """
        message = self._build_critical_message(
            critical_count=critical_count,
            error_types=error_types,
            additional_info=additional_info
        )

        success = True

        if self.slack_webhook_url:
            success &= self._send_slack(message, color='danger', channel=channel)

        if self.email_config:
            success &= self._send_email(
                subject='🔴 CRITICAL ERROR ALERT',
                body=message
            )

        return success

    def send_warning_alert(self, pending_count: int,
                          error_types: Dict[str, int],
                          additional_info: Optional[Dict] = None,
                          channel: str = '#kafka-error') -> bool:
        """
        경고 알림 발송 (🟡 노랑)

        Args:
            pending_count: Pending_retry 에러 개수
            error_types: 에러 타입별 개수 {'DatabaseError': 10, ...}
            additional_info: 추가 정보
            channel: Slack 채널명 (기본값: #kafka-error)

        Returns:
            bool: 성공 여부
        """
        message = self._build_warning_message(
            pending_count=pending_count,
            error_types=error_types,
            additional_info=additional_info
        )

        success = True

        if self.slack_webhook_url:
            success &= self._send_slack(message, color='warning', channel=channel)

        if self.email_config:
            success &= self._send_email(
                subject='🟡 WARNING: High Error Rate',
                body=message
            )

        return success

    def send_success_alert(self, total_errors: int,
                          stats: Dict,
                          channel: str = '#kafka-error') -> bool:
        """
        정상 완료 알림 발송 (🟢 초록)

        Args:
            total_errors: 총 에러 개수
            stats: 통계 정보
            channel: Slack 채널명 (기본값: #kafka-error)

        Returns:
            bool: 성공 여부
        """
        message = self._build_success_message(
            total_errors=total_errors,
            stats=stats
        )

        success = True

        if self.slack_webhook_url:
            success &= self._send_slack(message, color='good', channel=channel)

        return success

    def _build_critical_message(self, critical_count: int,
                               error_types: List[str],
                               additional_info: Optional[Dict] = None) -> str:
        """Critical 에러 메시지 작성"""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        message = f"""
🚨 CRITICAL ERROR ALERT 🚨

⏰ 발생 시간: {timestamp}
🔴 Critical 에러: {critical_count}개
📋 에러 타입: {', '.join(error_types)}

⚠️  긴급 조치가 필요합니다!

시스템 상태:
- Consumer 프로세스 중단 가능성
- 데이터 손실 위험
- 서비스 장애 가능성

권장 조치:
1. 즉시 로그 확인
2. 원인 파악 및 해결
3. 시스템 재시작 검토
"""

        if additional_info:
            message += f"\n추가 정보:\n"
            for key, value in additional_info.items():
                message += f"  {key}: {value}\n"

        return message

    def _build_warning_message(self, pending_count: int,
                              error_types: Dict[str, int],
                              additional_info: Optional[Dict] = None) -> str:
        """경고 메시지 작성"""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        message = f"""
⚠️  WARNING: High Error Rate ⚠️

⏰ 발생 시간: {timestamp}
🟡 재시도 대기 에러: {pending_count}개

📊 에러 타입별 분포:
"""

        for error_type, count in sorted(error_types.items(), key=lambda x: x[1], reverse=True):
            message += f"  - {error_type}: {count}개\n"

        message += """
상황:
- 데이터베이스 연결 지연
- Kafka 브로커 부하
- 네트워크 지연 등의 일시적 이슈

권장 조치:
1. 시스템 상태 모니터링
2. 리소스 사용률 확인
3. 네트워크 연결 상태 점검
"""

        if additional_info:
            message += f"\n추가 정보:\n"
            for key, value in additional_info.items():
                message += f"  {key}: {value}\n"

        return message

    def _build_success_message(self, total_errors: int,
                              stats: Dict) -> str:
        """정상 완료 메시지 작성"""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        message = f"""
✅ 에러 수집 완료

⏰ 수집 시간: {timestamp}
📊 처리된 에러: {total_errors}개

상세 통계:
"""

        if 'by_error_type' in stats:
            message += "\n에러 타입별:\n"
            for error_type, count in sorted(
                    stats['by_error_type'].items(),
                    key=lambda x: x[1],
                    reverse=True
            ):
                message += f"  - {error_type}: {count}개\n"

        if 'by_module' in stats:
            message += "\n모듈별:\n"
            for module, count in sorted(
                    stats['by_module'].items(),
                    key=lambda x: x[1],
                    reverse=True
            ):
                message += f"  - {module}: {count}개\n"

        message += f"\n상태: ✅ 정상 처리"

        return message

    def _send_slack(self, message: str, color: str = 'good', channel: str = '#kafka-error') -> bool:
        """
        Slack으로 메시지 발송

        Args:
            message: 발송할 메시지
            color: 색상 ('good'=초록, 'warning'=노랑, 'danger'=빨강)
            channel: Slack 채널명 (예: #kafka-error, #data_quality_check)

        Returns:
            bool: 성공 여부
        """
        if not self.slack_webhook_url:
            logger.warning("⚠️  Slack Webhook URL이 설정되지 않았습니다")
            return False

        try:
            payload = {
                "channel": channel,
                "attachments": [
                    {
                        "color": color,
                        "text": message,
                        "footer": "Error Data Collection",
                        "ts": int(datetime.now().timestamp())
                    }
                ]
            }

            response = requests.post(
                self.slack_webhook_url,
                json=payload,
                timeout=10
            )

            if response.status_code == 200:
                logger.info(f"✅ Slack 메시지 발송 성공 ({channel})")
                return True
            else:
                logger.error(f"❌ Slack 발송 실패 ({channel}, 상태코드: {response.status_code})")
                return False

        except requests.exceptions.RequestException as e:
            logger.error(f"❌ Slack 발송 에러 ({channel}): {e}")
            return False

    def _send_email(self, subject: str, body: str) -> bool:
        """
        Email로 메시지 발송

        Args:
            subject: 제목
            body: 본문

        Returns:
            bool: 성공 여부
        """
        if not self.email_config:
            logger.warning("⚠️  Email 설정이 없습니다")
            return False

        try:
            import smtplib
            from email.mime.text import MIMEText
            from email.mime.multipart import MIMEMultipart

            # Email 설정
            smtp_host = self.email_config.get('smtp_host', 'smtp.gmail.com')
            smtp_port = self.email_config.get('smtp_port', 587)
            from_addr = self.email_config.get('from')
            password = self.email_config.get('password')
            to_addrs = self.email_config.get('to', [])

            if not (from_addr and password and to_addrs):
                logger.warning("⚠️  Email 설정이 불완전합니다")
                return False

            # 메시지 작성
            msg = MIMEMultipart()
            msg['From'] = from_addr
            msg['To'] = ', '.join(to_addrs)
            msg['Subject'] = subject

            msg.attach(MIMEText(body, 'plain'))

            # 발송
            with smtplib.SMTP(smtp_host, smtp_port) as server:
                server.starttls()
                server.login(from_addr, password)
                server.sendmail(from_addr, to_addrs, msg.as_string())

            logger.info(f"✅ Email 발송 성공 ({', '.join(to_addrs)})")
            return True

        except Exception as e:
            logger.error(f"❌ Email 발송 실패: {e}")
            return False


# ============================================================
# 테스트 코드
# ============================================================

if __name__ == '__main__':
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    print("\n" + "="*60)
    print("Error Notifier 테스트")
    print("="*60 + "\n")

    # Notifier 초기화 (Slack 없음 - 테스트용)
    notifier = ErrorNotifier(slack_webhook_url=None)

    print("1️⃣ Critical 알림 테스트\n")
    notifier.send_critical_alert(
        critical_count=3,
        error_types=['DatabaseError', 'KafkaError', 'TimeoutError']
    )

    print("\n2️⃣ Warning 알림 테스트\n")
    notifier.send_warning_alert(
        pending_count=50,
        error_types={'DatabaseError': 30, 'KafkaError': 20}
    )

    print("\n3️⃣ Success 알림 테스트\n")
    notifier.send_success_alert(
        total_errors=100,
        stats={
            'critical_count': 5,
            'pending_count': 95,
            'by_error_type': {
                'DatabaseError': 60,
                'KafkaError': 40
            },
            'by_module': {
                'consumer_postgres': 90,
                'spark_job': 10
            }
        }
    )

    print("\n" + "="*60)
    print("테스트 완료")
    print("="*60 + "\n")
