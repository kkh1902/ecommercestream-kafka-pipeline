import os
from dotenv import load_dotenv

load_dotenv()

# Kafka 설정
KAFKA_BROKERS = os.getenv('KAFKA_BROKERS', 'localhost:9092').split(',')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'clickstream')

# File Paths
CSV_FILE_PATH = os.getenv('CSV_FILE_PATH', 'data/raw/events.csv')
BACKUP_DIR = os.getenv('BACKUP_DIR', 'data/raw/backup')

# Producer 설정
PRODUCER_INTERVAL = int(os.getenv('PRODUCER_INTERVAL', 0))  # 0 = 즉시 전송

# Consumer 설정
CONSUMER_GROUP_ID = os.getenv('CONSUMER_GROUP_ID', 'ecommerce_consumer_group')

# PostgreSQL 설정
POSTGRES_HOST = os.getenv('POSTGRES_HOST', 'localhost')
POSTGRES_PORT = int(os.getenv('POSTGRES_PORT', 5432))
POSTGRES_DB = os.getenv('POSTGRES_DB', 'ecommerce')
POSTGRES_USER = os.getenv('POSTGRES_USER', 'admin')
POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD', 'admin123')

# Slack 알림 설정
SLACK_WEBHOOK_URL = os.getenv('SLACK_WEBHOOK_URL', '')
SLACK_ALERT_ENABLED = os.getenv('SLACK_ALERT_ENABLED', 'false').lower() == 'true'

# Email 알림 설정
EMAIL_ENABLED = os.getenv('EMAIL_ENABLED', 'false').lower() == 'true'
EMAIL_SMTP_HOST = os.getenv('EMAIL_SMTP_HOST', 'smtp.gmail.com')
EMAIL_SMTP_PORT = int(os.getenv('EMAIL_SMTP_PORT', 587))
EMAIL_FROM = os.getenv('EMAIL_FROM', '')
EMAIL_PASSWORD = os.getenv('EMAIL_PASSWORD', '')
EMAIL_TO = os.getenv('EMAIL_TO', '')

# 테스트
if __name__ == '__main__':
    print(f"KAFKA_BROKERS: {KAFKA_BROKERS}")
    print(f"KAFKA_TOPIC: {KAFKA_TOPIC}")
    print(f"CSV_FILE_PATH: {CSV_FILE_PATH}")
    print(f"BACKUP_DIR: {BACKUP_DIR}")
    print(f"PRODUCER_INTERVAL: {PRODUCER_INTERVAL}")
    print(f"CONSUMER_GROUP_ID: {CONSUMER_GROUP_ID}")
    print(f"POSTGRES: {POSTGRES_USER}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}")
