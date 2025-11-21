"""
Kafka error_data Topic Consumer
error_data topic에서 에러 메시지를 읽어 PostgreSQL error_logs 테이블에 저장

사용 예:
    consumer = ErrorDataConsumer()
    errors = consumer.consume_errors(timeout_ms=10000)  # 최대 10초 대기
    consumer.store_errors_to_db(errors)
"""

import json
import logging
from datetime import datetime
from typing import List, Dict, Optional

from kafka import KafkaConsumer
from kafka.errors import KafkaError
import psycopg2
from psycopg2 import sql

logger = logging.getLogger(__name__)


class ErrorDataConsumer:
    """error_data topic 소비 및 저장"""

    def __init__(self, bootstrap_servers: str = 'localhost:9092',
                 group_id: str = 'error_consumer_group',
                 postgres_config: Optional[Dict] = None):
        """
        에러 컨슈머 초기화

        Args:
            bootstrap_servers: Kafka 브로커 주소 (comma separated)
            group_id: Consumer Group ID
            postgres_config: PostgreSQL 연결 정보
                {
                    'host': 'localhost',
                    'port': 5432,
                    'database': 'ecommerce',
                    'user': 'admin',
                    'password': 'admin123'
                }
        """
        self.bootstrap_servers = bootstrap_servers
        self.group_id = group_id
        self.postgres_config = postgres_config or {
            'host': 'localhost',
            'port': 5432,
            'database': 'ecommerce',
            'user': 'admin',
            'password': 'admin123'
        }

        self.consumer = None
        self.db_conn = None
        self._init_kafka_consumer()

    def _init_kafka_consumer(self):
        """Kafka Consumer 초기화"""
        try:
            self.consumer = KafkaConsumer(
                'error_data',
                bootstrap_servers=self.bootstrap_servers.split(','),
                group_id=self.group_id,
                auto_offset_reset='earliest',  # 새로운 consumer: 처음부터 읽기
                enable_auto_commit=False,  # 수동으로 offset 관리
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                consumer_timeout_ms=10000  # 10초 타임아웃
            )
            logger.info(f"✅ Kafka Consumer 초기화 완료 (group: {self.group_id})")
        except KafkaError as e:
            logger.error(f"❌ Kafka Consumer 초기화 실패: {e}")
            raise

    def _init_db_connection(self):
        """PostgreSQL 연결 초기화"""
        try:
            self.db_conn = psycopg2.connect(**self.postgres_config)
            logger.info(
                f"✅ PostgreSQL 연결 완료 ({self.postgres_config['host']}:{self.postgres_config['port']})"
            )
        except psycopg2.Error as e:
            logger.error(f"❌ PostgreSQL 연결 실패: {e}")
            raise

    def consume_errors(self, max_records: int = 100,
                      timeout_ms: int = 10000) -> List[Dict]:
        """
        error_data topic에서 에러 메시지 소비

        Args:
            max_records: 최대 읽을 메시지 개수
            timeout_ms: 대기 시간 (밀리초)

        Returns:
            List[Dict]: 에러 메시지 리스트
        """
        errors = []

        logger.info(f"📥 error_data topic 읽기 시작 (max: {max_records}개, timeout: {timeout_ms}ms)")

        try:
            for message in self.consumer:
                if len(errors) >= max_records:
                    break

                error_data = message.value
                logger.debug(f"📌 에러 메시지 수신: {error_data['error_type']}")

                errors.append({
                    'timestamp': error_data.get('timestamp'),
                    'error_type': error_data.get('error_type'),
                    'message': error_data.get('message'),
                    'module': error_data.get('module'),
                    'batch_count': error_data.get('batch_count'),
                    'status': error_data.get('status', 'pending_retry'),
                    'kafka_offset': message.offset,
                    'kafka_partition': message.partition
                })

            if errors:
                logger.info(f"✅ {len(errors)}개의 에러 메시지 수신 완료")
                # 마지막 메시지 offset commit
                self.consumer.commit()

        except KafkaError as e:
            logger.error(f"❌ 에러 메시지 수신 실패: {e}")
            raise

        return errors

    def store_errors_to_db(self, errors: List[Dict]) -> int:
        """
        에러 메시지를 PostgreSQL error_logs 테이블에 저장

        Args:
            errors: 저장할 에러 메시지 리스트

        Returns:
            int: 저장된 레코드 개수
        """
        if not errors:
            logger.info("저장할 에러가 없습니다")
            return 0

        if not self.db_conn:
            self._init_db_connection()

        inserted_count = 0

        try:
            cursor = self.db_conn.cursor()

            insert_query = sql.SQL(
                """
                INSERT INTO error_logs
                (timestamp, error_type, message, module, batch_count, status)
                VALUES (%s, %s, %s, %s, %s, %s)
                """
            )

            for error in errors:
                try:
                    cursor.execute(
                        insert_query,
                        (
                            error['timestamp'],
                            error['error_type'],
                            error['message'],
                            error['module'],
                            error['batch_count'],
                            error['status']
                        )
                    )
                    inserted_count += 1

                except psycopg2.Error as e:
                    logger.error(f"⚠️  에러 저장 실패 (offset {error['kafka_offset']}): {e}")
                    continue

            self.db_conn.commit()
            logger.info(f"✅ {inserted_count}/{len(errors)}개 에러 저장 완료")

            cursor.close()

        except psycopg2.Error as e:
            logger.error(f"❌ 에러 저장 실패: {e}")
            self.db_conn.rollback()
            raise

        return inserted_count

    def calculate_statistics(self) -> Dict:
        """
        지난 1시간의 에러 통계 계산

        Returns:
            Dict: 에러 통계
                {
                    'total_errors': int,
                    'critical_count': int,
                    'pending_count': int,
                    'by_error_type': {
                        'DatabaseError': 10,
                        'KafkaError': 5,
                        ...
                    },
                    'by_module': {...}
                }
        """
        if not self.db_conn:
            self._init_db_connection()

        try:
            cursor = self.db_conn.cursor()

            # 1시간 에러 통계
            cursor.execute("""
                SELECT
                    COUNT(*) as total_errors,
                    SUM(CASE WHEN status = 'critical' THEN 1 ELSE 0 END) as critical_count,
                    SUM(CASE WHEN status = 'pending_retry' THEN 1 ELSE 0 END) as pending_count
                FROM error_logs
                WHERE timestamp > NOW() - INTERVAL '1 hour'
            """)

            row = cursor.fetchone()
            stats = {
                'total_errors': row[0] or 0,
                'critical_count': row[1] or 0,
                'pending_count': row[2] or 0,
            }

            # 에러 타입별 통계
            cursor.execute("""
                SELECT error_type, COUNT(*) as count
                FROM error_logs
                WHERE timestamp > NOW() - INTERVAL '1 hour'
                GROUP BY error_type
                ORDER BY count DESC
            """)

            stats['by_error_type'] = {row[0]: row[1] for row in cursor.fetchall()}

            # 모듈별 통계
            cursor.execute("""
                SELECT module, COUNT(*) as count
                FROM error_logs
                WHERE timestamp > NOW() - INTERVAL '1 hour'
                GROUP BY module
                ORDER BY count DESC
            """)

            stats['by_module'] = {row[0]: row[1] for row in cursor.fetchall()}

            logger.info(f"📊 에러 통계: 총 {stats['total_errors']}개 "
                       f"(Critical: {stats['critical_count']}, Pending: {stats['pending_count']})")

            cursor.close()
            return stats

        except psycopg2.Error as e:
            logger.error(f"❌ 통계 계산 실패: {e}")
            raise

    def store_statistics_to_db(self, stats: Dict) -> int:
        """
        에러 통계를 error_statistics 테이블에 저장

        Args:
            stats: calculate_statistics()의 반환값

        Returns:
            int: 저장된 레코드 개수
        """
        if not self.db_conn:
            self._init_db_connection()

        inserted_count = 0

        try:
            cursor = self.db_conn.cursor()

            hour_timestamp = datetime.now().replace(minute=0, second=0, microsecond=0)

            insert_query = sql.SQL(
                """
                INSERT INTO error_statistics
                (hour_timestamp, error_type, error_count, critical_count, pending_count)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (hour_timestamp, error_type) DO UPDATE SET
                    error_count = EXCLUDED.error_count,
                    critical_count = EXCLUDED.critical_count,
                    pending_count = EXCLUDED.pending_count
                """
            )

            # 에러 타입별로 저장
            for error_type, count in stats['by_error_type'].items():
                critical_count = 0
                pending_count = 0

                # 각 에러 타입별 critical/pending 개수 조회
                cursor.execute("""
                    SELECT
                        SUM(CASE WHEN status = 'critical' THEN 1 ELSE 0 END),
                        SUM(CASE WHEN status = 'pending_retry' THEN 1 ELSE 0 END)
                    FROM error_logs
                    WHERE timestamp > NOW() - INTERVAL '1 hour'
                    AND error_type = %s
                """, (error_type,))

                row = cursor.fetchone()
                if row:
                    critical_count = row[0] or 0
                    pending_count = row[1] or 0

                cursor.execute(
                    insert_query,
                    (hour_timestamp, error_type, count, critical_count, pending_count)
                )
                inserted_count += 1

            self.db_conn.commit()
            logger.info(f"✅ {inserted_count}개 통계 저장 완료")

            cursor.close()

        except psycopg2.Error as e:
            logger.error(f"❌ 통계 저장 실패: {e}")
            self.db_conn.rollback()
            raise

        return inserted_count

    def close(self):
        """연결 종료"""
        if self.consumer:
            self.consumer.close()
            logger.info("✅ Kafka Consumer 종료")

        if self.db_conn:
            self.db_conn.close()
            logger.info("✅ PostgreSQL 연결 종료")


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
    print("Error Data Consumer 테스트")
    print("="*60 + "\n")

    try:
        # 에러 컨슈머 초기화
        consumer = ErrorDataConsumer(
            bootstrap_servers='localhost:9092',
            group_id='error_consumer_group'
        )

        # 에러 메시지 소비
        errors = consumer.consume_errors(max_records=100)

        if errors:
            # 에러를 DB에 저장
            inserted = consumer.store_errors_to_db(errors)
            print(f"\n✅ {inserted}개 에러 저장 완료")

            # 통계 계산
            stats = consumer.calculate_statistics()
            print(f"\n📊 통계:")
            print(f"   총 에러: {stats['total_errors']}개")
            print(f"   Critical: {stats['critical_count']}개")
            print(f"   Pending: {stats['pending_count']}개")
            print(f"   에러 타입: {stats['by_error_type']}")

            # 통계 저장
            consumer.store_statistics_to_db(stats)
        else:
            print("✅ 처리할 에러가 없습니다")

        consumer.close()

    except Exception as e:
        print(f"❌ 에러: {e}")
        import traceback
        traceback.print_exc()

    print("\n" + "="*60)
    print("테스트 완료")
    print("="*60 + "\n")
