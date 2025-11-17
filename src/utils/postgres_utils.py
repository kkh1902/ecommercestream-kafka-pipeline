import psycopg2
from psycopg2.extras import execute_values, RealDictCursor
from psycopg2.extensions import connection, cursor
from contextlib import contextmanager
from typing import List, Tuple, Dict, Any, Optional
from .logger import get_logger

logger = get_logger(__name__)


def get_postgres_connection(
    host: str,
    port: int,
    database: str,
    user: str,
    password: str,
    **kwargs
) -> connection:
    try:
        conn = psycopg2.connect(
            host=host,
            port=port,
            database=database,
            user=user,
            password=password,
            **kwargs
        )
        logger.info(f"PostgreSQL 연결 성공: {host}:{port}/{database}")
        return conn
    except psycopg2.Error as e:
        logger.error(f"PostgreSQL 연결 실패: {e}")
        raise


@contextmanager
def get_postgres_cursor(conn: connection, dict_cursor: bool = False):
    cursor_factory = RealDictCursor if dict_cursor else None
    cur = conn.cursor(cursor_factory=cursor_factory)
    try:
        yield cur
        conn.commit()
    except Exception as e:
        conn.rollback()
        logger.error(f"트랜잭션 롤백: {e}")
        raise
    finally:
        cur.close()


def execute_query(
    conn: connection,
    query: str,
    params: Optional[Tuple] = None,
    fetch: bool = False,
    dict_cursor: bool = False
) -> Optional[List]:
    with get_postgres_cursor(conn, dict_cursor) as cur:
        try:
            cur.execute(query, params)
            if fetch:
                return cur.fetchall()
            logger.debug(f"쿼리 실행 성공: {query[:50]}...")
        except psycopg2.Error as e:
            logger.error(f"쿼리 실행 실패: {e}")
            raise


def batch_insert(
    conn: connection,
    table_name: str,
    columns: List[str],
    data: List[Tuple],
    page_size: int = 1000
) -> int:
    if not data:
        logger.warning("삽입할 데이터가 없습니다")
        return 0

    columns_str = ', '.join(columns)
    query = f"INSERT INTO {table_name} ({columns_str}) VALUES %s"

    with get_postgres_cursor(conn) as cur:
        try:
            execute_values(cur, query, data, page_size=page_size)
            inserted_count = len(data)
            logger.info(f"배치 INSERT 완료: {table_name} 테이블에 {inserted_count}개 삽입")
            return inserted_count
        except psycopg2.Error as e:
            logger.error(f"배치 INSERT 실패: {e}")
            raise


def table_exists(conn: connection, table_name: str, schema: str = 'public') -> bool:
    query = """
        SELECT EXISTS (
            SELECT FROM information_schema.tables
            WHERE table_schema = %s
            AND table_name = %s
        );
    """
    result = execute_query(conn, query, (schema, table_name), fetch=True)
    return result[0][0] if result else False


def get_table_row_count(conn: connection, table_name: str) -> int:
    query = f"SELECT COUNT(*) FROM {table_name};"
    result = execute_query(conn, query, fetch=True)
    count = result[0][0] if result else 0
    logger.info(f"{table_name} 테이블 행 개수: {count}")
    return count


def test_postgres_connection(
    host: str,
    port: int,
    database: str,
    user: str,
    password: str
) -> bool:
    try:
        conn = get_postgres_connection(host, port, database, user, password)
        conn.close()
        logger.info("PostgreSQL 연결 테스트 성공")
        return True
    except Exception as e:
        logger.error(f"PostgreSQL 연결 테스트 실패: {e}")
        return False


def create_table_from_schema(conn: connection, schema_file: str):
    try:
        with open(schema_file, 'r', encoding='utf-8') as f:
            schema_sql = f.read()

        with get_postgres_cursor(conn) as cur:
            cur.execute(schema_sql)

        logger.info(f"스키마 적용 완료: {schema_file}")
    except FileNotFoundError:
        logger.error(f"스키마 파일을 찾을 수 없습니다: {schema_file}")
        raise
    except psycopg2.Error as e:
        logger.error(f"스키마 적용 실패: {e}")
        raise
