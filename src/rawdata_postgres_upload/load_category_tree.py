"""
Category Tree 데이터를 PostgreSQL에 로드
"""

import psycopg2 # (PostgreSQL)
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from config.settings import (
    POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD
)


# ============== 메인 실행 ==============
def main():

    print("Category Tree 로드 시작...")

    # PostgreSQL 연결
    conn = psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        database=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD
    )
    cursor = conn.cursor()

    # 테이블 생성
    cursor.execute("DROP TABLE IF EXISTS category_tree CASCADE")
    cursor.execute("""
        CREATE TABLE category_tree (
            categoryid INTEGER PRIMARY KEY,
            parentid INTEGER
        )
    """)
    cursor.execute("CREATE INDEX idx_category_parentid ON category_tree(parentid)")
    print("테이블 생성 완료")

    # COPY 명령으로 CSV 로드
    print("CSV 로드 시작")
    with open("data/raw/category_tree.csv", 'r', encoding='utf-8') as f:
        cursor.copy_expert("""
            COPY category_tree (categoryid, parentid)
            FROM STDIN WITH CSV HEADER
        """, f)

    conn.commit()

    # 삽입된 개수 확인
    cursor.execute("SELECT COUNT(*) FROM category_tree")
    total = cursor.fetchone()[0]
    print(f"완료: 총 {total:,}개 삽입")

    conn.close()


if __name__ == '__main__':
    main()
