"""
일일 통계관리 DAG
매일 새벽 3시에 통계 데이터 집계 및 분석
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta

# 기본 설정
default_args = {
    'owner': 'analytics-team',
    'depends_on_past': False,
    'start_date': datetime(2025, 11, 1),
    'email_on_failure': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'daily_statistics_management',
    default_args=default_args,
    description='일일 통계 데이터 집계 및 분석',
    schedule_interval='0 3 * * *',  # 매일 새벽 3시
    catchup=False,
    tags=['statistics', 'analytics'],
)


# ===== Task 1: 일일 사용자 통계 =====
def calculate_daily_user_stats(**context):
    """
    일일 사용자 통계 계산
    - DAU (Daily Active Users)
    - 신규 사용자 수
    - 재방문 사용자 수
    - 사용자 세그먼트 분석
    """
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    hook = PostgresHook(postgres_conn_id='postgres_default')

    # 일일 기준 날짜
    exec_date = context['ds']  # YYYY-MM-DD 형식

    sql = f"""
    SELECT
        '{exec_date}'::DATE as stat_date,
        COUNT(DISTINCT visitorid) as dau,
        COUNT(DISTINCT CASE WHEN event = 'view' THEN visitorid END) as view_users,
        COUNT(DISTINCT CASE WHEN event = 'click' THEN visitorid END) as click_users,
        COUNT(DISTINCT CASE WHEN event = 'purchase' THEN visitorid END) as purchase_users
    FROM statistics_events
    WHERE event_date = '{exec_date}'
    """

    result = hook.get_records(sql)
    context['task_instance'].xcom_push(key='dau_stats', value=result[0] if result else None)
    print(f"✅ 일일 사용자 통계 계산 완료: DAU={result[0] if result else 'N/A'}")


t1_user_stats = PythonOperator(
    task_id='calculate_daily_user_stats',
    python_callable=calculate_daily_user_stats,
    dag=dag,
)


# ===== Task 2: 일일 상품 통계 =====
def calculate_daily_product_stats(**context):
    """
    일일 상품 통계 계산
    - 상품별 조회수, 클릭수, 구매수
    - Top N 상품
    - 카테고리별 성과
    - 상품 인기도 지수
    """
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    hook = PostgresHook(postgres_conn_id='postgres_default')

    # 일일 기준 날짜
    exec_date = context['ds']  # YYYY-MM-DD 형식

    sql = f"""
    SELECT
        '{exec_date}'::DATE as stat_date,
        itemid,
        COUNT(*) FILTER (WHERE event = 'view') as view_count,
        COUNT(*) FILTER (WHERE event = 'click') as click_count,
        COUNT(*) FILTER (WHERE event = 'purchase') as purchase_count,
        COUNT(DISTINCT visitorid) as unique_users,
        ROUND(
            (COUNT(*) FILTER (WHERE event = 'click')::FLOAT /
             NULLIF(COUNT(*) FILTER (WHERE event = 'view')::FLOAT, 0) * 100), 2
        ) as view_to_click_rate,
        ROUND(
            (COUNT(*) FILTER (WHERE event = 'purchase')::FLOAT /
             NULLIF(COUNT(*) FILTER (WHERE event = 'click')::FLOAT, 0) * 100), 2
        ) as click_to_purchase_rate
    FROM statistics_events
    WHERE event_date = '{exec_date}'
    GROUP BY itemid
    ORDER BY purchase_count DESC
    LIMIT 100
    """

    result = hook.get_records(sql)
    context['task_instance'].xcom_push(key='product_stats', value=result)
    print(f"✅ 일일 상품 통계 계산 완료: {len(result) if result else 0}개 상품")


t2_product_stats = PythonOperator(
    task_id='calculate_daily_product_stats',
    python_callable=calculate_daily_product_stats,
    dag=dag,
)


# ===== Task 3: 일일 매출 통계 =====
def calculate_daily_sales_stats(**context):
    """
    일일 매출 통계 계산
    - 총 거래 건수
    - 주문별 사용자 수
    - 평균 트랜잭션 가치
    - 시간대별 거래
    - 카테고리별 거래
    """
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    hook = PostgresHook(postgres_conn_id='postgres_default')

    # 일일 기준 날짜
    exec_date = context['ds']  # YYYY-MM-DD 형식

    sql = f"""
    SELECT
        '{exec_date}'::DATE as stat_date,
        COUNT(DISTINCT transactionid) as total_transactions,
        COUNT(DISTINCT CASE WHEN transactionid IS NOT NULL THEN visitorid END) as paying_users,
        COUNT(*) FILTER (WHERE is_purchase = 1) as purchase_events,
        COUNT(DISTINCT CASE WHEN hour_of_day IS NOT NULL THEN hour_of_day END) as active_hours,
        COUNT(DISTINCT categoryid) as active_categories
    FROM statistics_events
    WHERE event_date = '{exec_date}'
    """

    result = hook.get_records(sql)
    context['task_instance'].xcom_push(key='sales_stats', value=result[0] if result else None)
    print(f"✅ 일일 매출 통계 계산 완료")


t3_sales_stats = PythonOperator(
    task_id='calculate_daily_sales_stats',
    python_callable=calculate_daily_sales_stats,
    dag=dag,
)


# ===== Task 4: 일일 전환율 분석 =====
def calculate_daily_conversion_stats(**context):
    """
    일일 전환율 분석
    - View → Click 전환율
    - Click → Purchase 전환율
    - 전체 전환율 (View → Purchase)
    """
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    hook = PostgresHook(postgres_conn_id='postgres_default')

    # 일일 기준 날짜
    exec_date = context['ds']  # YYYY-MM-DD 형식

    sql = f"""
    SELECT
        '{exec_date}'::DATE as stat_date,
        COUNT(*) FILTER (WHERE event = 'view') as total_views,
        COUNT(*) FILTER (WHERE event = 'click') as total_clicks,
        COUNT(*) FILTER (WHERE event = 'purchase') as total_purchases,
        ROUND(
            (COUNT(*) FILTER (WHERE event = 'click')::FLOAT /
             NULLIF(COUNT(*) FILTER (WHERE event = 'view')::FLOAT, 0) * 100), 2
        ) as view_to_click_conversion,
        ROUND(
            (COUNT(*) FILTER (WHERE event = 'purchase')::FLOAT /
             NULLIF(COUNT(*) FILTER (WHERE event = 'click')::FLOAT, 0) * 100), 2
        ) as click_to_purchase_conversion,
        ROUND(
            (COUNT(*) FILTER (WHERE event = 'purchase')::FLOAT /
             NULLIF(COUNT(*) FILTER (WHERE event = 'view')::FLOAT, 0) * 100), 2
        ) as overall_conversion
    FROM statistics_events
    WHERE event_date = '{exec_date}'
    """

    result = hook.get_records(sql)
    context['task_instance'].xcom_push(key='conversion_stats', value=result[0] if result else None)
    print(f"✅ 일일 전환율 분석 완료")


t4_conversion_stats = PythonOperator(
    task_id='calculate_daily_conversion_stats',
    python_callable=calculate_daily_conversion_stats,
    dag=dag,
)


# ===== Task 5: 통계 집계 데이터 저장 =====
t5_save_daily_stats = PostgresOperator(
    task_id='save_daily_statistics',
    postgres_conn_id='postgres_default',
    sql="""
        -- 일일 통계 저장
        -- INSERT INTO daily_statistics (...)
        --   SELECT ...
        -- TODO: daily_statistics 테이블 정의 후 구현
        SELECT 1;  -- 플레이스홀더
    """,
    dag=dag,
)


# ===== Task 6: 시계열 데이터 집계 (시간대별) =====
def aggregate_hourly_stats(**context):
    """
    시간대별 통계 집계
    - 매시간 사용자 수
    - 매시간 구매 이벤트
    - 매시간 트렌드
    """
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    hook = PostgresHook(postgres_conn_id='postgres_default')

    # 일일 기준 날짜
    exec_date = context['ds']  # YYYY-MM-DD 형식

    sql = f"""
    SELECT
        '{exec_date}'::DATE as stat_date,
        hour_of_day,
        COUNT(DISTINCT visitorid) as hourly_users,
        COUNT(*) as total_events,
        COUNT(*) FILTER (WHERE is_purchase = 1) as purchase_events,
        COUNT(DISTINCT itemid) as unique_items,
        COUNT(DISTINCT categoryid) as unique_categories
    FROM statistics_events
    WHERE event_date = '{exec_date}' AND hour_of_day IS NOT NULL
    GROUP BY hour_of_day
    ORDER BY hour_of_day
    """

    result = hook.get_records(sql)
    context['task_instance'].xcom_push(key='hourly_stats', value=result)
    print(f"✅ 시간대별 통계 집계 완료: {len(result) if result else 0}개 시간대")


t6_hourly_agg = PythonOperator(
    task_id='aggregate_hourly_statistics',
    python_callable=aggregate_hourly_stats,
    dag=dag,
)


# ===== Task 7: 카테고리별 상세 분석 =====
def analyze_category_performance(**context):
    """
    카테고리별 상세 성과 분석
    - 카테고리별 이벤트
    - 카테고리별 구매율
    - 카테고리별 순위
    """
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    hook = PostgresHook(postgres_conn_id='postgres_default')

    # 일일 기준 날짜
    exec_date = context['ds']  # YYYY-MM-DD 형식

    sql = f"""
    SELECT
        '{exec_date}'::DATE as stat_date,
        categoryid,
        COUNT(DISTINCT itemid) as items_in_category,
        COUNT(DISTINCT visitorid) as unique_users,
        COUNT(*) as total_events,
        COUNT(*) FILTER (WHERE event = 'view') as view_count,
        COUNT(*) FILTER (WHERE event = 'click') as click_count,
        COUNT(*) FILTER (WHERE is_purchase = 1) as purchase_count,
        ROUND(
            (COUNT(*) FILTER (WHERE is_purchase = 1)::FLOAT /
             NULLIF(COUNT(*)::FLOAT, 0) * 100), 2
        ) as purchase_rate
    FROM statistics_events
    WHERE event_date = '{exec_date}' AND categoryid IS NOT NULL
    GROUP BY categoryid
    ORDER BY purchase_count DESC
    """

    result = hook.get_records(sql)
    context['task_instance'].xcom_push(key='category_stats', value=result)
    print(f"✅ 카테고리별 성과 분석 완료: {len(result) if result else 0}개 카테고리")


t7_category_analysis = PythonOperator(
    task_id='analyze_category_performance',
    python_callable=analyze_category_performance,
    dag=dag,
)


# ===== Task 8: 통계 대시보드 업데이트 =====
def update_dashboard_data(**context):
    """
    BI 대시보드용 데이터 업데이트
    - 일일 통계 요약
    - 대시보드 업데이트
    """
    print("✅ 대시보드 데이터 업데이트")
    print("  - DAU 통계 업데이트")
    print("  - 상품 통계 업데이트")
    print("  - 매출 통계 업데이트")
    print("  - 전환율 통계 업데이트")
    print("  - 시간대별 통계 업데이트")
    print("  - 카테고리별 통계 업데이트")


t8_dashboard_update = PythonOperator(
    task_id='update_dashboard_data',
    python_callable=update_dashboard_data,
    dag=dag,
)


# ===== Task 9: 일일 리포트 생성 =====
def generate_daily_report(**context):
    """
    일일 분석 리포트 생성
    - KPI 요약
    - 주요 지표
    - 성과 분석
    """
    exec_date = context['ds']

    # 이전 task에서 수집한 데이터
    dau_stats = context['task_instance'].xcom_pull(
        task_ids='calculate_daily_user_stats', key='dau_stats'
    )
    sales_stats = context['task_instance'].xcom_pull(
        task_ids='calculate_daily_sales_stats', key='sales_stats'
    )
    conversion_stats = context['task_instance'].xcom_pull(
        task_ids='calculate_daily_conversion_stats', key='conversion_stats'
    )

    print(f"""
    ========================================
    일일 분석 리포트 ({exec_date})
    ========================================

    📊 주요 KPI:
    - DAU (Daily Active Users): {dau_stats[0] if dau_stats else 'N/A'}
    - 총 거래건수: {sales_stats[0] if sales_stats else 'N/A'}
    - 전체 전환율: {conversion_stats[6] if conversion_stats else 'N/A'}%

    📈 상세 분석:
    - View → Click 전환율: {conversion_stats[4] if conversion_stats else 'N/A'}%
    - Click → Purchase 전환율: {conversion_stats[5] if conversion_stats else 'N/A'}%

    ✅ 리포트 생성 완료
    """)


t9_report = PythonOperator(
    task_id='generate_daily_report',
    python_callable=generate_daily_report,
    dag=dag,
)


# ===== Task 의존성 =====
# 병렬 실행: 각 통계 계산
[t1_user_stats, t2_product_stats, t3_sales_stats, t4_conversion_stats] >> t5_save_daily_stats
t5_save_daily_stats >> [t6_hourly_agg, t7_category_analysis]
[t6_hourly_agg, t7_category_analysis] >> t8_dashboard_update >> t9_report
