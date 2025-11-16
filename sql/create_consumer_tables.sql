-- ===================================
-- Consumer 2 (Spark Streaming) 테이블들
-- ===================================

-- 1. statistics_events 테이블 (통계 분석용)
-- 원본 데이터 + 카테고리 정보 + 통계 특성
-- BI 대시보드 및 경영진 분석용
CREATE TABLE IF NOT EXISTS statistics_events (
    id BIGINT PRIMARY KEY,
    timestamp BIGINT NOT NULL,
    visitorid INTEGER NOT NULL,
    itemid INTEGER NOT NULL,
    categoryid INTEGER,
    event VARCHAR(50) NOT NULL,
    transactionid INTEGER,

    -- 시간 정보
    event_date VARCHAR(10),           -- YYYY-MM-DD 형식
    hour_of_day INTEGER,              -- 0-23
    day_of_week INTEGER,              -- 1=Sunday, 7=Saturday

    -- 구매 관련
    is_purchase SMALLINT,             -- 0 또는 1 (구매 여부)

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 인덱스
CREATE INDEX IF NOT EXISTS idx_stats_timestamp ON statistics_events(timestamp);
CREATE INDEX IF NOT EXISTS idx_stats_visitorid ON statistics_events(visitorid);
CREATE INDEX IF NOT EXISTS idx_stats_itemid ON statistics_events(itemid);
CREATE INDEX IF NOT EXISTS idx_stats_categoryid ON statistics_events(categoryid);
CREATE INDEX IF NOT EXISTS idx_stats_event ON statistics_events(event);
CREATE INDEX IF NOT EXISTS idx_stats_event_date ON statistics_events(event_date);
CREATE INDEX IF NOT EXISTS idx_stats_hour_of_day ON statistics_events(hour_of_day);
CREATE INDEX IF NOT EXISTS idx_stats_is_purchase ON statistics_events(is_purchase);

COMMENT ON TABLE statistics_events IS '통계 분석용 데이터 (Consumer 2 - Spark Streaming, BI 대시보드용)';
COMMENT ON COLUMN statistics_events.categoryid IS 'item_properties에서 JOIN한 카테고리 ID';
COMMENT ON COLUMN statistics_events.event_date IS '이벤트 발생 날짜 (YYYY-MM-DD)';
COMMENT ON COLUMN statistics_events.hour_of_day IS '시간대 (0-23)';
COMMENT ON COLUMN statistics_events.day_of_week IS '요일 (1=일, 7=토)';
COMMENT ON COLUMN statistics_events.is_purchase IS '구매 이벤트 여부';

-- NOTE: ml_prepared_events는 src/ml/recommendation_data.py에서
-- PostgreSQL이 아닌 Python 메모리에서 처리됨 (별도 관리)


-- 2. item_properties 테이블 (상품 정보 매핑용)
-- categoryid 매핑 및 향후 상품 정보 저장
CREATE TABLE IF NOT EXISTS item_properties (
    itemid INTEGER PRIMARY KEY,
    categoryid INTEGER,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 인덱스
CREATE INDEX IF NOT EXISTS idx_item_properties_categoryid ON item_properties(categoryid);

COMMENT ON TABLE item_properties IS '상품별 카테고리 정보 (item_properties.csv에서 로드)';


-- 3. category_tree 테이블 (카테고리 계층 구조)
-- 카테고리의 부모-자식 관계 저장
CREATE TABLE IF NOT EXISTS category_tree (
    categoryid INTEGER PRIMARY KEY,
    parentid INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 인덱스
CREATE INDEX IF NOT EXISTS idx_category_tree_parentid ON category_tree(parentid);

COMMENT ON TABLE category_tree IS '카테고리 계층 구조 (category_tree.csv에서 로드)';
