-- ===================================
-- Consumer 2 (Spark Streaming) 테이블들
-- ===================================

-- 1. statistics_events 테이블 (통계 분석용)
-- 원본 데이터 + 카테고리 정보
-- BI 대시보드 및 경영진 분석용
CREATE TABLE IF NOT EXISTS statistics_events (
    id BIGINT PRIMARY KEY,
    timestamp BIGINT NOT NULL,
    visitorid INTEGER NOT NULL,
    itemid INTEGER NOT NULL,
    categoryid INTEGER,
    event VARCHAR(50) NOT NULL,
    transactionid INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 인덱스
CREATE INDEX IF NOT EXISTS idx_stats_timestamp ON statistics_events(timestamp);
CREATE INDEX IF NOT EXISTS idx_stats_visitorid ON statistics_events(visitorid);
CREATE INDEX IF NOT EXISTS idx_stats_itemid ON statistics_events(itemid);
CREATE INDEX IF NOT EXISTS idx_stats_categoryid ON statistics_events(categoryid);
CREATE INDEX IF NOT EXISTS idx_stats_event ON statistics_events(event);
CREATE INDEX IF NOT EXISTS idx_stats_transactionid ON statistics_events(transactionid);

COMMENT ON TABLE statistics_events IS '통계 분석용 데이터 (Consumer 2 - Spark Streaming)';
COMMENT ON COLUMN statistics_events.categoryid IS 'item_properties에서 JOIN한 카테고리 ID';


-- 2. ml_prepared_events 테이블 (ML 학습용)
-- 현재: 원본 데이터 저장
-- 향후: 정제된 데이터 + 특성 추가
CREATE TABLE IF NOT EXISTS ml_prepared_events (
    id BIGINT PRIMARY KEY,
    timestamp BIGINT NOT NULL,
    visitorid INTEGER NOT NULL,
    itemid INTEGER NOT NULL,
    categoryid INTEGER,
    event VARCHAR(50) NOT NULL,
    transactionid INTEGER,

    -- 향후 추가 예정 컬럼
    -- user_segment VARCHAR(50),
    -- item_popularity FLOAT,
    -- user_lifetime_events INT,
    -- ...

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 인덱스
CREATE INDEX IF NOT EXISTS idx_ml_timestamp ON ml_prepared_events(timestamp);
CREATE INDEX IF NOT EXISTS idx_ml_visitorid ON ml_prepared_events(visitorid);
CREATE INDEX IF NOT EXISTS idx_ml_itemid ON ml_prepared_events(itemid);
CREATE INDEX IF NOT EXISTS idx_ml_event ON ml_prepared_events(event);

COMMENT ON TABLE ml_prepared_events IS 'ML 학습용 데이터 (Consumer 2 - Spark Streaming)';
COMMENT ON COLUMN ml_prepared_events.categoryid IS 'item_properties에서 JOIN한 카테고리 ID';


-- 3. item_properties 테이블 (상품 정보 매핑용)
-- categoryid 매핑 및 향후 상품 정보 저장
CREATE TABLE IF NOT EXISTS item_properties (
    itemid INTEGER PRIMARY KEY,
    categoryid INTEGER,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 인덱스
CREATE INDEX IF NOT EXISTS idx_item_properties_categoryid ON item_properties(categoryid);

COMMENT ON TABLE item_properties IS '상품별 카테고리 정보 (item_properties.csv에서 로드)';


-- 4. category_tree 테이블 (카테고리 계층 구조)
-- 카테고리의 부모-자식 관계 저장
CREATE TABLE IF NOT EXISTS category_tree (
    categoryid INTEGER PRIMARY KEY,
    parentid INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 인덱스
CREATE INDEX IF NOT EXISTS idx_category_tree_parentid ON category_tree(parentid);

COMMENT ON TABLE category_tree IS '카테고리 계층 구조 (category_tree.csv에서 로드)';
