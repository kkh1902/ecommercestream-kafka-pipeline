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


-- 2. ml_prepared_events 테이블 (ML 학습용)
-- 정제 + 특성이 추가된 ML 학습 데이터
-- NULL 값 제거, 유효성 확인, 시계열 특성 포함
CREATE TABLE IF NOT EXISTS ml_prepared_events (
    id BIGINT PRIMARY KEY,
    timestamp BIGINT NOT NULL,
    visitorid INTEGER NOT NULL,
    itemid INTEGER NOT NULL,
    categoryid INTEGER,
    event VARCHAR(50) NOT NULL,
    transactionid INTEGER,

    -- 정제 관련 특성
    is_buyer SMALLINT,                -- 0 또는 1 (구매 이력 여부)

    -- 시간 특성
    event_hour INTEGER,               -- 0-23 (이벤트 발생 시간)
    event_dow INTEGER,                -- 1-7 (요일)
    event_month VARCHAR(2),           -- 01-12 (월)

    -- 사용자 관점 특성 (누적)
    user_event_count INTEGER,         -- 사용자가 지금까지 한 이벤트 수
    is_user_first_event SMALLINT,     -- 0 또는 1 (사용자의 첫 이벤트)

    -- 상품 관점 특성 (누적)
    item_event_count INTEGER,         -- 상품이 지금까지 받은 이벤트 수
    is_item_first_event SMALLINT,     -- 0 또는 1 (상품의 첫 이벤트)

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 인덱스
CREATE INDEX IF NOT EXISTS idx_ml_timestamp ON ml_prepared_events(timestamp);
CREATE INDEX IF NOT EXISTS idx_ml_visitorid ON ml_prepared_events(visitorid);
CREATE INDEX IF NOT EXISTS idx_ml_itemid ON ml_prepared_events(itemid);
CREATE INDEX IF NOT EXISTS idx_ml_event ON ml_prepared_events(event);
CREATE INDEX IF NOT EXISTS idx_ml_is_buyer ON ml_prepared_events(is_buyer);
CREATE INDEX IF NOT EXISTS idx_ml_user_event_count ON ml_prepared_events(user_event_count);
CREATE INDEX IF NOT EXISTS idx_ml_item_event_count ON ml_prepared_events(item_event_count);

COMMENT ON TABLE ml_prepared_events IS 'ML 학습용 정제 데이터 (Consumer 2 - Spark Streaming, 정제+특성 포함)';
COMMENT ON COLUMN ml_prepared_events.categoryid IS 'item_properties에서 JOIN한 카테고리 ID';
COMMENT ON COLUMN ml_prepared_events.is_buyer IS 'transactionid 있음 = 구매 고객';
COMMENT ON COLUMN ml_prepared_events.event_hour IS '이벤트 발생 시간대 (0-23)';
COMMENT ON COLUMN ml_prepared_events.event_dow IS '요일 (1=일, 7=토)';
COMMENT ON COLUMN ml_prepared_events.user_event_count IS '이 시점까지 사용자의 누적 이벤트 수';
COMMENT ON COLUMN ml_prepared_events.is_user_first_event IS '사용자의 첫 이벤트 여부';
COMMENT ON COLUMN ml_prepared_events.item_event_count IS '이 시점까지 상품의 누적 이벤트 수';
COMMENT ON COLUMN ml_prepared_events.is_item_first_event IS '상품의 첫 이벤트 여부';


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
