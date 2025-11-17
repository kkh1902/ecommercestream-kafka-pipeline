-- 원본 클릭스트림 데이터 (Kafka Consumer에서 저장)
CREATE TABLE IF NOT EXISTS raw_clickstream_events (
    id SERIAL PRIMARY KEY,
    timestamp BIGINT NOT NULL,
    visitorid INTEGER NOT NULL,
    event VARCHAR(50) NOT NULL,
    itemid INTEGER NOT NULL,
    transactionid INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 처리된 클릭스트림 데이터 (Spark Streaming에서 저장)
CREATE TABLE IF NOT EXISTS clickstream_events (
    id SERIAL PRIMARY KEY,
    timestamp TIMESTAMP NOT NULL,
    visitorid INTEGER NOT NULL,
    event VARCHAR(50) NOT NULL,
    itemid INTEGER NOT NULL,
    transactionid INTEGER,
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 인덱스 생성 (조회 성능 향상)
CREATE INDEX IF NOT EXISTS idx_timestamp ON clickstream_events(timestamp);
CREATE INDEX IF NOT EXISTS idx_visitorid ON clickstream_events(visitorid);
CREATE INDEX IF NOT EXISTS idx_event ON clickstream_events(event);
CREATE INDEX IF NOT EXISTS idx_itemid ON clickstream_events(itemid);

-- 통계를 위한 뷰
CREATE OR REPLACE VIEW event_summary AS
SELECT
    event,
    COUNT(*) as event_count,
    COUNT(DISTINCT visitorid) as unique_visitors,
    COUNT(DISTINCT itemid) as unique_items
FROM clickstream_events
GROUP BY event;

-- 카테고리 트리 (상품 카테고리 계층 구조)
CREATE TABLE IF NOT EXISTS category_tree (
    categoryid INTEGER PRIMARY KEY,
    parentid INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_category_parentid ON category_tree(parentid);

-- 상품 속성 (아이템별 속성 정보)
CREATE TABLE IF NOT EXISTS item_properties (
    id SERIAL PRIMARY KEY,
    timestamp BIGINT,
    itemid BIGINT NOT NULL,
    property VARCHAR(100),
    value TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_item_itemid ON item_properties(itemid);
CREATE INDEX IF NOT EXISTS idx_item_property ON item_properties(property);

-- 일일 통계 (매일 새벽 2시에 집계)
CREATE TABLE IF NOT EXISTS daily_statistics (
    id SERIAL PRIMARY KEY,
    stats_date DATE NOT NULL UNIQUE,
    total_sales INT,
    total_events INT,
    unique_visitors INT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_daily_stats_date ON daily_statistics(stats_date);

-- 데이터 품질 검증 결과 저장 테이블
CREATE TABLE IF NOT EXISTS data_quality_results (
    id SERIAL PRIMARY KEY,
    check_date DATE NOT NULL,
    table_name VARCHAR(100) NOT NULL,
    total_records BIGINT,
    quality_score FLOAT,
    status VARCHAR(20),
    null_count INT,
    invalid_count INT,
    details TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_dq_table_date ON data_quality_results(table_name, check_date);
