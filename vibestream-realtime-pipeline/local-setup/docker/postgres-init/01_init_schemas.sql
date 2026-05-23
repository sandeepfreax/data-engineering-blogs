-- VibeStream Feature Store — Schema Initialization
-- This mirrors what dbt Gold models will populate.

CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

-- Gold layer: ML Feature tables
CREATE TABLE IF NOT EXISTS gold.user_engagement_features (
    user_id             VARCHAR(64)     NOT NULL,
    window_start        TIMESTAMP       NOT NULL,
    window_end          TIMESTAMP       NOT NULL,
    total_likes         BIGINT          DEFAULT 0,
    total_shares        BIGINT          DEFAULT 0,
    total_comments      BIGINT          DEFAULT 0,
    total_impressions   BIGINT          DEFAULT 0,
    engagement_score    DECIMAL(10, 4)  DEFAULT 0.0,   -- Computed feature for ML
    content_virality    DECIMAL(10, 4)  DEFAULT 0.0,   -- Virality index
    created_at          TIMESTAMP       DEFAULT NOW(),
    PRIMARY KEY (user_id, window_start)
);

CREATE TABLE IF NOT EXISTS gold.content_trending_features (
    content_id          VARCHAR(64)     NOT NULL,
    window_start        TIMESTAMP       NOT NULL,
    window_end          TIMESTAMP       NOT NULL,
    reaction_count      BIGINT          DEFAULT 0,
    share_velocity      DECIMAL(10, 4)  DEFAULT 0.0,   -- Shares per minute
    is_trending         BOOLEAN         DEFAULT FALSE,
    trending_score      DECIMAL(10, 4)  DEFAULT 0.0,
    created_at          TIMESTAMP       DEFAULT NOW(),
    PRIMARY KEY (content_id, window_start)
);

COMMENT ON TABLE gold.user_engagement_features IS
    'ML Feature Store: 5-min rolling user engagement signals for recommendation model';
COMMENT ON TABLE gold.content_trending_features IS
    'ML Feature Store: Content virality and trending signals for trend detection model';
