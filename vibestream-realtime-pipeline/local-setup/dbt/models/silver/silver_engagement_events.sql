{{
  config(materialized = 'view')
}}

/*
  Silver View — thin pass-through over the PostgreSQL silver table.
  In production with dbt-spark/dbt-databricks, this references
  the Delta Lake table directly without a loader step.
*/

SELECT
    event_id,
    event_type,
    user_id,
    content_id,
    content_type,
    creator_id,
    device_type,
    platform,
    event_ts::TIMESTAMP     AS event_ts,
    ingestion_ts::TIMESTAMP AS ingestion_ts,
    session_id,
    watch_duration,
    comment_length,
    event_hour,
    is_mobile,
    is_viral_content_type
FROM silver.engagement_events
WHERE event_ts IS NOT NULL