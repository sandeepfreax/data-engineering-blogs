{{
  config(
    materialized    = 'incremental',
    unique_key      = ['user_id', 'window_start'],
    incremental_strategy = 'merge',
    on_schema_change = 'append_new_columns'
  )
}}

/*
  Gold Model: user_engagement_features
  =====================================
  Computes 5-minute rolling engagement features per user.
  These features are consumed by the ML Recommendation Model
  to personalize the VibeStream content feed.

  Feature definitions:
  - engagement_score  : Weighted sum of interactions (shares > comments > likes > impressions)
  - content_virality  : Share velocity relative to total impressions (proxy for content spread)

  Incremental strategy:
  - On each dbt run, only process events from the last 1 hour (configurable).
  - MERGE on (user_id, window_start) ensures idempotent runs.
*/

WITH engagement_raw AS (
    SELECT
        user_id,
        event_type,
        event_ts,
        -- 5-minute tumbling window
        DATE_TRUNC('minute', event_ts) -
            INTERVAL '1 minute' * (EXTRACT(MINUTE FROM event_ts)::INT % 5) AS window_start,
        DATE_TRUNC('minute', event_ts) -
            INTERVAL '1 minute' * (EXTRACT(MINUTE FROM event_ts)::INT % 5) +
            INTERVAL '5 minutes' AS window_end
    FROM {{ ref('silver_engagement_events') }}

    {% if is_incremental() %}
    -- Incremental: only process last 2 hours to catch late-arriving events
    WHERE event_ts >= NOW() - INTERVAL '2 hours'
    {% endif %}
),

aggregated AS (
    SELECT
        user_id,
        window_start,
        window_end,
        COUNT(*) FILTER (WHERE event_type = 'like')        AS total_likes,
        COUNT(*) FILTER (WHERE event_type = 'share')       AS total_shares,
        COUNT(*) FILTER (WHERE event_type = 'comment')     AS total_comments,
        COUNT(*) FILTER (WHERE event_type = 'impression')  AS total_impressions
    FROM engagement_raw
    GROUP BY user_id, window_start, window_end
)

SELECT
    user_id,
    window_start,
    window_end,
    total_likes,
    total_shares,
    total_comments,
    total_impressions,
    /*
      Engagement Score Formula:
      Shares carry 4x weight (highest intent), comments 3x, likes 2x, impressions 1x.
      Normalized per impression to account for content exposure differences.
      A score of 1.0 means every impression resulted in a share — extremely viral.
    */
    CASE
        WHEN total_impressions = 0 THEN 0
        ELSE ROUND(
            (total_shares * 4.0 + total_comments * 3.0 + total_likes * 2.0 + total_impressions * 1.0)
            / NULLIF(total_impressions, 0),
            4
        )
    END AS engagement_score,
    /*
      Content Virality Index:
      Ratio of shares to total impressions.
      Ranges 0-1 (1 = every view resulted in a share).
    */
    ROUND(
        total_shares::DECIMAL / NULLIF(total_impressions, 0),
        4
    ) AS content_virality,
    NOW() AS created_at

FROM aggregated
