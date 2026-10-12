WITH
-- 固定本次核验的九个账号；按平台与稳定账号 ID 关联，避免同名旧号混入。
roster AS (
    SELECT JSON_VALUE(account, '$.blogger_name') AS blogger_name,
        JSON_VALUE(account, '$.blogger_id') AS blogger_id,
        JSON_VALUE(account, '$.channel') AS channel,
        JSON_VALUE(account, '$.creator_id') AS creator_id,
        JSON_VALUE(account, '$.account_handle') AS roster_handle
    FROM UNNEST(JSON_QUERY_ARRAY(@accounts_json)) AS account
),
-- 复用视频维表的准入范围，保留无播放的视频；不按正播放日判断发布。
cohort AS (
    SELECT r.blogger_name, r.blogger_id, d.channel, d.creator_id,
        d.video_key, d.video_id, d.video_title, d.video_link,
        COALESCE(d.account_handle, r.roster_handle) AS account_handle,
        d.account_url, d.published_ts_utc, d.published_dt,
        CASE WHEN d.video_title IS NULL AND d.video_link IS NULL
            AND d.published_ts_utc = d.formal_tag_created_ts_utc
            THEN '正式号标记时间（待平台回流）'
            ELSE '平台发布时间' END AS published_time_basis,
        d.update_time AS dim_updated_at
    FROM `my-project-8584-jetonai.decom.dim_video` d
    INNER JOIN roster r USING (channel, creator_id)
    WHERE d.published_dt BETWEEN @start_date AND @end_date
    QUALIFY ROW_NUMBER() OVER (PARTITION BY d.video_key ORDER BY d.update_time DESC) = 1
),
-- 仅读取本次发布 cohort 的日状态，日期条件裁剪分区。
daily_state AS (
    SELECT s.dt, s.video_key, s.day_end_views, s.day_end_likes,
        s.day_end_comments, s.day_end_shares, s.update_time
    FROM `my-project-8584-jetonai.decom.dws_social_video_state_daily_v2` s
    INNER JOIN cohort c USING (video_key)
    WHERE s.dt BETWEEN @start_date AND @end_date
),
-- 只有 snapshot 行能证明当天真实采集；state 的沿用行不代表新采集。
real_snapshots AS (
    SELECT s.dt, s.video_key,
        LOGICAL_OR(s.day_end_views IS NOT NULL) AS views_known,
        LOGICAL_OR(s.day_end_likes IS NOT NULL) AS likes_known,
        LOGICAL_OR(s.day_end_comments IS NOT NULL) AS comments_known,
        LOGICAL_OR(s.day_end_shares IS NOT NULL) AS shares_known
    FROM `my-project-8584-jetonai.decom.dws_social_video_snapshot_daily_v2` s
    INNER JOIN cohort c USING (video_key)
    WHERE s.dt BETWEEN @start_date AND @end_date
    GROUP BY s.dt, s.video_key
),
-- 从视频发布时间起逐日展开，源表尚未更新的日期保留为未知而非补零。
calendar AS (
    SELECT c.*, dt
    FROM cohort c
    CROSS JOIN UNNEST(GENERATE_DATE_ARRAY(c.published_dt, @end_date)) AS dt
),
-- 标明源状态是否存在，并记录截至该日最近一个真实采集日。
observations AS (
    SELECT c.*, s.video_key IS NOT NULL AS state_available,
        p.video_key IS NOT NULL AS has_snapshot,
        MAX(IF(p.video_key IS NOT NULL, c.dt, NULL)) OVER (
            PARTITION BY c.video_key ORDER BY c.dt
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS last_snapshot_dt,
        COUNTIF(p.views_known) OVER known_days > 0 AS views_known,
        COUNTIF(p.likes_known) OVER known_days > 0 AS likes_known,
        COUNTIF(p.comments_known) OVER known_days > 0 AS comments_known,
        COUNTIF(p.shares_known) OVER known_days > 0 AS shares_known,
        s.day_end_views, s.day_end_likes, s.day_end_comments,
        s.day_end_shares, s.update_time AS state_updated_at
    FROM calendar c
    LEFT JOIN daily_state s ON s.dt = c.dt AND s.video_key = c.video_key
    LEFT JOIN real_snapshots p ON p.dt = c.dt AND p.video_key = c.video_key
    WINDOW known_days AS (PARTITION BY c.video_key ORDER BY c.dt
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
-- 首次真实采集前，状态表里的占位零还原为未知；YouTube 分享数始终未知。
known_metrics AS (
    SELECT * EXCEPT(day_end_views, day_end_likes, day_end_comments, day_end_shares),
        IF(views_known AND state_available, day_end_views, NULL) AS total_views,
        IF(likes_known AND state_available, day_end_likes, NULL) AS total_likes,
        IF(comments_known AND state_available, day_end_comments, NULL) AS total_comments,
        IF(shares_known AND state_available AND channel != 'youtube', day_end_shares, NULL) AS total_shares
    FROM observations
)
SELECT dt, 'Ever Pretty' AS brand, blogger_id, blogger_name, channel, creator_id,
    account_handle, account_url, video_key, video_id, video_title, video_link,
    published_ts_utc, published_dt, published_time_basis,
    total_views, total_likes, total_comments, total_shares,
    total_views - LAG(total_views) OVER video_days AS view_increment,
    total_likes - LAG(total_likes) OVER video_days AS like_increment,
    total_comments - LAG(total_comments) OVER video_days AS comment_increment,
    total_shares - LAG(total_shares) OVER video_days AS share_increment,
    has_snapshot, state_available, last_snapshot_dt,
    CASE WHEN NOT state_available THEN '日状态尚未更新'
        WHEN has_snapshot THEN '当天真实采集'
        WHEN last_snapshot_dt IS NOT NULL THEN '沿用上次采集'
        ELSE '尚无真实采集' END AS collection_status,
    dt < CURRENT_DATE('America/Toronto') AS is_complete_day,
    dim_updated_at, state_updated_at, CURRENT_TIMESTAMP() AS update_time
FROM known_metrics
WINDOW video_days AS (PARTITION BY video_key ORDER BY dt)
