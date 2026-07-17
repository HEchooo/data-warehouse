-- 素材生产与互动全量临时取数（2026-07）
-- 统计范围：全历史。
-- 粒度：每个 asset_code 一行。
WITH
  -- 素材主数据层：同一 asset_code 存在多条记录时，保留最近更新的一条。
  asset_detail AS (
    SELECT
      CAST(asset_code AS STRING) AS asset_code,
      DATETIME(TIMESTAMP(gmt_create, 'UTC'), 'Asia/Shanghai') AS production_time_cn
    FROM
      `my-project-8584-jetonai.v3_decom.community_fashion_asset`
    WHERE
      NULLIF(CAST(asset_code AS STRING), '') IS NOT NULL
    QUALIFY
      ROW_NUMBER() OVER (
        PARTITION BY
          CAST(asset_code AS STRING)
        ORDER BY
          TIMESTAMP(COALESCE(gmt_update, gmt_create), 'UTC') DESC,
          id DESC
      ) = 1
  ),
  -- 曝光事件素材明细层：将事件 args.asset_code 数组拆到素材粒度，并排除匿名用户。
  exposure_event_asset AS (
    SELECT
      NULLIF(JSON_VALUE(asset_code_json), '') AS asset_code,
      e.raw_event_id,
      CAST(e.prop_user_id AS STRING) AS user_id
    FROM
      `my-project-8584-jetonai.decom.dwd_event_log` e
      CROSS JOIN UNNEST (
        IFNULL(
          JSON_QUERY_ARRAY(e.args, '$.asset_code'),
          ARRAY<JSON >[]
        )
      ) AS asset_code_json
    WHERE
      e.event_name IN ('v_home_feeds', 'v_closet_feeds')
      AND NULLIF(CAST(e.prop_user_id AS STRING), '') IS NOT NULL
      AND NULLIF(JSON_VALUE(asset_code_json), '') IS NOT NULL
  ),
  -- 素材曝光聚合层：PV 按原始事件去重，UV 按有效登录用户去重。
  exposure_agg AS (
    SELECT
      asset_code,
      COUNT(DISTINCT raw_event_id) AS exposure_pv,
      COUNT(DISTINCT user_id) AS exposure_uv
    FROM
      exposure_event_asset
    GROUP BY
      asset_code
  ),
  -- 素材点赞聚合层：按点赞用户去重，空用户不参与统计。
  like_agg AS (
    SELECT
      CAST(asset_code AS STRING) AS asset_code,
      COUNT(DISTINCT NULLIF(CAST(user_id AS STRING), '')) AS like_uv
    FROM
      `my-project-8584-jetonai.v3_decom.community_asset_like`
    WHERE
      NULLIF(CAST(asset_code AS STRING), '') IS NOT NULL
    GROUP BY
      asset_code
  )
  -- 最终结果：以素材主表为基表，保留没有曝光或点赞的素材。
SELECT
  a.asset_code,
  a.production_time_cn,
  COALESCE(e.exposure_pv, 0) AS exposure_pv,
  COALESCE(e.exposure_uv, 0) AS exposure_uv,
  COALESCE(l.like_uv, 0) AS like_uv
FROM
  asset_detail a
  LEFT JOIN exposure_agg e USING (asset_code)
  LEFT JOIN like_agg l USING (asset_code)
ORDER BY
  a.production_time_cn,
  a.asset_code;
