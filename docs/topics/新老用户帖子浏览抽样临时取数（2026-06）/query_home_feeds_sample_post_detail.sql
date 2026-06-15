-- Home Feeds 样本用户对应日期的帖子详情浏览临时取数（2026-06）
-- 样本来源：0601-0607_homefeeds_analytics.csv 去重后的 browse_dt × user_group × browse_user_id。
-- 时间：2026-06-01 ~ 2026-06-07，按 America/Toronto 切天。
-- 粒度：浏览日期 × 用户分群 × 浏览用户 ID × 帖子 ID。
DECLARE toronto_tz STRING DEFAULT 'America/Toronto';

DECLARE shanghai_tz STRING DEFAULT 'Asia/Shanghai';

DECLARE start_dt DATE DEFAULT DATE '2026-06-01';

DECLARE end_dt DATE DEFAULT DATE '2026-06-07';

WITH
  -- 固定样本用户层：名单由 Home Feeds 结果 CSV 提取，每天每组 10 个用户，共 140 个日期用户键。
  sample_users AS (
    SELECT
      DATE(parts[OFFSET(0)]) AS browse_dt,
      parts[OFFSET(1)] AS user_group,
      parts[OFFSET(2)] AS browse_user_id
    FROM
      UNNEST (
        SPLIT(
          TRIM(
            """2026-06-01|新用户|2605316417296
2026-06-01|新用户|2606010440504
2026-06-01|新用户|2606011908552
2026-06-01|新用户|2606012208922
2026-06-01|新用户|2606014095339
2026-06-01|新用户|2606014256531
2026-06-01|新用户|2606015696202
2026-06-01|新用户|2606016823255
2026-06-01|新用户|2606019715023
2026-06-01|新用户|2606019820672
2026-06-01|老用户|2510162898468
2026-06-01|老用户|2511258165694
2026-06-01|老用户|2512159659015
2026-06-01|老用户|2604271009420
2026-06-01|老用户|2605194377916
2026-06-01|老用户|2605239055970
2026-06-01|老用户|2605241707896
2026-06-01|老用户|2605277332196
2026-06-01|老用户|2605311248478
2026-06-01|老用户|2605314234019
2026-06-02|新用户|2606015613624
2026-06-02|新用户|2606021342397
2026-06-02|新用户|2606021855904
2026-06-02|新用户|2606022695095
2026-06-02|新用户|2606024154717
2026-06-02|新用户|2606026077980
2026-06-02|新用户|2606026858195
2026-06-02|新用户|2606029070202
2026-06-02|新用户|2606029856391
2026-06-02|新用户|2606038761971
2026-06-02|老用户|2602144606922
2026-06-02|老用户|2604142118917
2026-06-02|老用户|2605194377916
2026-06-02|老用户|2605247932696
2026-06-02|老用户|2605259634714
2026-06-02|老用户|2605291163230
2026-06-02|老用户|2605309518622
2026-06-02|老用户|2605311014963
2026-06-02|老用户|2606014047621
2026-06-02|老用户|2606015814467
2026-06-03|新用户|2606025741764
2026-06-03|新用户|2606031267799
2026-06-03|新用户|2606033917828
2026-06-03|新用户|2606034053512
2026-06-03|新用户|2606034134011
2026-06-03|新用户|2606035843935
2026-06-03|新用户|2606036179289
2026-06-03|新用户|2606038498557
2026-06-03|新用户|2606038512161
2026-06-03|新用户|2606039791087
2026-06-03|老用户|2602048034348
2026-06-03|老用户|2605021047830
2026-06-03|老用户|2605157722995
2026-06-03|老用户|2605165186230
2026-06-03|老用户|2605172815427
2026-06-03|老用户|2605290096997
2026-06-03|老用户|2605294053504
2026-06-03|老用户|2605310645988
2026-06-03|老用户|2605315034123
2026-06-03|老用户|2606012522487
2026-06-04|新用户|2606038084713
2026-06-04|新用户|2606040789732
2026-06-04|新用户|2606041653399
2026-06-04|新用户|2606043014396
2026-06-04|新用户|2606045942805
2026-06-04|新用户|2606046640672
2026-06-04|新用户|2606047629576
2026-06-04|新用户|2606049267628
2026-06-04|新用户|2606049457885
2026-06-04|新用户|2606049639784
2026-06-04|老用户|2604250780501
2026-06-04|老用户|2605215834191
2026-06-04|老用户|2605229124377
2026-06-04|老用户|2605275023374
2026-06-04|老用户|2605292810534
2026-06-04|老用户|2605303067536
2026-06-04|老用户|2606020458588
2026-06-04|老用户|2606020782190
2026-06-04|老用户|2606032214303
2026-06-04|老用户|2606035383161
2026-06-05|新用户|2606050816154
2026-06-05|新用户|2606051963413
2026-06-05|新用户|2606053232324
2026-06-05|新用户|2606054325129
2026-06-05|新用户|2606054560140
2026-06-05|新用户|2606054609124
2026-06-05|新用户|2606056775848
2026-06-05|新用户|2606057076647
2026-06-05|新用户|2606057197799
2026-06-05|新用户|2606058867962
2026-06-05|老用户|2512174048741
2026-06-05|老用户|2602048034348
2026-06-05|老用户|2604210820391
2026-06-05|老用户|2605172833368
2026-06-05|老用户|2605239203787
2026-06-05|老用户|2606024179268
2026-06-05|老用户|2606037816428
2026-06-05|老用户|2606040361407
2026-06-05|老用户|2606046057876
2026-06-05|老用户|2606049583364
2026-06-06|新用户|2606060546208
2026-06-06|新用户|2606062872655
2026-06-06|新用户|2606062969842
2026-06-06|新用户|2606064688531
2026-06-06|新用户|2606065084571
2026-06-06|新用户|2606065561291
2026-06-06|新用户|2606066110509
2026-06-06|新用户|2606068562808
2026-06-06|新用户|2606068873959
2026-06-06|新用户|2606069525274
2026-06-06|老用户|2604050191087
2026-06-06|老用户|2605069377210
2026-06-06|老用户|2605185024584
2026-06-06|老用户|2605213568838
2026-06-06|老用户|2605239290796
2026-06-06|老用户|2605246467434
2026-06-06|老用户|2605250594927
2026-06-06|老用户|2605306071885
2026-06-06|老用户|2606019487263
2026-06-06|老用户|2606050069056
2026-06-07|新用户|2606068328868
2026-06-07|新用户|2606073678208
2026-06-07|新用户|2606074798274
2026-06-07|新用户|2606074904676
2026-06-07|新用户|2606075217412
2026-06-07|新用户|2606077376181
2026-06-07|新用户|2606077424152
2026-06-07|新用户|2606078536924
2026-06-07|新用户|2606079858516
2026-06-07|新用户|2606079960629
2026-06-07|老用户|2604106488719
2026-06-07|老用户|2604268872300
2026-06-07|老用户|2605149802871
2026-06-07|老用户|2605211597110
2026-06-07|老用户|2605230098173
2026-06-07|老用户|2605308893521
2026-06-07|老用户|2605309098468
2026-06-07|老用户|2606017445939
2026-06-07|老用户|2606058056453
2026-06-07|老用户|2606066261896"""
          ),
          '\n'
        )
      ) AS sample_line
      CROSS JOIN UNNEST ([STRUCT(SPLIT(sample_line, '|') AS parts)])
  ),
  -- 事件基础层：利用物理分区缩小扫描范围，再按埋点上报时区还原真实时间并切美东业务日。
  event_base AS (
    SELECT
      DATE(
        TIMESTAMP(
          DATETIME(logAt_timestamp, 'UTC'),
          COALESCE(NULLIF(prop_timezone, ''), 'UTC')
        ),
        toronto_tz
      ) AS browse_dt,
      TIMESTAMP(
        DATETIME(logAt_timestamp, 'UTC'),
        COALESCE(NULLIF(prop_timezone, ''), 'UTC')
      ) AS browse_ts,
      CAST(prop_user_id AS STRING) AS browse_user_id,
      CAST(post_code AS STRING) AS post_code,
      COALESCE(NULLIF(raw_event_id, ''), NULLIF(hash_id, '')) AS browse_event_id
    FROM
      `my-project-8584-jetonai.decom.dwd_event_log`
    WHERE
      DATE(logAt_timestamp) BETWEEN DATE_SUB(start_dt, INTERVAL 1 DAY) AND DATE_ADD(end_dt, INTERVAL 1 DAY)
      AND event_name IN (
        'v_star_post_detail',
        'v_magazine_post_detail',
        'v_brand_post_detail'
      )
      AND NULLIF(CAST(prop_user_id AS STRING), '') IS NOT NULL
      AND NULLIF(CAST(post_code AS STRING), '') IS NOT NULL
  ),
  -- 样本浏览明细：严格按样本中的对应日期和用户匹配详情浏览，并聚合到用户帖子粒度。
  sampled_browse_detail AS (
    SELECT
      e.browse_dt,
      s.user_group,
      e.browse_user_id,
      e.post_code,
      MIN(e.browse_ts) AS first_browse_ts,
      COUNT(DISTINCT e.browse_event_id) AS browse_count
    FROM
      event_base e
      INNER JOIN sample_users s ON s.browse_dt = e.browse_dt
      AND s.browse_user_id = e.browse_user_id
    WHERE
      e.browse_dt BETWEEN start_dt AND end_dt
    GROUP BY
      e.browse_dt,
      s.user_group,
      e.browse_user_id,
      e.post_code
  ),
  -- 帖子主数据层：同一 post_code 如有多条记录，保留最近更新且属于目标内容类型的一条。
  post_detail AS (
    SELECT
      CAST(post_code AS STRING) AS post_code,
      CAST(poster_user_id AS STRING) AS poster_user_id,
      CASE
        WHEN post_type IN (2, 4) THEN '明星'
        WHEN post_type = 5 THEN '杂志'
        WHEN post_type IN (6, 7) THEN '品牌'
      END AS content_type,
      CAST(creator AS STRING) AS creator,
      DATETIME(TIMESTAMP(gmt_create, 'UTC'), shanghai_tz) AS content_create_time_cn,
      DATETIME(TIMESTAMP(publish_time, 'UTC'), shanghai_tz) AS content_publish_time_cn,
      ARRAY(
        SELECT
          JSON_VALUE(asset, '$.assetCode')
        FROM
          UNNEST (
            IFNULL(
              JSON_QUERY_ARRAY(post_ext, '$.mainAssets'),
              ARRAY<JSON >[]
            )
          ) AS asset
        WITH
        OFFSET
          AS asset_offset
        WHERE
          NULLIF(JSON_VALUE(asset, '$.assetCode'), '') IS NOT NULL
        ORDER BY
          asset_offset
      ) AS asset_codes,
      ARRAY(
        SELECT
          COALESCE(JSON_VALUE(asset, '$.image.url'), '')
        FROM
          UNNEST (
            IFNULL(
              JSON_QUERY_ARRAY(post_ext, '$.mainAssets'),
              ARRAY<JSON >[]
            )
          ) AS asset
        WITH
        OFFSET
          AS asset_offset
        WHERE
          NULLIF(JSON_VALUE(asset, '$.assetCode'), '') IS NOT NULL
        ORDER BY
          asset_offset
      ) AS post_image_urls
    FROM
      `my-project-8584-jetonai.v3_decom.community_post`
    WHERE
      post_code IS NOT NULL
      AND post_type IN (2, 4, 5, 6, 7)
    QUALIFY
      ROW_NUMBER() OVER (
        PARTITION BY
          CAST(post_code AS STRING)
        ORDER BY
          TIMESTAMP(COALESCE(gmt_update, gmt_create), 'UTC') DESC,
          id DESC
      ) = 1
  )
  -- 最终结果：只输出样本用户在对应日期浏览的明星、杂志和品牌帖子。
SELECT
  b.browse_dt,
  b.user_group,
  b.browse_user_id,
  b.post_code,
  p.poster_user_id,
  p.asset_codes,
  p.content_type,
  p.creator,
  p.content_create_time_cn,
  p.content_publish_time_cn,
  DATETIME(b.first_browse_ts, toronto_tz) AS first_browse_time_et,
  b.browse_count,
  p.post_image_urls
FROM
  sampled_browse_detail b
  INNER JOIN post_detail p ON p.post_code = b.post_code
ORDER BY
  b.browse_dt,
  b.user_group,
  b.browse_user_id,
  b.first_browse_ts,
  b.post_code;
