-- 【Makepod】高价值用户监控：支付订单数分布临时取数
-- 统计范围：三个自然周
--   2026-06-28 ~ 2026-07-04
--   2026-07-05 ~ 2026-07-11
--   2026-07-12 ~ 2026-07-18
-- 粒度：每个 周 × 订单数分桶 × 渠道 一行。
-- 口径对齐 jobs/dws/dws_makepod_order_daily.py：pay_status = 4，同一 order_id 只保留最新支付记录，
-- pay_amount + pay_fee 按 currency_conversion_dict 换算 USD，用户渠道来自 dim_makepod_user_channel，未命中回退 Organic。
WITH
  -- 统计周区间：与业务方确认后的三个自然周（第三周由 07.02-07.18 更正为顺延的 07.12-07.18）。
  week_range AS (
    SELECT
      *
    FROM
      UNNEST (
        [
          STRUCT(
            '2026.06.28-07.04' AS week_label,
            DATE '2026-06-28' AS week_start,
            DATE '2026-07-04' AS week_end
          ),
          STRUCT(
            '2026.07.05-07.11',
            DATE '2026-07-05',
            DATE '2026-07-11'
          ),
          STRUCT(
            '2026.07.12-07.18',
            DATE '2026-07-12',
            DATE '2026-07-18'
          )
        ]
      )
  ),
  -- 汇率字典：按币种统一映射到 USD。
  currency_rate_dim AS (
    SELECT
      UPPER(from_currency) AS from_currency,
      MAX(exchange_rate) AS usd_rate
    FROM
      `my-project-8584-jetonai.decom.currency_conversion_dict`
    WHERE
      UPPER(to_currency) = 'USD'
    GROUP BY
      1
  ),
  -- 用户渠道维表：与 ads_daily_makepod 保持一致的渠道归属。
  user_channel_dim AS (
    SELECT
      service_user_id,
      channel
    FROM
      `my-project-8584-jetonai.decom.dim_makepod_user_channel`
    WHERE
      NULLIF(service_user_id, '') IS NOT NULL
  ),
  -- 支付成功订单：同一 order_id 只保留最新一条支付记录，避免金额重复放大。
  paid_order_fact AS (
    SELECT
      * EXCEPT (row_num)
    FROM
      (
        SELECT
          CAST(p.order_id AS STRING) AS order_id,
          CAST(p.user_id AS STRING) AS user_id,
          DATE(TIMESTAMP(p.pay_time, 'UTC'), 'Asia/Shanghai') AS pay_date,
          p.pay_amount,
          p.pay_fee,
          p.currency_type,
          ROW_NUMBER() OVER (
            PARTITION BY
              CAST(p.order_id AS STRING)
            ORDER BY
              COALESCE(
                TIMESTAMP_MILLIS(p.datastream_metadata.source_timestamp),
                TIMESTAMP(p.gmt_update, 'UTC'),
                TIMESTAMP(p.gmt_create, 'UTC')
              ) DESC,
              p.id DESC
          ) AS row_num
        FROM
          `my-project-8584-jetonai.v3_decom.pay_order` p
        WHERE
          p.tenant_code = 'T18002707'
          AND p.pay_status = 4
          AND p.pay_time IS NOT NULL
          AND NULLIF(CAST(p.order_id AS STRING), '') IS NOT NULL
          AND NULLIF(CAST(p.user_id AS STRING), '') IS NOT NULL
      )
    WHERE
      row_num = 1
  ),
  -- 订单 GMV：pay_amount + pay_fee 按币种换算 USD，口径与 ads_daily_makepod.pay_gmv_usd 一致。
  paid_order_with_gmv AS (
    SELECT
      f.order_id,
      f.user_id,
      f.pay_date,
      (
        COALESCE(SAFE_CAST(f.pay_amount AS NUMERIC), 0) + COALESCE(SAFE_CAST(f.pay_fee AS NUMERIC), 0)
      ) * COALESCE(
        r.usd_rate,
        CASE
          WHEN UPPER(CAST(f.currency_type AS STRING)) = 'USD' THEN 1
          ELSE 0
        END
      ) AS gmv_usd
    FROM
      paid_order_fact f
      LEFT JOIN currency_rate_dim r ON UPPER(CAST(f.currency_type AS STRING)) = r.from_currency
  ),
  -- 按周关联订单，并打上用户渠道。
  order_in_week AS (
    SELECT
      w.week_label,
      o.order_id,
      o.user_id,
      o.gmv_usd,
      COALESCE(c.channel, 'Organic') AS channel
    FROM
      week_range w
      JOIN paid_order_with_gmv o ON o.pay_date BETWEEN w.week_start AND w.week_end
      LEFT JOIN user_channel_dim c ON o.user_id = c.service_user_id
  ),
  -- 用户 × 周维度聚合：一个用户在同一周内只属于一个渠道（沿用用户渠道维表口径）。
  user_week_agg AS (
    SELECT
      week_label,
      user_id,
      channel,
      COUNT(DISTINCT order_id) AS order_count,
      SUM(gmv_usd) AS gmv_usd
    FROM
      order_in_week
    GROUP BY
      1,
      2,
      3
  ),
  -- 订单数分桶：1单 / 2单 / 3单及以上。
  user_week_bucket AS (
    SELECT
      *,
      CASE
        WHEN order_count = 1 THEN '1单'
        WHEN order_count = 2 THEN '2单'
        ELSE '3单及以上'
      END AS order_bucket
    FROM
      user_week_agg
  )
  -- 分桶明细：周 × 分桶 × 渠道。
SELECT
  week_label,
  order_bucket,
  channel,
  COUNT(DISTINCT user_id) AS pay_user_count,
  SUM(order_count) AS pay_order_count,
  ROUND(SUM(gmv_usd), 2) AS pay_gmv_usd
FROM
  user_week_bucket
GROUP BY
  1,
  2,
  3
ORDER BY
  week_label,
  order_bucket,
  channel;
