"""DWS 联盟项目视频日事实 ETL。

橱窗号视频日骨架左连广告（tiktok_item_id）、ECN 首次归属 / 投流时间、RPA 成交。
只落可加分子分母、投流前件数、首次投放日和累计投放次数。量级小，全表重建。
"""

import argparse
from datetime import datetime
import logging
from zoneinfo import ZoneInfo

from google.cloud import bigquery
from google.cloud.exceptions import NotFound

PROJECT_ID = "my-project-8584-jetonai"
DATASET_ID = "decom"
VIDEO_SOURCE_DATASET_ID = "videos"
TORONTO_TZ = "America/Toronto"
TABLE_NAME = "dws_social_affiliate_project_video_daily"
AFFILIATE_USAGE_TYPE = "tiktok-affiliate"
AFFILIATE_CHANNEL = "tiktok"
RPA_SOURCE_TABLE_NAME = "tiktok_shop_rpa_video_stats_snapshots"

client = bigquery.Client(project=PROJECT_ID)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="构建联盟项目视频日事实表（dt × video_key），全表重建"
    )
    return parser.parse_args()


def table_id(table_name: str) -> str:
    return f"{PROJECT_ID}.{DATASET_ID}.{table_name}"


def target_table_exists() -> bool:
    try:
        client.get_table(table_id(TABLE_NAME))
        return True
    except NotFound:
        return False


def build_select_sql() -> str:
    return f"""
    WITH
    -- 橱窗号名单，取账号日快照最新分区。
    affiliate_creator AS (
        SELECT
            creator_id,
            account_handle,
            account_name
        FROM `{table_id("dws_social_creator_snapshot_daily")}`
        WHERE channel = '{AFFILIATE_CHANNEL}'
            AND usage_type = '{AFFILIATE_USAGE_TYPE}'
            AND dt = (
                SELECT MAX(dt)
                FROM `{table_id("dws_social_creator_snapshot_daily")}`
                WHERE channel = '{AFFILIATE_CHANNEL}'
            )
    ),
    -- 橱窗号视频。绑定前存量视频已在 dim_video 排除。
    affiliate_video AS (
        SELECT
            v.creator_id,
            v.video_id,
            v.video_key,
            v.channel,
            v.published_dt,
            v.video_link,
            a.account_name,
            a.account_handle
        FROM `{table_id("dim_video")}` v
        INNER JOIN affiliate_creator a
            ON v.creator_id = a.creator_id
        WHERE v.channel = '{AFFILIATE_CHANNEL}'
    ),
    -- 日历骨架：有播放状态的日子，并上有广告的日子。
    date_spine AS (
        SELECT DISTINCT
            s.dt,
            v.video_key
        FROM `{table_id("dws_social_video_state_daily_v2")}` s
        INNER JOIN affiliate_video v
            ON s.video_key = v.video_key
        WHERE s.channel = '{AFFILIATE_CHANNEL}'
            AND s.dt < CURRENT_DATE('{TORONTO_TZ}')
            AND s.dt >= v.published_dt

        UNION DISTINCT

        SELECT DISTINCT
            ad.dt,
            v.video_key
        FROM `{table_id("ods_tiktok_ad_daily")}` ad
        INNER JOIN affiliate_video v
            ON v.video_id = ad.tiktok_item_id
        WHERE ad.tiktok_item_id IS NOT NULL
            AND ad.dt < CURRENT_DATE('{TORONTO_TZ}')
            AND ad.dt >= v.published_dt
    ),
    -- 广告按视频收口。多个广告打同一条视频时把可加指标加总。
    ad_daily AS (
        SELECT
            dt,
            tiktok_item_id AS video_id,
            TRUE AS has_ad,
            SUM(IFNULL(spend, 0)) AS spend,
            SUM(shop_gross_revenue_by_order_submission) AS ad_gmv,
            SUM(shop_total_items_purchased) AS ad_items,
            SUM(shop_total_purchase_by_order_submission) AS ad_purchases,
            SUM(impressions) AS impressions,
            SUM(clicks) AS clicks,
            SUM(video_play_actions) AS video_play_actions,
            SUM(onsite_on_web_detail) AS onsite_on_web_detail,
            ANY_VALUE(currency) AS ad_currency
        FROM `{table_id("ods_tiktok_ad_daily")}`
        WHERE tiktok_item_id IS NOT NULL
        GROUP BY dt, tiktok_item_id
    ),
    -- 有消耗的连续日历日算一次投放；中间断一天就算新的一次。
    ad_spend_days AS (
        SELECT DISTINCT
            tiktok_item_id AS video_id,
            dt
        FROM `{table_id("ods_tiktok_ad_daily")}`
        WHERE tiktok_item_id IS NOT NULL
            AND IFNULL(spend, 0) > 0
    ),
    ad_run_marked AS (
        SELECT
            video_id,
            dt,
            DATE_SUB(dt, INTERVAL ROW_NUMBER() OVER (PARTITION BY video_id ORDER BY dt) DAY) AS grp
        FROM ad_spend_days
    ),
    ad_runs AS (
        SELECT
            video_id,
            MIN(dt) AS run_start,
            DENSE_RANK() OVER (PARTITION BY video_id ORDER BY MIN(dt)) AS run_seq
        FROM ad_run_marked
        GROUP BY video_id, grp
    ),
    -- 本表全量重建，首次投放日取全历史最早有消耗日，不是窗口内 MIN。
    video_first_ad AS (
        SELECT
            video_id,
            MIN(run_start) AS first_ad_dt
        FROM ad_runs
        GROUP BY video_id
    ),
    -- 截止当天已经开始了几次投放。还没投过保持空。
    video_run_as_of AS (
        SELECT
            d.dt,
            v.video_id,
            MAX(r.run_seq) AS ad_run_cnt
        FROM date_spine d
        INNER JOIN affiliate_video v
            ON d.video_key = v.video_key
        LEFT JOIN ad_runs r
            ON r.video_id = v.video_id
            AND r.run_start <= d.dt
        GROUP BY d.dt, v.video_id
    ),
    -- RPA 一天多次抓取，按统计日 × 视频 × 商品保留最后一次；无商品名的行也留下。
    latest_rpa AS (
        SELECT * EXCEPT(row_num)
        FROM (
            SELECT
                stats_date,
                tiktok_video_id,
                product_title,
                currency,
                gmv,
                items_sold,
                fetched_at,
                id,
                ROW_NUMBER() OVER (
                    PARTITION BY stats_date, tiktok_video_id, IFNULL(product_title, '')
                    ORDER BY fetched_at DESC, id DESC
                ) AS row_num
            FROM `{PROJECT_ID}.{VIDEO_SOURCE_DATASET_ID}.{RPA_SOURCE_TABLE_NAME}`
            WHERE stats_date IS NOT NULL
                AND NULLIF(tiktok_video_id, '') IS NOT NULL
        )
        WHERE row_num = 1
    ),
    -- 视频当日成交，跨商品加总。
    rpa_daily AS (
        SELECT
            stats_date AS dt,
            tiktok_video_id AS video_id,
            ANY_VALUE(UPPER(currency)) AS rpa_currency,
            SUM(CAST(IFNULL(gmv, 0) AS FLOAT64)) AS rpa_gmv,
            SUM(IFNULL(items_sold, 0)) AS rpa_items
        FROM latest_rpa
        GROUP BY stats_date, tiktok_video_id
    ),
    -- 已投流视频：投流时间之前的 RPA 快照。
    rpa_before_eligible AS (
        SELECT
            s.tiktok_video_id AS video_id,
            s.product_title,
            s.items_sold,
            s.fetched_at,
            s.id
        FROM `{PROJECT_ID}.{VIDEO_SOURCE_DATASET_ID}.{RPA_SOURCE_TABLE_NAME}` s
        INNER JOIN `{table_id("ods_ecn_video_promotion_entry")}` e
            ON e.platform = '{AFFILIATE_CHANNEL}'
            AND e.video_id = s.tiktok_video_id
            AND e.ads_first_sent_at IS NOT NULL
        WHERE NULLIF(s.tiktok_video_id, '') IS NOT NULL
            AND CASE
                WHEN s.fetched_at IS NOT NULL
                    THEN TIMESTAMP(s.fetched_at, 'UTC') < e.ads_first_sent_at
                ELSE s.stats_date < DATE(e.ads_first_sent_at)
            END
    ),
    -- 每个商品取投流前最新一次，再把件数加总。
    rpa_before AS (
        SELECT
            video_id,
            SUM(IFNULL(items_sold, 0)) AS items_before_ads
        FROM (
            SELECT
                video_id,
                items_sold,
                ROW_NUMBER() OVER (
                    PARTITION BY video_id, IFNULL(product_title, '')
                    ORDER BY fetched_at DESC, id DESC
                ) AS row_num
            FROM rpa_before_eligible
        )
        WHERE row_num = 1
        GROUP BY video_id
    ),
    -- 播放 / 点赞日末状态，给增量和前一日对比用。
    video_state AS (
        SELECT
            s.dt,
            s.video_key,
            s.day_end_views,
            s.day_end_likes
        FROM `{table_id("dws_social_video_state_daily_v2")}` s
        INNER JOIN affiliate_video v
            ON s.video_key = v.video_key
        WHERE s.channel = '{AFFILIATE_CHANNEL}'
    )
    SELECT
        d.dt,
        v.channel,
        v.creator_id,
        v.account_name,
        v.account_handle,
        v.video_id,
        v.video_key,
        v.published_dt,
        v.video_link,
        aff.affiliation_label AS region,
        e.ads_first_sent_at,
        IFNULL(ad.has_ad, FALSE) AS has_ad,
        CASE
            WHEN fa.first_ad_dt IS NULL OR d.dt < fa.first_ad_dt THEN NULL
            ELSE fa.first_ad_dt
        END AS first_ad_dt,
        runs.ad_run_cnt,
        ad.spend,
        ad.ad_gmv,
        ad.ad_items,
        ad.ad_purchases,
        ad.impressions,
        ad.clicks,
        ad.video_play_actions,
        ad.onsite_on_web_detail,
        ad.ad_currency,
        r.rpa_gmv,
        r.rpa_items,
        r.rpa_currency,
        -- 只有 ECN 有投流时间才落投流前件数；没投流或没有投流前快照保持空，不拿当天 RPA 顶。
        b.items_before_ads,
        CASE
            WHEN cur.day_end_views IS NULL THEN NULL
            ELSE cur.day_end_views - COALESCE(prev.day_end_views, 0)
        END AS view_increment,
        CASE
            WHEN cur.day_end_likes IS NULL THEN NULL
            ELSE cur.day_end_likes - COALESCE(prev.day_end_likes, 0)
        END AS like_increment,
        CURRENT_TIMESTAMP() AS update_time
    FROM date_spine d
    INNER JOIN affiliate_video v
        ON d.video_key = v.video_key
    LEFT JOIN video_state cur
        ON cur.video_key = d.video_key
        AND cur.dt = d.dt
    LEFT JOIN video_state prev
        ON prev.video_key = d.video_key
        AND prev.dt = DATE_SUB(d.dt, INTERVAL 1 DAY)
    LEFT JOIN ad_daily ad
        ON ad.dt = d.dt
        AND ad.video_id = v.video_id
    LEFT JOIN video_first_ad fa
        ON fa.video_id = v.video_id
    LEFT JOIN video_run_as_of runs
        ON runs.dt = d.dt
        AND runs.video_id = v.video_id
    LEFT JOIN rpa_daily r
        ON r.dt = d.dt
        AND r.video_id = v.video_id
    LEFT JOIN `{table_id("dim_ecn_account_affiliation")}` aff
        ON aff.platform = '{AFFILIATE_CHANNEL}'
        AND LOWER(LTRIM(aff.account_handle, '@')) = LOWER(LTRIM(v.account_handle, '@'))
    LEFT JOIN `{table_id("ods_ecn_video_promotion_entry")}` e
        ON e.platform = '{AFFILIATE_CHANNEL}'
        AND e.video_id = v.video_id
    LEFT JOIN rpa_before b
        ON b.video_id = v.video_id
    """


def validate(staging_table: str) -> None:
    result = list(
        client.query(
            f"""
            SELECT
                COUNT(*) AS row_count,
                COUNT(DISTINCT CONCAT(CAST(dt AS STRING), '|', video_key)) AS unique_key_count,
                COUNTIF(dt IS NULL OR video_key IS NULL) AS null_key_count,
                COUNTIF(channel IS NULL OR channel = '') AS null_channel_count,
                COUNTIF(spend < 0 OR ad_gmv < 0 OR rpa_gmv < 0 OR ad_purchases < 0) AS negative_amount_count,
                COUNTIF(ad_items < 0 OR rpa_items < 0 OR items_before_ads < 0) AS negative_item_count,
                COUNTIF(
                    impressions < 0
                    OR clicks < 0
                    OR video_play_actions < 0
                    OR onsite_on_web_detail < 0
                ) AS negative_funnel_count,
                COUNTIF(IFNULL(spend, 0) > 0 AND first_ad_dt IS NULL) AS missing_first_ad_count,
                COUNTIF(IFNULL(spend, 0) > 0 AND IFNULL(ad_run_cnt, 0) < 1) AS missing_run_cnt_count
            FROM `{staging_table}`
            """
        ).result()
    )[0]
    if result.row_count == 0:
        raise RuntimeError("联盟项目视频事实校验失败：staging 无数据")
    if result.row_count != result.unique_key_count:
        raise RuntimeError("联盟项目视频事实校验失败：统计日和视频不唯一")
    if result.null_key_count:
        raise RuntimeError("联盟项目视频事实校验失败：主键字段存在空值")
    if result.null_channel_count:
        raise RuntimeError("联盟项目视频事实校验失败：channel 存在空值")
    if result.negative_amount_count:
        raise RuntimeError("联盟项目视频事实校验失败：存在负金额")
    if result.negative_item_count:
        raise RuntimeError("联盟项目视频事实校验失败：存在负件数")
    if result.negative_funnel_count:
        raise RuntimeError("联盟项目视频事实校验失败：存在负漏斗次数")
    if result.missing_first_ad_count:
        raise RuntimeError("联盟项目视频事实校验失败：有消耗的行缺首次投放日")
    if result.missing_run_cnt_count:
        raise RuntimeError("联盟项目视频事实校验失败：有消耗的行缺累计投放次数")


def run() -> None:
    target_table = table_id(TABLE_NAME)
    run_id = datetime.now(ZoneInfo(TORONTO_TZ)).strftime("%Y%m%d%H%M%S")
    staging_table = f"{target_table}_tmp_{run_id}"
    target_dropped = False

    try:
        client.query(
            f"""
            CREATE TABLE `{staging_table}`
            PARTITION BY dt
            CLUSTER BY channel, creator_id, video_key AS
            {build_select_sql()}
            """
        ).result()

        validate(staging_table)

        if target_table_exists():
            client.query(f"DROP TABLE `{target_table}`").result()
            target_dropped = True
        client.query(f"ALTER TABLE `{staging_table}` RENAME TO {TABLE_NAME}").result()
    except Exception:
        if target_dropped:
            logging.error("目标表已删除但换名未完成，staging 表保留待恢复: %s", staging_table)
        else:
            client.query(f"DROP TABLE IF EXISTS `{staging_table}`").result()
        raise

    logging.info("%s 刷新完成", TABLE_NAME)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
    )
    started_at = datetime.now(ZoneInfo(TORONTO_TZ))
    parse_args()
    run()
    logging.info(
        "%s 完成，耗时: %.1f 秒",
        TABLE_NAME,
        (datetime.now(ZoneInfo(TORONTO_TZ)) - started_at).total_seconds(),
    )
