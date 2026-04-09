from datetime import date
import logging

from google.cloud import bigquery


PROJECT_ID = "my-project-8584-jetonai"
DATASET_ID = "decom"
V3_DATASET_ID = "v3_decom"
TORONTO_TZ = "America/Toronto"

client = bigquery.Client(project=PROJECT_ID)


def dates_to_sql_list(dates: list[date]) -> str:
    return ", ".join([f"'{d}'" for d in dates])


def get_column_post_dates_to_process() -> list[date]:
    """
    只补昨日快照。
    主 ETL 可能每小时触发，但帖子快照语义是“昨日固化一次”，
    因此只在目标表缺少昨日分区时返回该日期。
    """
    query = f"""
    WITH
    -- 只取多伦多昨日作为快照候选日。
    candidate_dates AS (
        SELECT DATE_SUB(CURRENT_DATE('{TORONTO_TZ}'), INTERVAL 1 DAY) AS dt
    )
    SELECT c.dt
    FROM candidate_dates c
    LEFT JOIN `{PROJECT_ID}.{DATASET_ID}.dws_column_post_daily` t
        ON t.dt = c.dt
    WHERE t.dt IS NULL
    ORDER BY c.dt
    """
    results = client.query(query).result()
    return [row.dt for row in results]


def run_dws_column_post_daily(dates: list[date]) -> None:
    """
    产出专栏维度的在线帖子日快照。

    粒度：
    - dt × module × column_id

    说明：
    - 该表只承载“昨日 0 点快照时点的在线帖子存量”
    - 在线帖子定义：v3_decom.community_post.status = 1
    - dt 表示被快照的业务日；需要在 America/Toronto 日界线的 0 点后由独立日任务执行
    """
    if not dates:
        logging.info("没有新的 dws_column_post_daily 日期需要处理")
        return

    dates_str = dates_to_sql_list(dates)

    query = f"""
    DELETE FROM `{PROJECT_ID}.{DATASET_ID}.dws_column_post_daily`
    WHERE dt IN ({dates_str});

    INSERT INTO `{PROJECT_ID}.{DATASET_ID}.dws_column_post_daily`
    (dt, module, column_id, post_total_count, update_time)
    WITH
    -- 目标快照业务日；该脚本应在次日 0 点后执行，把当下在线帖子数写回昨日。
    target_dates AS (
        SELECT DATE(dt_str) AS dt
        FROM UNNEST([{dates_str}]) AS dt_str
    ),
    -- 专栏维表：把 kol_type 映射成统一 module。
    column_map AS (
        SELECT
            CASE
                WHEN kol_type = 2 THEN 'star'
                WHEN kol_type = 3 THEN 'magazine'
                WHEN kol_type = 4 THEN 'brand'
                ELSE NULL
            END AS module,
            CAST(user_id AS STRING) AS column_id
        FROM `{PROJECT_ID}.{V3_DATASET_ID}.kol_rel`
        WHERE status = 0
            AND kol_type IN (2, 3, 4)
            AND user_id IS NOT NULL
        GROUP BY module, column_id
    ),
    -- 当前在线帖子；快照不感知创建时间，只看当前 status=1 的存量。
    online_post_base AS (
        SELECT
            CAST(id AS STRING) AS post_id,
            CAST(poster_user_id AS STRING) AS column_id
        FROM `{PROJECT_ID}.{V3_DATASET_ID}.community_post`
        WHERE status = 1
            AND id IS NOT NULL
            AND poster_user_id IS NOT NULL
            AND CAST(poster_user_id AS STRING) != ''
    ),
    -- 按专栏聚合当前在线帖子存量。
    post_daily AS (
        SELECT
            t.dt,
            cm.module,
            p.column_id,
            COUNT(DISTINCT p.post_id) AS post_total_count
        FROM target_dates t
        JOIN online_post_base p
            ON TRUE
        JOIN column_map cm
            ON cm.column_id = p.column_id
        GROUP BY t.dt, cm.module, p.column_id
    )
    SELECT
        dt,
        module,
        column_id,
        post_total_count,
        CURRENT_TIMESTAMP() AS update_time
    FROM post_daily;
    """

    logging.info("开始处理: dws_column_post_daily")
    job = client.query(query)
    job.result()
    logging.info(f"dws_column_post_daily 刷新完成, 处理日期: {dates}")


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
    )
    dates = get_column_post_dates_to_process()
    if not dates:
        logging.info("没有新的 dws_column_post_daily 日期需要处理")
    else:
        run_dws_column_post_daily(dates)
