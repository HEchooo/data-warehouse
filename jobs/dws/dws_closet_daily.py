from datetime import date, datetime, timezone
import logging
from pathlib import Path
import sys

from google.cloud import bigquery
from google.cloud.exceptions import NotFound


PROJECT_ID = "my-project-8584-jetonai"
DATASET_ID = "decom"
V3_DATASET_ID = "v3_decom"
TORONTO_TZ = "America/Toronto"
TABLE_NAME = "dws_closet_daily"
PROJECT_ROOT = Path(__file__).resolve().parents[2]
LOG_DIR = PROJECT_ROOT / "logs" / "dws" / TABLE_NAME
INCREMENTAL_WINDOW_HOURS = 6

client = bigquery.Client(project=PROJECT_ID)


def configure_logging() -> Path:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    log_path = LOG_DIR / f"{datetime.now().strftime('%Y-%m-%d')}_{TABLE_NAME}.log"

    logger = logging.getLogger()
    logger.handlers.clear()
    logger.setLevel(logging.INFO)

    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")

    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

    file_handler = logging.FileHandler(log_path, encoding="utf-8")
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    return log_path


def dates_to_sql_list(dates: list[date]) -> str:
    return ", ".join([f"'{d}'" for d in dates])


def target_table_id() -> str:
    return f"{PROJECT_ID}.{DATASET_ID}.{TABLE_NAME}"


def target_table_exists() -> bool:
    try:
        client.get_table(target_table_id())
        return True
    except NotFound:
        return False


def get_target_row_count() -> int:
    if not target_table_exists():
        return 0

    query = f"""
    SELECT COUNT(*) AS row_count
    FROM `{target_table_id()}`
    """
    rows = list(client.query(query).result())
    return int(rows[0].row_count) if rows else 0


def event_ts_expr() -> str:
    """
    将 logAt_timestamp 先按 prop_timezone 解释为真实时间，再转成 UTC TIMESTAMP。
    logAt_timestamp 存的是“本地时间字面值”，不能直接当 UTC 使用。
    """
    return (
        "TIMESTAMP("
        "DATETIME(logAt_timestamp, 'UTC'), "
        "COALESCE(NULLIF(prop_timezone, ''), 'UTC')"
        ")"
    )


def service_ts_expr(field_name: str = "gmt_create") -> str:
    """v3 服务表的 gmt_create 是 UTC DATETIME，这里统一转成 TIMESTAMP。"""
    return f"TIMESTAMP({field_name}, 'UTC')"


def service_change_ts_expr() -> str:
    """
    Datastream 源表优先用 source_timestamp 判断最近变更；
    缺失时回退到 gmt_update / gmt_create，避免漏掉最近同步的新行。
    """
    return (
        "COALESCE("
        "TIMESTAMP_MILLIS(datastream_metadata.source_timestamp), "
        "TIMESTAMP(COALESCE(gmt_update, gmt_create), 'UTC')"
        ")"
    )


def get_closet_daily_dates_to_process(
    window_hours: int = INCREMENTAL_WINDOW_HOURS,
) -> list[date]:
    """
    衣橱主题 DWS 同时依赖事件曝光、登录用户日事实和服务端衣橱明细。

    规则：
    - 目标表不存在或为空时，按衣橱主题首个业务日起全量初始化到今天
    - 目标表已存在时，取最近 6 小时受影响的最早业务日起，重算到今天
      以覆盖累计总计使用用户这类向后传播指标
    """
    event_ts = event_ts_expr()
    service_ts = service_ts_expr()
    service_change_ts = service_change_ts_expr()
    is_full_snapshot = (not target_table_exists()) or get_target_row_count() == 0

    if is_full_snapshot:
        query = f"""
        WITH
        -- 衣橱主题首个业务日：取首页曝光与衣橱服务端创建的并集最早日期。
        source_bounds AS (
            SELECT MIN(dt) AS min_dt
            FROM (
                SELECT DATE({event_ts}, '{TORONTO_TZ}') AS dt
                FROM `{PROJECT_ID}.{DATASET_ID}.dwd_event_log`
                WHERE event_name = 'v_closet'
                    AND prop_user_id IS NOT NULL
                    AND prop_user_id != ''
                    AND DATE({event_ts}, '{TORONTO_TZ}') IS NOT NULL
                UNION ALL
                SELECT DATE({service_ts}, '{TORONTO_TZ}') AS dt
                FROM `{PROJECT_ID}.{V3_DATASET_ID}.user_fashion_closet`
                WHERE user_id IS NOT NULL
                    AND user_id != ''
                    AND type IN ('SINGLE', 'OUTFIT')
                    AND DATE({service_ts}, '{TORONTO_TZ}') IS NOT NULL
            )
        )
        SELECT dt
        FROM source_bounds,
        UNNEST(
            IF(
                min_dt IS NULL,
                ARRAY<DATE>[],
                GENERATE_DATE_ARRAY(min_dt, CURRENT_DATE('{TORONTO_TZ}'))
            )
        ) AS dt
        ORDER BY dt
        """
        results = client.query(query).result()
        return [row.dt for row in results]

    query = f"""
    WITH
    -- 最近 6 小时有变化的衣橱首页曝光业务日。
    changed_exposure_dates AS (
        SELECT DISTINCT DATE({event_ts}, '{TORONTO_TZ}') AS dt
        FROM `{PROJECT_ID}.{DATASET_ID}.dwd_event_log`
        WHERE update_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {window_hours} HOUR)
            AND event_name = 'v_closet'
            AND prop_user_id IS NOT NULL
            AND prop_user_id != ''
            AND DATE({event_ts}, '{TORONTO_TZ}') IS NOT NULL
    ),
    -- 最近 6 小时有变化的登录用户业务日，影响渗透率分母。
    changed_login_dates AS (
        SELECT DISTINCT dt
        FROM `{PROJECT_ID}.{DATASET_ID}.dws_user_daily`
        WHERE update_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {window_hours} HOUR)
            AND dt IS NOT NULL
    ),
    -- 最近 6 小时有变化的衣橱首次使用维表日期，影响新增用户和累计用户。
    changed_first_use_dates AS (
        SELECT DISTINCT
            COALESCE(
                LEAST(first_single_dt, first_any_dt),
                first_single_dt,
                first_any_dt
            ) AS dt
        FROM `{PROJECT_ID}.{DATASET_ID}.dim_user_closet_first_use`
        WHERE update_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {window_hours} HOUR)
    ),
    -- 最近 6 小时有变化的衣橱服务端业务日。
    changed_service_dates AS (
        SELECT DISTINCT DATE({service_ts}, '{TORONTO_TZ}') AS dt
        FROM `{PROJECT_ID}.{V3_DATASET_ID}.user_fashion_closet`
        WHERE {service_change_ts} >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {window_hours} HOUR)
            AND user_id IS NOT NULL
            AND user_id != ''
            AND type IN ('SINGLE', 'OUTFIT')
            AND DATE({service_ts}, '{TORONTO_TZ}') IS NOT NULL
    ),
    -- 受影响最早业务日：累计指标从这里往后都要重算。
    changed_bounds AS (
        SELECT MIN(dt) AS min_dt
        FROM (
            SELECT dt FROM changed_exposure_dates
            UNION ALL
            SELECT dt FROM changed_login_dates
            UNION ALL
            SELECT dt FROM changed_first_use_dates
            UNION ALL
            SELECT dt FROM changed_service_dates
        )
    )
    SELECT dt
    FROM changed_bounds,
    UNNEST(
        IF(
            min_dt IS NULL,
            ARRAY<DATE>[],
            GENERATE_DATE_ARRAY(min_dt, CURRENT_DATE('{TORONTO_TZ}'))
        )
    ) AS dt
    ORDER BY dt
    """
    results = client.query(query).result()
    return [row.dt for row in results]


def run_dws_closet_daily(dates: list[date]) -> None:
    """
    产出衣橱概览主题日事实层。

    粒度：
    - dt

    指标：
    - closet_home_exposure_uv：衣橱首页曝光 UV
    - all_event_uv：全站当日已登录事件 UV，作为渗透率分母
    - new_closet_user_count：首次发生 SINGLE / OUTFIT 的新增衣橱使用用户数
    - new_item_count：当日新增 item 数量
    - total_closet_user_count：历史累计衣橱使用用户数
    - penetration_user_count：当日曝光且当日存在 SINGLE / OUTFIT 的交集用户数
    """
    if not dates:
        logging.info("没有新的 dws_closet_daily 日期需要处理")
        return

    dates_str = dates_to_sql_list(dates)
    start_date = min(dates)
    max_date = max(dates)
    event_ts = event_ts_expr()
    service_ts = service_ts_expr()

    query = f"""
    DELETE FROM `{target_table_id()}`
    WHERE dt IN ({dates_str});

    INSERT INTO `{target_table_id()}`
    (
        dt,
        closet_home_exposure_uv,
        all_event_uv,
        new_closet_user_count,
        new_item_count,
        total_closet_user_count,
        penetration_user_count,
        update_time
    )
    WITH
    -- 全量日期骨架：保留 0 值日期，并为累计指标提供窗口序列。
    calendar AS (
        SELECT dt
        FROM
        UNNEST(
            GENERATE_DATE_ARRAY(DATE '{start_date}', DATE '{max_date}')
        ) AS dt
    ),
    -- 当日衣橱首页曝光用户集合：仅保留已登录用户。
    closet_home_user_daily AS (
        SELECT DISTINCT
            DATE({event_ts}, '{TORONTO_TZ}') AS dt,
            prop_user_id AS user_id
        FROM `{PROJECT_ID}.{DATASET_ID}.dwd_event_log`
        WHERE event_name = 'v_closet'
            AND prop_user_id IS NOT NULL
            AND prop_user_id != ''
            AND DATE({event_ts}, '{TORONTO_TZ}')
                BETWEEN DATE '{start_date}' AND DATE '{max_date}'
    ),
    -- 当日衣橱首页曝光 UV。
    closet_home_metric_daily AS (
        SELECT
            dt,
            COUNT(DISTINCT user_id) AS closet_home_exposure_uv
        FROM closet_home_user_daily
        GROUP BY dt
    ),
    -- 全站当日已登录事件 UV：复用 dws_user_daily，避免重扫全量事件。
    all_event_metric_daily AS (
        SELECT
            dt,
            COUNT(DISTINCT prop_user_id) AS all_event_uv
        FROM `{PROJECT_ID}.{DATASET_ID}.dws_user_daily`
        WHERE dt BETWEEN DATE '{start_date}' AND DATE '{max_date}'
            AND prop_user_id IS NOT NULL
            AND prop_user_id != ''
        GROUP BY dt
    ),
    -- 当日衣橱服务端明细：统一纳入 SINGLE / OUTFIT。
    closet_service_daily AS (
        SELECT
            DATE({service_ts}, '{TORONTO_TZ}') AS dt,
            user_id,
            type,
            uuid,
            item_uuids
        FROM `{PROJECT_ID}.{V3_DATASET_ID}.user_fashion_closet`
        WHERE type IN ('SINGLE', 'OUTFIT')
            AND user_id IS NOT NULL
            AND user_id != ''
            AND DATE({service_ts}, '{TORONTO_TZ}')
                BETWEEN DATE '{start_date}' AND DATE '{max_date}'
    ),
    -- 首次衣橱使用维表：复用用户级首日，避免每次全扫服务端明细。
    first_use_dim AS (
        SELECT
            user_id,
            first_single_dt,
            first_any_dt
        FROM `{PROJECT_ID}.{DATASET_ID}.dim_user_closet_first_use`
        WHERE first_any_dt IS NOT NULL
            AND first_any_dt <= DATE '{max_date}'
    ),
    -- 当日衣橱服务端去重用户集合：SINGLE / OUTFIT 任一类型都算。
    closet_use_user_daily AS (
        SELECT DISTINCT
            dt,
            user_id
        FROM closet_service_daily
    ),
    -- 当日新增 item 数量：
    -- - SINGLE：按非空 uuid 记 1
    -- - OUTFIT：拆 item_uuids JSON 数组后计元素个数；异常空值元素会被过滤
    closet_item_metric_daily AS (
        SELECT
            dt,
            SUM(
                CASE
                    WHEN type = 'SINGLE' THEN
                        CASE
                            WHEN uuid IS NULL OR uuid = '' THEN 0
                            ELSE 1
                        END
                    WHEN type = 'OUTFIT' THEN
                        ARRAY_LENGTH(
                            ARRAY(
                                SELECT JSON_VALUE(item_uuid_json)
                                FROM UNNEST(
                                    COALESCE(
                                        JSON_QUERY_ARRAY(item_uuids, '$'),
                                        ARRAY<JSON>[]
                                    )
                                ) AS item_uuid_json
                                WHERE JSON_VALUE(item_uuid_json) IS NOT NULL
                                    AND JSON_VALUE(item_uuid_json) != ''
                            )
                        )
                    ELSE 0
                END
            ) AS new_item_count
        FROM closet_service_daily
        GROUP BY dt
    ),
    -- 新增衣橱使用用户：首次 SINGLE / OUTFIT 首日记 1 次。
    new_closet_user_metric_daily AS (
        SELECT
            first_any_dt AS dt,
            COUNT(*) AS new_closet_user_count
        FROM first_use_dim
        WHERE first_any_dt BETWEEN DATE '{start_date}' AND DATE '{max_date}'
        GROUP BY first_any_dt
    ),
    -- 窗口起点之前的累计存量基数：增量重算时要把历史存量带入。
    prior_total_user_base AS (
        SELECT
            COUNT(*) AS prior_total_closet_user_count
        FROM first_use_dim
        WHERE first_any_dt < DATE '{start_date}'
    ),
    -- 每日新增总计使用用户：供累计用户做 running sum。
    new_total_user_metric_daily AS (
        SELECT
            first_any_dt AS dt,
            COUNT(*) AS new_total_closet_user_count
        FROM first_use_dim
        WHERE first_any_dt BETWEEN DATE '{start_date}' AND DATE '{max_date}'
        GROUP BY first_any_dt
    ),
    -- 历史累计总计使用用户。
    total_closet_user_metric_daily AS (
        SELECT
            c.dt,
            b.prior_total_closet_user_count
            + SUM(COALESCE(n.new_total_closet_user_count, 0))
                OVER (ORDER BY c.dt) AS total_closet_user_count
        FROM calendar c
        CROSS JOIN prior_total_user_base b
        LEFT JOIN new_total_user_metric_daily n USING (dt)
    ),
    -- 当日渗透人数：同日进入衣橱首页且同日存在 SINGLE / OUTFIT 的交集用户。
    penetration_metric_daily AS (
        SELECT
            h.dt,
            COUNT(DISTINCT h.user_id) AS penetration_user_count
        FROM closet_home_user_daily h
        INNER JOIN closet_use_user_daily u
            ON h.dt = u.dt
            AND h.user_id = u.user_id
        GROUP BY h.dt
    )
    SELECT
        c.dt,
        COALESCE(h.closet_home_exposure_uv, 0) AS closet_home_exposure_uv,
        COALESCE(a.all_event_uv, 0) AS all_event_uv,
        COALESCE(n.new_closet_user_count, 0) AS new_closet_user_count,
        COALESCE(i.new_item_count, 0) AS new_item_count,
        COALESCE(t.total_closet_user_count, 0) AS total_closet_user_count,
        COALESCE(p.penetration_user_count, 0) AS penetration_user_count,
        CURRENT_TIMESTAMP() AS update_time
    FROM calendar c
    LEFT JOIN closet_home_metric_daily h USING (dt)
    LEFT JOIN all_event_metric_daily a USING (dt)
    LEFT JOIN new_closet_user_metric_daily n USING (dt)
    LEFT JOIN closet_item_metric_daily i USING (dt)
    LEFT JOIN total_closet_user_metric_daily t USING (dt)
    LEFT JOIN penetration_metric_daily p USING (dt)
    WHERE c.dt IN ({dates_str});
    """
    logging.info("开始处理: dws_closet_daily")
    job = client.query(query)
    job.result()
    logging.info("dws_closet_daily 刷新完成, 处理日期: %s", dates)


if __name__ == "__main__":
    log_path = configure_logging()
    logging.info("日志文件: %s", log_path)

    start_time = datetime.now(timezone.utc)
    dates = get_closet_daily_dates_to_process()
    logging.info("待处理日期: %s", dates if dates else "[]")

    if not dates:
        logging.info("没有新的 dws_closet_daily 日期需要处理")
        raise SystemExit(0)

    run_dws_closet_daily(dates)

    elapsed = (datetime.now(timezone.utc) - start_time).total_seconds()
    logging.info("dws_closet_daily 完成, 耗时: %.1f 秒", elapsed)
