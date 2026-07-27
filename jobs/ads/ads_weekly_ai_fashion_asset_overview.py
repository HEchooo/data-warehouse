"""ADS 周级素材内容总览：【AI-Fashion】素材内容数据看板顶部周总览。

落地 ``decom.ads_weekly_ai_fashion_asset_overview``。

依赖：
- decom.dim_asset（素材模块归属、素材上线时间）
- decom.dws_asset_user_daily（素材曝光 / 试穿 / 负反馈用户事实）

统计对象固定为「本周新上线素材」：``dim_asset.publish_dt`` 落在本周的素材。
"""

from datetime import date, datetime, timezone
import logging
from pathlib import Path
import sys

from google.cloud import bigquery
from google.cloud.exceptions import NotFound


PROJECT_ID = "my-project-8584-jetonai"
DATASET_ID = "decom"
TORONTO_TZ = "America/Toronto"
TABLE_NAME = "ads_weekly_ai_fashion_asset_overview"
PROJECT_ROOT = Path(__file__).resolve().parents[2]
LOG_DIR = PROJECT_ROOT / "logs" / "ads" / TABLE_NAME
INCREMENTAL_WINDOW_HOURS = 6
WEEK_START_EXPR = "DATE_TRUNC({col}, WEEK(MONDAY))"
# 周总览只保留内容三模块，kol / video / other 不进本报表。
MODULES = ("brand", "magazine", "star")
# 有效曝光素材的周曝光 UV 阈值。
VALID_EXPOSURE_UV_THRESHOLD = 500

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


def dates_to_array_sql(dates: list[date]) -> str:
    return ", ".join([f"DATE '{d}'" for d in dates])


def string_values_to_sql_list(values: tuple[str, ...]) -> str:
    return ", ".join([f"'{v}'" for v in values])


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
    query = f"SELECT COUNT(*) AS row_count FROM `{target_table_id()}`"
    rows = list(client.query(query).result())
    return int(rows[0].row_count) if rows else 0


def get_week_starts_to_process(
    window_hours: int = INCREMENTAL_WINDOW_HOURS,
) -> list[date]:
    """
    周总览同时依赖 dim_asset（素材上线时间）和 dws_asset_user_daily（曝光/试穿/负反馈事实）。

    规则：
    - 目标表不存在或为空：按两张上游表的历史范围推算全部历史周
    - 目标表已存在：
      - dim_asset 最近变更映射出的 publish_dt 所在周需要重刷（新上线素材、模块归属变化）
      - dws_asset_user_daily 最近变更映射出的 dt 所在周需要重刷（新增曝光/试穿/负反馈）
      - 固定补当前周，保证 T+0 数据可见
    """
    week_start_from_publish = WEEK_START_EXPR.format(col="publish_dt")
    week_start_from_dt = WEEK_START_EXPR.format(col="dt")
    is_full_snapshot = (not target_table_exists()) or get_target_row_count() == 0

    if is_full_snapshot:
        query = f"""
        WITH
        -- dim_asset 历史素材上线周。
        asset_weeks AS (
            SELECT DISTINCT {week_start_from_publish} AS week_start_dt
            FROM `{PROJECT_ID}.{DATASET_ID}.dim_asset`
            WHERE publish_dt IS NOT NULL
        ),
        -- dws_asset_user_daily 历史事实周。
        fact_weeks AS (
            SELECT DISTINCT {week_start_from_dt} AS week_start_dt
            FROM `{PROJECT_ID}.{DATASET_ID}.dws_asset_user_daily`
            WHERE dt IS NOT NULL
        )
        SELECT DISTINCT week_start_dt
        FROM (
            SELECT week_start_dt FROM asset_weeks
            UNION DISTINCT
            SELECT week_start_dt FROM fact_weeks
        )
        WHERE week_start_dt IS NOT NULL
            -- 服务端存在已排期但尚未到来的未来发布素材，报表只统计到当前周为止。
            AND week_start_dt <= {WEEK_START_EXPR.format(col=f"CURRENT_DATE('{TORONTO_TZ}')")}
        ORDER BY week_start_dt
        """
        results = client.query(query).result()
        return [row.week_start_dt for row in results]

    query = f"""
    WITH
    -- 最近 6 小时 dim_asset 变更映射出的素材上线周。
    changed_asset_weeks AS (
        SELECT DISTINCT {week_start_from_publish} AS week_start_dt
        FROM `{PROJECT_ID}.{DATASET_ID}.dim_asset`
        WHERE update_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {window_hours} HOUR)
            AND publish_dt IS NOT NULL
    ),
    -- 最近 6 小时 dws_asset_user_daily 变更映射出的事实周。
    changed_fact_weeks AS (
        SELECT DISTINCT {week_start_from_dt} AS week_start_dt
        FROM `{PROJECT_ID}.{DATASET_ID}.dws_asset_user_daily`
        WHERE update_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {window_hours} HOUR)
            AND dt IS NOT NULL
    ),
    -- 固定补当前周，保证本周数据随事实滚动更新。
    current_week AS (
        SELECT {WEEK_START_EXPR.format(col=f"CURRENT_DATE('{TORONTO_TZ}')")} AS week_start_dt
    )
    SELECT DISTINCT week_start_dt
    FROM (
        SELECT week_start_dt FROM changed_asset_weeks
        UNION DISTINCT
        SELECT week_start_dt FROM changed_fact_weeks
        UNION DISTINCT
        SELECT week_start_dt FROM current_week
    )
    WHERE week_start_dt IS NOT NULL
        -- 服务端存在已排期但尚未到来的未来发布素材，报表只统计到当前周为止。
        AND week_start_dt <= {WEEK_START_EXPR.format(col=f"CURRENT_DATE('{TORONTO_TZ}')")}
    ORDER BY week_start_dt
    """
    results = client.query(query).result()
    return [row.week_start_dt for row in results]


def run(week_starts: list[date]) -> None:
    """
    素材内容周总览。

    粒度：
    - week_start_dt × module（brand / magazine / star）

    指标：
    - 本周新上线素材总数、获得曝光/有效曝光/样本不足/0 曝光素材数
    - 曝光 UV / 试穿 UV / 负反馈 UV（周内去重）、负反馈率
    """
    if not week_starts:
        logging.info("没有新的 %s 周需要处理", TABLE_NAME)
        return

    week_starts_str = dates_to_sql_list(week_starts)
    week_starts_array_sql = dates_to_array_sql(week_starts)
    modules_sql = string_values_to_sql_list(MODULES)

    query = f"""
    DELETE FROM `{target_table_id()}`
    WHERE week_start_dt IN ({week_starts_str});

    INSERT INTO `{target_table_id()}`
    (
        week_start_dt,
        week_range,
        module,
        new_asset_count,
        exposed_asset_count,
        valid_exposure_asset_count,
        insufficient_sample_asset_count,
        zero_exposure_asset_count,
        exposure_uv,
        tryon_uv,
        negative_feedback_uv,
        negative_feedback_rate,
        update_time
    )
    WITH
    -- 目标周骨架：week_start_dt × module。
    target_weeks AS (
        SELECT
            week_start_dt,
            DATE_ADD(week_start_dt, INTERVAL 6 DAY) AS week_end_dt
        FROM UNNEST([{week_starts_array_sql}]) AS week_start_dt
    ),
    week_module_spine AS (
        SELECT w.week_start_dt, w.week_end_dt, module
        FROM target_weeks w
        CROSS JOIN UNNEST([{modules_sql}]) AS module
    ),
    -- 本周新上线素材：素材上线时间落在目标周内，且模块属于 brand/magazine/star。
    new_assets AS (
        SELECT
            w.week_start_dt,
            w.week_end_dt,
            a.module,
            a.asset_code
        FROM `{PROJECT_ID}.{DATASET_ID}.dim_asset` a
        JOIN target_weeks w
            ON a.publish_dt BETWEEN w.week_start_dt AND w.week_end_dt
        WHERE a.module IN ({modules_sql})
    ),
    -- 本周新上线素材的用户级事实（曝光/试穿/负反馈），仅覆盖有行为的素材。
    fact_scoped AS (
        SELECT
            na.week_start_dt,
            na.module,
            na.asset_code,
            f.user_id,
            f.exposure_pv,
            f.tryon_count,
            f.negative_feedback_pv
        FROM new_assets na
        JOIN `{PROJECT_ID}.{DATASET_ID}.dws_asset_user_daily` f
            ON f.asset_code = na.asset_code
            AND f.dt BETWEEN na.week_start_dt AND na.week_end_dt
    ),
    -- 素材级周曝光 UV：用于有效曝光/样本不足/0 曝光三档分类。
    asset_week_exposure AS (
        SELECT
            week_start_dt,
            module,
            asset_code,
            COUNT(DISTINCT IF(exposure_pv > 0, user_id, NULL)) AS asset_exposure_uv
        FROM fact_scoped
        GROUP BY week_start_dt, module, asset_code
    ),
    asset_classification AS (
        SELECT
            na.week_start_dt,
            na.module,
            na.asset_code,
            COALESCE(e.asset_exposure_uv, 0) AS asset_exposure_uv
        FROM new_assets na
        LEFT JOIN asset_week_exposure e
            USING (week_start_dt, module, asset_code)
    ),
    -- 素材数三档分类聚合。
    module_week_asset_counts AS (
        SELECT
            week_start_dt,
            module,
            COUNT(*) AS new_asset_count,
            COUNTIF(asset_exposure_uv > 0) AS exposed_asset_count,
            COUNTIF(asset_exposure_uv > {VALID_EXPOSURE_UV_THRESHOLD}) AS valid_exposure_asset_count,
            COUNTIF(asset_exposure_uv > 0 AND asset_exposure_uv <= {VALID_EXPOSURE_UV_THRESHOLD})
                AS insufficient_sample_asset_count
        FROM asset_classification
        GROUP BY week_start_dt, module
    ),
    -- 模块级周用户指标：曝光/试穿/负反馈 UV 按周去重（不能由素材级 UV 求和）。
    module_week_user_metrics AS (
        SELECT
            week_start_dt,
            module,
            COUNT(DISTINCT IF(exposure_pv > 0, user_id, NULL)) AS exposure_uv,
            COUNT(DISTINCT IF(tryon_count > 0, user_id, NULL)) AS tryon_uv,
            COUNT(DISTINCT IF(negative_feedback_pv > 0, user_id, NULL)) AS negative_feedback_uv
        FROM fact_scoped
        GROUP BY week_start_dt, module
    )
    SELECT
        s.week_start_dt,
        CONCAT(
            FORMAT_DATE('%Y-%m-%d', s.week_start_dt), ' ~ ',
            FORMAT_DATE('%Y-%m-%d', s.week_end_dt)
        ) AS week_range,
        s.module,
        COALESCE(c.new_asset_count, 0) AS new_asset_count,
        COALESCE(c.exposed_asset_count, 0) AS exposed_asset_count,
        COALESCE(c.valid_exposure_asset_count, 0) AS valid_exposure_asset_count,
        COALESCE(c.insufficient_sample_asset_count, 0) AS insufficient_sample_asset_count,
        COALESCE(c.new_asset_count, 0) - COALESCE(c.exposed_asset_count, 0)
            AS zero_exposure_asset_count,
        COALESCE(u.exposure_uv, 0) AS exposure_uv,
        COALESCE(u.tryon_uv, 0) AS tryon_uv,
        COALESCE(u.negative_feedback_uv, 0) AS negative_feedback_uv,
        SAFE_DIVIDE(u.negative_feedback_uv, u.exposure_uv) AS negative_feedback_rate,
        CURRENT_TIMESTAMP() AS update_time
    FROM week_module_spine s
    LEFT JOIN module_week_asset_counts c
        USING (week_start_dt, module)
    LEFT JOIN module_week_user_metrics u
        USING (week_start_dt, module)
    ORDER BY s.week_start_dt, s.module;
    """
    logging.info("开始处理: %s", TABLE_NAME)
    client.query(query).result()
    logging.info("%s 刷新完成, 处理周: %s", TABLE_NAME, week_starts)


if __name__ == "__main__":
    log_path = configure_logging()
    logging.info("日志文件: %s", log_path)

    start_time = datetime.now(timezone.utc)
    week_starts = get_week_starts_to_process()
    logging.info("待处理周: %s", week_starts if week_starts else "[]")

    if not week_starts:
        logging.info("没有新的 %s 周需要处理", TABLE_NAME)
        raise SystemExit(0)

    run(week_starts)

    elapsed = (datetime.now(timezone.utc) - start_time).total_seconds()
    logging.info("%s 完成, 耗时: %.1f 秒", TABLE_NAME, elapsed)
