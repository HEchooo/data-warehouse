"""ADS Makepod 独立站周报：支付用户订单分布，支撑【Makepod】平台看板新增 chart。

落地 ``decom.ads_weekly_makepod_paid_user_order``。

依赖：
- decom.dws_makepod_order_weekly（周 × 渠道 × 订单分组支付用户/订单/GMV 事实，
  已按渠道枚举 × 订单分组枚举补齐周骨架）

粒度：week_start_dt（周一起自然周） × channel × order_group

字段口径：
- 支付用户数/支付订单数/支付GMV：直接取自 dws_makepod_order_weekly
- 周环比 = SAFE_DIVIDE(本周 - 上周, 上周)，按 channel + order_group 分区、按周排序 LAG
  取相邻上一周；首周或上周为 0 时返回 NULL
- 用户数占比/GMV占比：分母固定为当周 channel='Overall' 且 order_group='全部分组' 的行
  （全站去重汇总，不分渠道也不分订单分组），不是各渠道或各分组相加
- 出口只保留有支付数据的 周 × 渠道 × 订单分组 组合（不做固定枚举补零展示行）
"""

from datetime import datetime, timezone
import logging
from pathlib import Path
import sys

from google.cloud import bigquery
from google.cloud.exceptions import NotFound


PROJECT_ID = "my-project-8584-jetonai"
DATASET_ID = "decom"
TABLE_NAME = "ads_weekly_makepod_paid_user_order"
SOURCE_TABLE_NAME = "dws_makepod_order_weekly"
PROJECT_ROOT = Path(__file__).resolve().parents[2]
LOG_DIR = PROJECT_ROOT / "logs" / "ads" / TABLE_NAME

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


def target_table_id() -> str:
    return f"{PROJECT_ID}.{DATASET_ID}.{TABLE_NAME}"


def source_table_id() -> str:
    return f"{PROJECT_ID}.{DATASET_ID}.{SOURCE_TABLE_NAME}"


def target_table_exists() -> bool:
    try:
        client.get_table(target_table_id())
        return True
    except NotFound:
        return False


def build_select_sql() -> str:
    return f"""
    WITH
    -- 源头周事实：已按 9 个渠道枚举 × 4 个订单分组枚举补齐周骨架，环比才能对齐到真实的相邻上一周。
    weekly_base AS (
        SELECT
            week_start_dt,
            channel,
            order_group,
            pay_user_count,
            pay_order_count,
            pay_gmv_usd
        FROM `{source_table_id()}`
    ),
    -- 按 渠道 + 订单分组 分区、按周排序取相邻上一周，用于计算周环比。
    weekly_with_wow AS (
        SELECT
            week_start_dt,
            channel,
            order_group,
            pay_user_count,
            pay_order_count,
            pay_gmv_usd,
            LAG(pay_user_count) OVER (
                PARTITION BY channel, order_group ORDER BY week_start_dt
            ) AS prev_pay_user_count,
            LAG(pay_order_count) OVER (
                PARTITION BY channel, order_group ORDER BY week_start_dt
            ) AS prev_pay_order_count,
            LAG(pay_gmv_usd) OVER (
                PARTITION BY channel, order_group ORDER BY week_start_dt
            ) AS prev_pay_gmv_usd
        FROM weekly_base
    ),
    -- 当周全站汇总：占比分母固定用 channel='Overall' 且 order_group='全部分组' 的行，
    -- 不是各渠道或各订单分组相加。
    weekly_overall AS (
        SELECT
            week_start_dt,
            pay_user_count AS overall_pay_user_count,
            pay_gmv_usd AS overall_pay_gmv_usd
        FROM weekly_base
        WHERE channel = 'Overall'
            AND order_group = '全部分组'
    )
    SELECT
        w.week_start_dt,
        CONCAT(
            FORMAT_DATE('%Y-%m-%d', w.week_start_dt),
            ' ~ ',
            FORMAT_DATE('%Y-%m-%d', DATE_ADD(w.week_start_dt, INTERVAL 6 DAY))
        ) AS week_range,
        w.channel,
        w.order_group,
        w.pay_user_count,
        SAFE_DIVIDE(
            w.pay_user_count - w.prev_pay_user_count,
            w.prev_pay_user_count
        ) AS pay_user_count_wow,
        w.pay_order_count,
        SAFE_DIVIDE(
            w.pay_order_count - w.prev_pay_order_count,
            w.prev_pay_order_count
        ) AS pay_order_count_wow,
        w.pay_gmv_usd,
        SAFE_DIVIDE(
            w.pay_gmv_usd - w.prev_pay_gmv_usd,
            w.prev_pay_gmv_usd
        ) AS pay_gmv_usd_wow,
        SAFE_DIVIDE(w.pay_user_count, o.overall_pay_user_count) AS pay_user_count_share,
        SAFE_DIVIDE(w.pay_gmv_usd, o.overall_pay_gmv_usd) AS pay_gmv_share,
        CURRENT_TIMESTAMP() AS update_time
    FROM weekly_with_wow w
    LEFT JOIN weekly_overall o
        USING (week_start_dt)
    -- 只保留有支付数据的 周 × 渠道 × 订单分组 组合，不做固定枚举补零展示行。
    WHERE NOT (
        w.pay_user_count = 0
        AND w.pay_order_count = 0
        AND w.pay_gmv_usd = 0
    )
    """


def run_ads_weekly_makepod_paid_user_order() -> None:
    target_table = target_table_id()
    run_id = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
    staging_table = f"{target_table}_tmp_{run_id}"
    target_dropped = False

    logging.info(
        "开始处理 %s, 目标表: %s, 临时表: %s",
        TABLE_NAME,
        target_table,
        staging_table,
    )

    try:
        client.query(
            f"""
            CREATE TABLE `{staging_table}`
            PARTITION BY week_start_dt
            CLUSTER BY channel AS
            {build_select_sql()}
            """
        ).result()

        validation_row = list(client.query(
            f"""
            SELECT
                COUNT(*) AS row_count,
                (
                    SELECT COUNT(*)
                    FROM (
                        SELECT week_start_dt, channel, order_group
                        FROM `{staging_table}`
                        GROUP BY week_start_dt, channel, order_group
                    )
                ) AS key_count
            FROM `{staging_table}`
            """
        ).result())[0]
        if validation_row.row_count != validation_row.key_count:
            raise ValueError(
                "ads_weekly_makepod_paid_user_order staging 主键校验失败: "
                f"row_count={validation_row.row_count}, key_count={validation_row.key_count}"
            )

        if target_table_exists():
            client.query(f"DROP TABLE `{target_table}`").result()
            target_dropped = True
            client.query(f"ALTER TABLE `{staging_table}` RENAME TO {TABLE_NAME}").result()
        else:
            client.query(
                f"""
                CREATE TABLE `{target_table}`
                PARTITION BY week_start_dt
                CLUSTER BY channel AS
                SELECT *
                FROM `{staging_table}`
                """
            ).result()
            client.query(f"DROP TABLE IF EXISTS `{staging_table}`").result()
    except Exception:
        if not target_dropped:
            client.query(f"DROP TABLE IF EXISTS `{staging_table}`").result()
        raise

    logging.info("%s 刷新完成", TABLE_NAME)


if __name__ == "__main__":
    log_path = configure_logging()
    logging.info("日志文件: %s", log_path)

    start_time = datetime.now(timezone.utc)
    run_ads_weekly_makepod_paid_user_order()
    elapsed = (datetime.now(timezone.utc) - start_time).total_seconds()
    logging.info("%s 完成, 耗时: %.1f 秒", TABLE_NAME, elapsed)
