from datetime import date, datetime
import logging
from pathlib import Path
import sys

from google.cloud import bigquery
from google.cloud.exceptions import NotFound


PROJECT_ID = "my-project-8584-jetonai"
DATASET_ID = "decom"
TORONTO_TZ = "America/Toronto"
TABLE_NAME = "dws_column_post_daily"
PROJECT_ROOT = Path(__file__).resolve().parents[2]
LOG_DIR = PROJECT_ROOT / "logs" / "dws" / TABLE_NAME

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


def get_column_post_dates_to_process() -> list[date]:
    """
    专栏帖子快照只补多伦多昨日。

    说明：
    - 这是“昨日 0 点后固化一次”的在线帖子存量快照，不做高频增量
    - 即使目标表为空，也只能初始化昨日；更早历史无法从当前在线状态准确回推
    """
    if not target_table_exists():
        query = f"""
        SELECT DATE_SUB(CURRENT_DATE('{TORONTO_TZ}'), INTERVAL 1 DAY) AS dt
        """
    else:
        query = f"""
        WITH
        -- 只取多伦多昨日作为快照候选日。
        candidate_dates AS (
            SELECT DATE_SUB(CURRENT_DATE('{TORONTO_TZ}'), INTERVAL 1 DAY) AS dt
        )
        SELECT c.dt
        FROM candidate_dates c
        LEFT JOIN `{target_table_id()}` t
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
    - 在线帖子定义：dim_post.status = 1
    - dt 表示被快照的业务日；需要在 America/Toronto 日界线的 0 点后执行
    """
    if not dates:
        logging.info("没有新的 dws_column_post_daily 日期需要处理")
        return

    dates_str = dates_to_sql_list(dates)

    query = f"""
    DELETE FROM `{target_table_id()}`
    WHERE dt IN ({dates_str});

    INSERT INTO `{target_table_id()}`
    (dt, module, column_id, post_total_count, update_time)
    WITH
    -- 目标快照业务日；该脚本在次日 0 点后执行，把当下在线帖子数写回昨日。
    target_dates AS (
        SELECT DATE(dt_str) AS dt
        FROM UNNEST([{dates_str}]) AS dt_str
    ),
    -- 专栏维表：统一 column_id -> module 映射，优先保留启用中的专栏。
    column_dim AS (
        SELECT
            column_id,
            module
        FROM `{PROJECT_ID}.{DATASET_ID}.dim_column`
        WHERE column_id IS NOT NULL
            AND module IS NOT NULL
            AND status = 0
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY column_id
            ORDER BY IF(status = 0, 0, 1), module, column_name
        ) = 1
    ),
    -- 当前在线帖子维表：只保留状态在线、且能映射到专栏的帖子主键。
    online_post_dim AS (
        SELECT
            post_id,
            column_id
        FROM `{PROJECT_ID}.{DATASET_ID}.dim_post`
        WHERE post_id IS NOT NULL
            AND column_id IS NOT NULL
            AND status = 1
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY post_id
            ORDER BY gmt_create DESC, post_code DESC
        ) = 1
    ),
    -- 按专栏聚合当前在线帖子存量。
    post_daily AS (
        SELECT
            t.dt,
            c.module,
            p.column_id,
            COUNT(DISTINCT p.post_id) AS post_total_count
        FROM target_dates t
        JOIN online_post_dim p
            ON TRUE
        JOIN column_dim c
            ON c.column_id = p.column_id
        GROUP BY t.dt, c.module, p.column_id
    )
    SELECT
        dt,
        module,
        column_id,
        post_total_count,
        CURRENT_TIMESTAMP() AS update_time
    FROM post_daily
    """

    logging.info("开始处理: dws_column_post_daily")
    job = client.query(query)
    job.result()
    logging.info("dws_column_post_daily 刷新完成, 处理日期: %s", dates)


if __name__ == "__main__":
    log_path = configure_logging()
    logging.info("日志文件: %s", log_path)

    dates = get_column_post_dates_to_process()
    if not dates:
        logging.info("没有新的 dws_column_post_daily 日期需要处理")
    else:
        run_dws_column_post_daily(dates)
