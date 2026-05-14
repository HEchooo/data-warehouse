from datetime import date, datetime, timedelta
import logging
from pathlib import Path
import sys
from zoneinfo import ZoneInfo

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

from google.cloud import bigquery
from google.cloud.exceptions import NotFound

from jobs.event_activity_rules import PASSIVE_ACTIVITY_EXCLUDED_EVENTS_SQL


PROJECT_ID = "my-project-8584-jetonai"
DATASET_ID = "decom"
V3_DATASET_ID = "v3_decom"
NEW_YORK_TZ = "America/New_York"
TABLE_NAME = "dws_column_interaction_monthly"
LOG_DIR = PROJECT_ROOT / "logs" / "dws" / TABLE_NAME
INCREMENTAL_WINDOW_HOURS = 6
MONTHLY_REBUILD_MONTHS = 2

COLUMN_EXPOSURE_EVENT_NAMES = (
    "v_star_post_detail",
    "v_magazine_post_detail",
    "v_brand_post_detail",
    "v_star_post_feeds",
    "v_brand_post_feeds",
)
PRODUCT_CLICK_EVENT_NAMES = (
    "c_go_product_detail",
    "c_open_sku_popup",
    "c_third_party_goods",
)
PRODUCT_CARD_EXPOSURE_EVENT_NAMES = (
    "v_shop_product",
    "v_post_product",
    "v_category_product",
    "v_creator_home_shop_product",
    "v_closet_feeds",
    "v_home_feeds",
    "v_shop_feeds",
    "v_tryon_result_detail",
)

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


def string_values_to_sql_list(values: tuple[str, ...]) -> str:
    return ", ".join([f"'{value}'" for value in values])


def first_day_of_month(value: date) -> date:
    return value.replace(day=1)


def clamp_recent_months(months: list[date]) -> list[date]:
    if not months:
        return []

    current_month = first_day_of_month(datetime.now(ZoneInfo(NEW_YORK_TZ)).date())
    allowed_months = set()
    cursor = current_month
    for _ in range(MONTHLY_REBUILD_MONTHS):
        allowed_months.add(cursor)
        cursor = first_day_of_month(cursor - timedelta(days=1))

    normalized_months = {first_day_of_month(month) for month in months}
    return sorted(month for month in normalized_months if month in allowed_months)


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


def clean_id_expr(field_name: str) -> str:
    """
    dwd_event_log 的 args_* 有时来自 JSON_EXTRACT，可能携带首尾双引号。
    例如：'"2602276189664"'。这里统一去掉首尾双引号并把空串归一为 NULL。
    """
    return f"NULLIF(NULLIF(TRIM(TRIM({field_name}), '\"'), ''), 'null')"


def module_expr(event_name_field: str = "event_name") -> str:
    star_id = clean_id_expr("args_star")
    magazine_id = clean_id_expr("args_magazine")
    brand_id = clean_id_expr("args_brand")
    return f"""
    CASE
        WHEN {event_name_field} IN ('v_star_post_detail', 'v_star_post_feeds') THEN 'star'
        WHEN {event_name_field} = 'v_magazine_post_detail' THEN 'magazine'
        WHEN {event_name_field} IN ('v_brand_post_detail', 'v_brand_post_feeds') THEN 'brand'
        WHEN {star_id} IS NOT NULL THEN 'star'
        WHEN {magazine_id} IS NOT NULL THEN 'magazine'
        WHEN {brand_id} IS NOT NULL THEN 'brand'
        ELSE NULL
    END
    """


def column_id_expr(event_name_field: str = "event_name") -> str:
    star_id = clean_id_expr("args_star")
    magazine_id = clean_id_expr("args_magazine")
    brand_id = clean_id_expr("args_brand")
    return f"""
    CASE
        WHEN {event_name_field} IN ('v_star_post_detail', 'v_star_post_feeds')
            THEN {star_id}
        WHEN {event_name_field} = 'v_magazine_post_detail' THEN {magazine_id}
        WHEN {event_name_field} IN ('v_brand_post_detail', 'v_brand_post_feeds')
            THEN {brand_id}
        ELSE COALESCE({star_id}, {magazine_id}, {brand_id})
    END
    """


def args_post_to_post_code_array_expr(field_name: str = "args_post") -> str:
    """
    统一把 args_post 解析成 post_code 数组：
    - 支持单值字符串和 JSON 数组
    - 过滤空串、"null"、[] 这类无效值
    """
    return f"""
    CASE
        WHEN {field_name} IS NULL
            OR TRIM({field_name}) IN ('', 'null', '[]', '""') THEN []
        WHEN STARTS_WITH(TRIM({field_name}), '[') THEN ARRAY(
            SELECT cleaned_post_code
            FROM (
                SELECT
                    NULLIF(
                        NULLIF(TRIM(TRIM(post_json), '"'), ''),
                        'null'
                    ) AS cleaned_post_code
                FROM UNNEST(JSON_EXTRACT_ARRAY({field_name}, '$')) AS post_json
            )
            WHERE cleaned_post_code IS NOT NULL
        )
        ELSE ARRAY(
            SELECT cleaned_post_code
            FROM (
                SELECT
                    NULLIF(
                        NULLIF(
                            COALESCE(
                                JSON_EXTRACT_SCALAR({field_name}, '$'),
                                TRIM(TRIM({field_name}), '"')
                            ),
                            ''
                        ),
                        'null'
                    ) AS cleaned_post_code
            )
            WHERE cleaned_post_code IS NOT NULL
        )
    END
    """


def get_column_interaction_months_to_process(
    window_hours: int = INCREMENTAL_WINDOW_HOURS,
) -> list[date]:
    """
    专栏月级行为事实依赖事件表、服务端事实表和维表修正，不复用日表简单汇总。

    规则：
    - 目标表不存在或为空时，按所有可能产出月报的自然月初始化
    - 目标表已存在时，只读取最近 6 小时上游变更映射出的受影响月份
    """
    event_ts = event_ts_expr()
    service_ts = service_ts_expr()
    is_full_snapshot = (not target_table_exists()) or get_target_row_count() == 0

    if is_full_snapshot:
        query = f"""
        WITH
        -- 事件层存在任意数据的自然月；用于支撑专栏曝光、商品事件和月活分母。
        event_months AS (
            SELECT DISTINCT DATE_TRUNC(DATE({event_ts}, '{NEW_YORK_TZ}'), MONTH) AS month
            FROM `{PROJECT_ID}.{DATASET_ID}.dwd_event_log`
        ),
        -- 点赞事实自然月。
        like_months AS (
            SELECT DISTINCT DATE_TRUNC(DATE({service_ts}, '{NEW_YORK_TZ}'), MONTH) AS month
            FROM `{PROJECT_ID}.{V3_DATASET_ID}.community_post_like`
        ),
        -- 新增关注事实自然月。
        follow_months AS (
            SELECT DISTINCT DATE_TRUNC(DATE({service_ts}, '{NEW_YORK_TZ}'), MONTH) AS month
            FROM `{PROJECT_ID}.{V3_DATASET_ID}.community_star_follow`
        ),
        -- 试穿事实自然月，只保留专栏报表使用的 TRY_ON / OUTFIT。
        tryon_months AS (
            SELECT DISTINCT DATE_TRUNC(DATE({service_ts}, '{NEW_YORK_TZ}'), MONTH) AS month
            FROM `{PROJECT_ID}.{V3_DATASET_ID}.common_ext_ai_task`
            WHERE task_type IN ('TRY_ON', 'OUTFIT')
        )
        SELECT DISTINCT month
        FROM (
            SELECT month FROM event_months
            UNION DISTINCT
            SELECT month FROM like_months
            UNION DISTINCT
            SELECT month FROM follow_months
            UNION DISTINCT
            SELECT month FROM tryon_months
        )
        WHERE month IS NOT NULL
        ORDER BY month
        """
        results = client.query(query).result()
        return clamp_recent_months([row.month for row in results])

    product_event_names_sql = string_values_to_sql_list(
        PRODUCT_CLICK_EVENT_NAMES + PRODUCT_CARD_EXPOSURE_EVENT_NAMES
    )

    query = f"""
    WITH
    -- 最近 6 小时变更的事件自然月；任何登录事件变化都可能影响月活分母。
    changed_event_months AS (
        SELECT DISTINCT DATE_TRUNC(DATE({event_ts}, '{NEW_YORK_TZ}'), MONTH) AS month
        FROM `{PROJECT_ID}.{DATASET_ID}.dwd_event_log`
        WHERE update_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {window_hours} HOUR)
    ),
    -- 最近 6 小时更新过的帖子维表键；用于反推商品、点赞、试穿受影响月份。
    changed_posts AS (
        SELECT DISTINCT
            post_code,
            CAST(post_id AS STRING) AS post_id
        FROM `{PROJECT_ID}.{DATASET_ID}.dim_post`
        WHERE update_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {window_hours} HOUR)
            AND post_code IS NOT NULL
            AND post_id IS NOT NULL
    ),
    -- 最近 6 小时更新过的专栏维表键；直接回推目标表中该专栏已有历史月份。
    changed_columns AS (
        SELECT DISTINCT column_id
        FROM `{PROJECT_ID}.{DATASET_ID}.dim_column`
        WHERE update_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {window_hours} HOUR)
            AND column_id IS NOT NULL
    ),
    -- 最近 6 小时服务端点赞事实自然月。
    changed_like_months AS (
        SELECT DISTINCT DATE_TRUNC(DATE({service_ts}, '{NEW_YORK_TZ}'), MONTH) AS month
        FROM `{PROJECT_ID}.{V3_DATASET_ID}.community_post_like`
        WHERE {service_ts} >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {window_hours} HOUR)
    ),
    -- 维表修正导致的历史点赞月份。
    changed_like_months_by_post AS (
        SELECT DISTINCT DATE_TRUNC(DATE({service_ts}, '{NEW_YORK_TZ}'), MONTH) AS month
        FROM `{PROJECT_ID}.{V3_DATASET_ID}.community_post_like` l
        JOIN changed_posts p
            ON CAST(l.post_id AS STRING) = p.post_id
    ),
    -- 最近 6 小时服务端新增关注自然月。
    changed_follow_months AS (
        SELECT DISTINCT DATE_TRUNC(DATE({service_ts}, '{NEW_YORK_TZ}'), MONTH) AS month
        FROM `{PROJECT_ID}.{V3_DATASET_ID}.community_star_follow`
        WHERE {service_ts} >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {window_hours} HOUR)
    ),
    -- 最近 6 小时试穿事实自然月。
    changed_tryon_months AS (
        SELECT DISTINCT DATE_TRUNC(DATE({service_ts}, '{NEW_YORK_TZ}'), MONTH) AS month
        FROM `{PROJECT_ID}.{V3_DATASET_ID}.common_ext_ai_task`
        WHERE {service_ts} >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {window_hours} HOUR)
            AND task_type IN ('TRY_ON', 'OUTFIT')
            AND NULLIF(JSON_VALUE(request_param, '$.postId'), '') IS NOT NULL
    ),
    -- 维表修正导致的历史试穿月份。
    changed_tryon_months_by_post AS (
        SELECT DISTINCT DATE_TRUNC(DATE({service_ts}, '{NEW_YORK_TZ}'), MONTH) AS month
        FROM `{PROJECT_ID}.{V3_DATASET_ID}.common_ext_ai_task` t
        JOIN changed_posts p
            ON NULLIF(JSON_VALUE(t.request_param, '$.postId'), '') = p.post_id
        WHERE t.task_type IN ('TRY_ON', 'OUTFIT')
    ),
    -- 维表修正导致的历史商品事件月份；商品事件只能通过 args_post 反推帖子。
    changed_product_event_months_by_post AS (
        SELECT DISTINCT DATE_TRUNC(DATE({event_ts}, '{NEW_YORK_TZ}'), MONTH) AS month
        FROM `{PROJECT_ID}.{DATASET_ID}.dwd_event_log` e
        CROSS JOIN UNNEST({args_post_to_post_code_array_expr("e.args_post")}) AS post_code
        JOIN changed_posts p
            ON post_code = p.post_code
        WHERE e.event_name IN ({product_event_names_sql})
            AND NULLIF(e.prop_user_id, '') IS NOT NULL
            AND NULLIF(e.product_code, '') IS NOT NULL
    )
    SELECT DISTINCT month
    FROM (
        SELECT month FROM changed_event_months
        UNION DISTINCT
        SELECT month FROM changed_like_months
        UNION DISTINCT
        SELECT month FROM changed_like_months_by_post
        UNION DISTINCT
        SELECT month FROM changed_follow_months
        UNION DISTINCT
        SELECT month FROM changed_tryon_months
        UNION DISTINCT
        SELECT month FROM changed_tryon_months_by_post
        UNION DISTINCT
        SELECT month FROM changed_product_event_months_by_post
        UNION DISTINCT
        SELECT DISTINCT t.month
        FROM `{target_table_id()}` t
        JOIN changed_columns c
            ON t.column_id = c.column_id
    )
    WHERE month IS NOT NULL
    ORDER BY month
    """
    results = client.query(query).result()
    return clamp_recent_months([row.month for row in results])


def run_dws_column_interaction_monthly(months: list[date]) -> None:
    """
    产出专栏主题月级核心行为事实。

    粒度：
    - month × module × column_id

    说明：
    - 月级 UV 和人数口径直接从原始行为事实去重，不从日表简单汇总
    - 终层率指标继续留在 ADS 计算
    """
    if not months:
        logging.info("没有新的 dws_column_interaction_monthly 月份需要处理")
        return

    months_str = dates_to_sql_list(months)
    event_ts = event_ts_expr()
    service_ts = service_ts_expr()
    module_sql = module_expr()
    column_id_sql = column_id_expr()
    column_exposure_events_sql = string_values_to_sql_list(COLUMN_EXPOSURE_EVENT_NAMES)
    product_click_events_sql = string_values_to_sql_list(PRODUCT_CLICK_EVENT_NAMES)
    product_card_exposure_events_sql = string_values_to_sql_list(
        PRODUCT_CARD_EXPOSURE_EVENT_NAMES
    )

    query = f"""
    DELETE FROM `{target_table_id()}`
    WHERE month IN ({months_str});

    INSERT INTO `{target_table_id()}`
    (
        month,
        module,
        column_id,
        column_exposure_uv,
        follow_total_count,
        follow_uv,
        product_click_uv,
        product_click_pv,
        product_card_exposure_uv,
        read_post_count,
        tryon_post_count,
        tryon_click_uv,
        like_post_count,
        like_uv,
        new_follow_count,
        follow_like_uv,
        mau_uv,
        update_time
    )
    WITH
    -- 当前批次要刷新的目标月份列表。
    target_months AS (
        SELECT DATE(month_str) AS month
        FROM UNNEST([{months_str}]) AS month_str
    ),
    -- 目标月份窗口边界，用于裁剪原始事件扫描范围。
    target_month_bounds AS (
        SELECT
            MIN(month) AS min_month,
            MAX(month) AS max_month
        FROM target_months
    ),
    -- 专栏维表映射：统一 column_id -> module，优先保留启用中的专栏。
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
    -- 帖子维表映射：统一 post_code / post_id -> column_id。
    post_dim AS (
        SELECT
            post_code,
            CAST(post_id AS STRING) AS post_id,
            column_id
        FROM `{PROJECT_ID}.{DATASET_ID}.dim_post`
        WHERE post_code IS NOT NULL
            AND post_id IS NOT NULL
            AND column_id IS NOT NULL
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY post_code
            ORDER BY status ASC, gmt_create DESC, post_id DESC
        ) = 1
    ),
    -- 登录事件基础层：统一时间和用户主体，供曝光、商品、关注、月活共用。
    base AS (
        SELECT
            DATE_TRUNC(DATE({event_ts}, '{NEW_YORK_TZ}'), MONTH) AS month,
            event_name,
            NULLIF(prop_user_id, '') AS user_id,
            raw_event_id,
            NULLIF(product_code, '') AS product_code,
            CAST(post_code AS STRING) AS post_code,
            args_post,
            args_star,
            args_magazine,
            args_brand
        FROM `{PROJECT_ID}.{DATASET_ID}.dwd_event_log`
        WHERE DATE({event_ts}, '{NEW_YORK_TZ}') BETWEEN (
                SELECT min_month
                FROM target_month_bounds
            ) AND (
                SELECT DATE_SUB(DATE_ADD(max_month, INTERVAL 1 MONTH), INTERVAL 1 DAY)
                FROM target_month_bounds
            )
            AND DATE_TRUNC(DATE({event_ts}, '{NEW_YORK_TZ}'), MONTH) IN (
                SELECT month
                FROM target_months
            )
            AND NULLIF(prop_user_id, '') IS NOT NULL
            AND event_name NOT IN ({PASSIVE_ACTIVITY_EXCLUDED_EVENTS_SQL})
    ),
    -- 月活分母：按任意已登录事件去重用户统计月活。
    mau_monthly AS (
        SELECT
            month,
            COUNT(DISTINCT user_id) AS mau_uv
        FROM base
        GROUP BY month
    ),
    -- 专栏曝光事件明细。
    exposure_events AS (
        SELECT
            month,
            user_id,
            post_code,
            {module_sql} AS module,
            {column_id_sql} AS column_id
        FROM base
        WHERE event_name IN ({column_exposure_events_sql})
    ),
    -- 按专栏聚合曝光 UV 与去重阅读帖子数。
    exposure_monthly AS (
        SELECT
            month,
            module,
            column_id,
            COUNT(DISTINCT user_id) AS column_exposure_uv,
            COUNT(
                DISTINCT IF(
                    post_code IS NOT NULL,
                    CONCAT(user_id, '-', post_code),
                    NULL
                )
            ) AS read_post_count
        FROM exposure_events
        WHERE module IS NOT NULL
            AND column_id IS NOT NULL
        GROUP BY month, module, column_id
    ),
    -- 商品事件基础层：只保留带 product_code 的商品曝光/点击事件。
    product_event_base AS (
        SELECT
            month,
            event_name,
            user_id,
            raw_event_id,
            product_code,
            args_post
        FROM base
        WHERE event_name IN (
                {product_click_events_sql},
                {product_card_exposure_events_sql}
            )
            AND product_code IS NOT NULL
    ),
    -- 从 args_post 解析帖子编码；商品事件只允许通过 args_post 做专栏映射。
    product_event_post_codes AS (
        SELECT
            e.month,
            e.event_name,
            e.user_id,
            e.raw_event_id,
            e.product_code,
            post_code
        FROM product_event_base e
        CROSS JOIN UNNEST({args_post_to_post_code_array_expr("e.args_post")}) AS post_code
    ),
    -- 商品事件按 args_post 补齐专栏维度；无法映射到帖子或专栏的事件直接丢弃。
    product_event_enriched AS (
        SELECT
            e.month,
            e.event_name,
            e.user_id,
            e.raw_event_id,
            e.product_code,
            c.module,
            p.column_id
        FROM product_event_post_codes e
        JOIN post_dim p
            ON e.post_code = p.post_code
        JOIN column_dim c
            ON p.column_id = c.column_id
    ),
    -- 商品点击事件先按 raw_event_id + 专栏去重，消除 dwd 拆商品多行带来的重复点击 PV。
    product_click_events_dedup AS (
        SELECT
            month,
            module,
            column_id,
            raw_event_id,
            ANY_VALUE(user_id) AS user_id
        FROM product_event_enriched
        WHERE event_name IN ({product_click_events_sql})
            AND raw_event_id IS NOT NULL
        GROUP BY month, module, column_id, raw_event_id
    ),
    -- 按专栏聚合商品点击 UV / PV。
    product_click_monthly AS (
        SELECT
            month,
            module,
            column_id,
            COUNT(DISTINCT user_id) AS product_click_uv,
            COUNT(*) AS product_click_pv
        FROM product_click_events_dedup
        GROUP BY month, module, column_id
    ),
    -- 按专栏聚合商品卡片曝光 UV。
    product_exposure_monthly AS (
        SELECT
            month,
            module,
            column_id,
            COUNT(DISTINCT user_id) AS product_card_exposure_uv
        FROM product_event_enriched
        WHERE event_name IN ({product_card_exposure_events_sql})
        GROUP BY month, module, column_id
    ),
    -- 服务端新增关注记录：关注相关指标统一改为服务端事实来源。
    follow_service_base AS (
        SELECT
            DATE_TRUNC(DATE({service_ts}, '{NEW_YORK_TZ}'), MONTH) AS month,
            c.module,
            CAST(f.star_user_id AS STRING) AS column_id,
            CAST(f.user_id AS STRING) AS user_id
        FROM `{PROJECT_ID}.{V3_DATASET_ID}.community_star_follow` f
        JOIN column_dim c
            ON c.column_id = CAST(f.star_user_id AS STRING)
        WHERE DATE_TRUNC(DATE({service_ts}, '{NEW_YORK_TZ}'), MONTH) IN (
                SELECT month
                FROM target_months
            )
            AND star_user_id IS NOT NULL
            AND CAST(star_user_id AS STRING) != ''
            AND user_id IS NOT NULL
            AND CAST(user_id AS STRING) != ''
    ),
    -- 按专栏聚合关注总次数、关注 UV 与新增关注用户数（同源服务端口径）。
    follow_service_monthly AS (
        SELECT
            month,
            module,
            column_id,
            COUNT(*) AS follow_total_count,
            COUNT(DISTINCT user_id) AS follow_uv,
            COUNT(DISTINCT user_id) AS new_follow_count
        FROM follow_service_base
        GROUP BY month, module, column_id
    ),
    -- 当月试穿任务原始记录；试穿是月内发生指标，不走快照。
    tryon_task_base AS (
        SELECT
            DATE_TRUNC(DATE({service_ts}, '{NEW_YORK_TZ}'), MONTH) AS month,
            NULLIF(JSON_VALUE(request_param, '$.postId'), '') AS post_id,
            COALESCE(
                NULLIF(JSON_VALUE(request_param, '$.user_id'), ''),
                NULLIF(JSON_VALUE(request_param, '$.userId'), ''),
                NULLIF(user_id, '')
            ) AS tryon_user_id
        FROM `{PROJECT_ID}.{V3_DATASET_ID}.common_ext_ai_task`
        WHERE DATE_TRUNC(DATE({service_ts}, '{NEW_YORK_TZ}'), MONTH) IN (
                SELECT month
                FROM target_months
            )
            AND task_type IN ('TRY_ON', 'OUTFIT')
    ),
    -- 把试穿任务映射到帖子所属专栏。
    tryon_task_enriched AS (
        SELECT
            t.month,
            c.module,
            p.column_id,
            t.post_id,
            t.tryon_user_id
        FROM tryon_task_base t
        JOIN post_dim p
            ON t.post_id = p.post_id
        JOIN column_dim c
            ON p.column_id = c.column_id
        WHERE t.post_id IS NOT NULL
    ),
    -- 按专栏聚合试穿帖子数与试穿点击人数。
    tryon_monthly AS (
        SELECT
            month,
            module,
            column_id,
            COUNT(DISTINCT post_id) AS tryon_post_count,
            COUNT(DISTINCT IF(tryon_user_id IS NOT NULL, tryon_user_id, NULL)) AS tryon_click_uv
        FROM tryon_task_enriched
        GROUP BY month, module, column_id
    ),
    -- 点赞服务表原始记录。
    like_service_base AS (
        SELECT
            DATE_TRUNC(DATE({service_ts}, '{NEW_YORK_TZ}'), MONTH) AS month,
            CAST(post_id AS STRING) AS post_id,
            CAST(user_id AS STRING) AS user_id
        FROM `{PROJECT_ID}.{V3_DATASET_ID}.community_post_like`
        WHERE DATE_TRUNC(DATE({service_ts}, '{NEW_YORK_TZ}'), MONTH) IN (
                SELECT month
                FROM target_months
            )
            AND post_id IS NOT NULL
            AND user_id IS NOT NULL
            AND CAST(user_id AS STRING) != ''
    ),
    -- 把点赞记录挂到帖子所属专栏。
    like_service_enriched AS (
        SELECT
            l.month,
            c.module,
            p.column_id,
            l.post_id,
            l.user_id
        FROM like_service_base l
        JOIN post_dim p
            ON l.post_id = p.post_id
        JOIN column_dim c
            ON p.column_id = c.column_id
    ),
    -- 按专栏聚合“获得点赞的帖子数”和点赞人数。
    like_monthly AS (
        SELECT
            month,
            module,
            column_id,
            COUNT(DISTINCT post_id) AS like_post_count,
            COUNT(DISTINCT user_id) AS like_uv
        FROM like_service_enriched
        GROUP BY month, module, column_id
    ),
    -- 按专栏聚合“新增关注用户 ∩ 点赞用户”的去重人数。
    follow_like_monthly AS (
        SELECT
            f.month,
            f.module,
            f.column_id,
            COUNT(DISTINCT f.user_id) AS follow_like_uv
        FROM follow_service_base f
        JOIN like_service_enriched l
            ON f.month = l.month
            AND f.module = l.module
            AND f.column_id = l.column_id
            AND f.user_id = l.user_id
        GROUP BY f.month, f.module, f.column_id
    ),
    -- 汇总所有可能产出指标的专栏主键。
    keys AS (
        SELECT DISTINCT month, module, column_id
        FROM exposure_monthly
        UNION DISTINCT
        SELECT DISTINCT month, module, column_id
        FROM follow_service_monthly
        UNION DISTINCT
        SELECT DISTINCT month, module, column_id
        FROM product_click_monthly
        UNION DISTINCT
        SELECT DISTINCT month, module, column_id
        FROM product_exposure_monthly
        UNION DISTINCT
        SELECT DISTINCT month, module, column_id
        FROM tryon_monthly
        UNION DISTINCT
        SELECT DISTINCT month, module, column_id
        FROM like_monthly
    )
    SELECT
        k.month,
        k.module,
        k.column_id,
        COALESCE(e.column_exposure_uv, 0) AS column_exposure_uv,
        COALESCE(fs.follow_total_count, 0) AS follow_total_count,
        COALESCE(fs.follow_uv, 0) AS follow_uv,
        COALESCE(pc.product_click_uv, 0) AS product_click_uv,
        COALESCE(pc.product_click_pv, 0) AS product_click_pv,
        COALESCE(pe.product_card_exposure_uv, 0) AS product_card_exposure_uv,
        COALESCE(e.read_post_count, 0) AS read_post_count,
        COALESCE(t.tryon_post_count, 0) AS tryon_post_count,
        COALESCE(t.tryon_click_uv, 0) AS tryon_click_uv,
        COALESCE(l.like_post_count, 0) AS like_post_count,
        COALESCE(l.like_uv, 0) AS like_uv,
        COALESCE(fs.new_follow_count, 0) AS new_follow_count,
        COALESCE(fl.follow_like_uv, 0) AS follow_like_uv,
        COALESCE(m.mau_uv, 0) AS mau_uv,
        CURRENT_TIMESTAMP() AS update_time
    FROM keys k
    LEFT JOIN exposure_monthly e
        ON k.month = e.month
        AND k.module = e.module
        AND k.column_id = e.column_id
    LEFT JOIN product_click_monthly pc
        ON k.month = pc.month
        AND k.module = pc.module
        AND k.column_id = pc.column_id
    LEFT JOIN product_exposure_monthly pe
        ON k.month = pe.month
        AND k.module = pe.module
        AND k.column_id = pe.column_id
    LEFT JOIN tryon_monthly t
        ON k.month = t.month
        AND k.module = t.module
        AND k.column_id = t.column_id
    LEFT JOIN like_monthly l
        ON k.month = l.month
        AND k.module = l.module
        AND k.column_id = l.column_id
    LEFT JOIN follow_service_monthly fs
        ON k.month = fs.month
        AND k.module = fs.module
        AND k.column_id = fs.column_id
    LEFT JOIN follow_like_monthly fl
        ON k.month = fl.month
        AND k.module = fl.module
        AND k.column_id = fl.column_id
    LEFT JOIN mau_monthly m
        ON k.month = m.month
    """

    logging.info("开始处理: dws_column_interaction_monthly")
    job = client.query(query)
    job.result()
    logging.info("dws_column_interaction_monthly 刷新完成, 处理月份: %s", months)


if __name__ == "__main__":
    log_path = configure_logging()
    logging.info("日志文件: %s", log_path)

    months = get_column_interaction_months_to_process()
    if not months:
        logging.info("没有新的 dws_column_interaction_monthly 月份需要处理")
    else:
        run_dws_column_interaction_monthly(months)
