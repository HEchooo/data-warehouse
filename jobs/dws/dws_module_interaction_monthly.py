from datetime import date, datetime
import logging
from pathlib import Path
import sys

from google.cloud import bigquery
from google.cloud.exceptions import NotFound


PROJECT_ID = "my-project-8584-jetonai"
DATASET_ID = "decom"
V3_DATASET_ID = "v3_decom"
NEW_YORK_TZ = "America/New_York"
TABLE_NAME = "dws_module_interaction_monthly"
PROJECT_ROOT = Path(__file__).resolve().parents[2]
LOG_DIR = PROJECT_ROOT / "logs" / "dws" / TABLE_NAME
INCREMENTAL_WINDOW_HOURS = 6

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


def get_module_interaction_months_to_process(
    window_hours: int = INCREMENTAL_WINDOW_HOURS,
) -> list[date]:
    """
    模块月级行为事实依赖事件表、服务端事实表和维表修正，不复用专栏月表简单汇总。

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
        -- 事件层存在任意数据的自然月；用于支撑模块曝光、商品事件和月活分母。
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
        -- 试穿事实自然月，只保留模块月报使用的 TRY_ON / OUTFIT。
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
        return [row.month for row in results]

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
    -- 最近 6 小时更新过的专栏维表键；用于回推已产出历史月份。
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
    ),
    -- 专栏维表修正导致的历史月份；借助已有专栏月表反推受影响月份。
    changed_months_by_column AS (
        SELECT DISTINCT month
        FROM `{PROJECT_ID}.{DATASET_ID}.dws_column_interaction_monthly`
        WHERE column_id IN (SELECT column_id FROM changed_columns)
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
        SELECT month FROM changed_months_by_column
    )
    WHERE month IS NOT NULL
    ORDER BY month
    """
    results = client.query(query).result()
    return [row.month for row in results]


def run_dws_module_interaction_monthly(months: list[date]) -> None:
    """
    产出模块主题月级核心行为事实。

    粒度：
    - month × module

    说明：
    - 月级 UV 和人数口径直接从原始行为事实去重，不从专栏月表简单汇总
    - 终层率指标继续留在 ADS 计算
    """
    if not months:
        logging.info("没有新的 dws_module_interaction_monthly 月份需要处理")
        return

    months_str = dates_to_sql_list(months)
    event_ts = event_ts_expr()
    service_ts = service_ts_expr()
    module_sql = module_expr()
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
    ),
    -- 月活分母：按任意已登录事件去重用户统计月活。
    mau_monthly AS (
        SELECT
            month,
            COUNT(DISTINCT user_id) AS mau_uv
        FROM base
        GROUP BY month
    ),
    -- 模块曝光事件明细。
    exposure_events AS (
        SELECT
            month,
            user_id,
            post_code,
            {module_sql} AS module
        FROM base
        WHERE event_name IN ({column_exposure_events_sql})
    ),
    -- 按模块聚合曝光 UV 与去重阅读帖子数。
    exposure_monthly AS (
        SELECT
            month,
            module,
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
        GROUP BY month, module
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
    -- 从 args_post 解析帖子编码；商品事件只允许通过 args_post 做模块映射。
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
    -- 商品事件按 args_post 补齐模块维度；无法映射到帖子或模块的事件直接丢弃。
    product_event_enriched AS (
        SELECT
            e.month,
            e.event_name,
            e.user_id,
            e.raw_event_id,
            e.product_code,
            c.module
        FROM product_event_post_codes e
        JOIN post_dim p
            ON e.post_code = p.post_code
        JOIN column_dim c
            ON p.column_id = c.column_id
    ),
    -- 商品点击事件先按 raw_event_id + 模块去重，消除 dwd 拆商品多行带来的重复点击 PV。
    product_click_events_dedup AS (
        SELECT
            month,
            module,
            raw_event_id,
            ANY_VALUE(user_id) AS user_id
        FROM product_event_enriched
        WHERE event_name IN ({product_click_events_sql})
            AND raw_event_id IS NOT NULL
        GROUP BY month, module, raw_event_id
    ),
    -- 按模块聚合商品点击 UV / PV。
    product_click_monthly AS (
        SELECT
            month,
            module,
            COUNT(DISTINCT user_id) AS product_click_uv,
            COUNT(*) AS product_click_pv
        FROM product_click_events_dedup
        GROUP BY month, module
    ),
    -- 按模块聚合商品卡片曝光 UV。
    product_exposure_monthly AS (
        SELECT
            month,
            module,
            COUNT(DISTINCT user_id) AS product_card_exposure_uv
        FROM product_event_enriched
        WHERE event_name IN ({product_card_exposure_events_sql})
        GROUP BY month, module
    ),
    -- c_follow 事件先按 raw_event_id 去重，保留模块级点击事实。
    follow_events_dedup AS (
        SELECT
            month,
            raw_event_id,
            ANY_VALUE(user_id) AS user_id,
            ANY_VALUE({module_sql}) AS module
        FROM base
        WHERE event_name = 'c_follow'
            AND raw_event_id IS NOT NULL
        GROUP BY month, raw_event_id
    ),
    -- 按模块聚合关注点击次数与关注点击 UV。
    follow_monthly AS (
        SELECT
            month,
            module,
            COUNT(*) AS follow_total_count,
            COUNT(DISTINCT user_id) AS follow_uv
        FROM follow_events_dedup
        WHERE module IS NOT NULL
        GROUP BY month, module
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
    -- 把试穿任务映射到帖子所属模块。
    tryon_task_enriched AS (
        SELECT
            t.month,
            c.module,
            t.post_id,
            t.tryon_user_id
        FROM tryon_task_base t
        JOIN post_dim p
            ON t.post_id = p.post_id
        JOIN column_dim c
            ON p.column_id = c.column_id
        WHERE t.post_id IS NOT NULL
    ),
    -- 按模块聚合试穿帖子数与试穿点击人数。
    tryon_monthly AS (
        SELECT
            month,
            module,
            COUNT(DISTINCT post_id) AS tryon_post_count,
            COUNT(DISTINCT IF(tryon_user_id IS NOT NULL, tryon_user_id, NULL)) AS tryon_click_uv
        FROM tryon_task_enriched
        GROUP BY month, module
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
    -- 把点赞记录挂到帖子所属模块。
    like_service_enriched AS (
        SELECT
            l.month,
            c.module,
            l.post_id,
            l.user_id
        FROM like_service_base l
        JOIN post_dim p
            ON l.post_id = p.post_id
        JOIN column_dim c
            ON p.column_id = c.column_id
    ),
    -- 按模块聚合“获得点赞的帖子数”和点赞人数。
    like_monthly AS (
        SELECT
            month,
            module,
            COUNT(DISTINCT post_id) AS like_post_count,
            COUNT(DISTINCT user_id) AS like_uv
        FROM like_service_enriched
        GROUP BY month, module
    ),
    -- 服务端新增关注记录。
    follow_service_base AS (
        SELECT
            DATE_TRUNC(DATE({service_ts}, '{NEW_YORK_TZ}'), MONTH) AS month,
            c.module,
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
    -- 按模块聚合新增关注用户数。
    follow_service_monthly AS (
        SELECT
            month,
            module,
            COUNT(DISTINCT user_id) AS new_follow_count
        FROM follow_service_base
        GROUP BY month, module
    ),
    -- 汇总所有可能产出指标的模块主键。
    keys AS (
        SELECT DISTINCT month, module
        FROM exposure_monthly
        UNION DISTINCT
        SELECT DISTINCT month, module
        FROM follow_monthly
        UNION DISTINCT
        SELECT DISTINCT month, module
        FROM product_click_monthly
        UNION DISTINCT
        SELECT DISTINCT month, module
        FROM product_exposure_monthly
        UNION DISTINCT
        SELECT DISTINCT month, module
        FROM tryon_monthly
        UNION DISTINCT
        SELECT DISTINCT month, module
        FROM like_monthly
        UNION DISTINCT
        SELECT DISTINCT month, module
        FROM follow_service_monthly
    )
    SELECT
        k.month,
        k.module,
        COALESCE(e.column_exposure_uv, 0) AS column_exposure_uv,
        COALESCE(f.follow_total_count, 0) AS follow_total_count,
        COALESCE(f.follow_uv, 0) AS follow_uv,
        COALESCE(pc.product_click_uv, 0) AS product_click_uv,
        COALESCE(pc.product_click_pv, 0) AS product_click_pv,
        COALESCE(pe.product_card_exposure_uv, 0) AS product_card_exposure_uv,
        COALESCE(e.read_post_count, 0) AS read_post_count,
        COALESCE(t.tryon_post_count, 0) AS tryon_post_count,
        COALESCE(t.tryon_click_uv, 0) AS tryon_click_uv,
        COALESCE(l.like_post_count, 0) AS like_post_count,
        COALESCE(l.like_uv, 0) AS like_uv,
        COALESCE(fs.new_follow_count, 0) AS new_follow_count,
        COALESCE(m.mau_uv, 0) AS mau_uv,
        CURRENT_TIMESTAMP() AS update_time
    FROM keys k
    LEFT JOIN exposure_monthly e
        ON k.month = e.month
        AND k.module = e.module
    LEFT JOIN follow_monthly f
        ON k.month = f.month
        AND k.module = f.module
    LEFT JOIN product_click_monthly pc
        ON k.month = pc.month
        AND k.module = pc.module
    LEFT JOIN product_exposure_monthly pe
        ON k.month = pe.month
        AND k.module = pe.module
    LEFT JOIN tryon_monthly t
        ON k.month = t.month
        AND k.module = t.module
    LEFT JOIN like_monthly l
        ON k.month = l.month
        AND k.module = l.module
    LEFT JOIN follow_service_monthly fs
        ON k.month = fs.month
        AND k.module = fs.module
    LEFT JOIN mau_monthly m
        ON k.month = m.month
    """

    logging.info("开始处理: dws_module_interaction_monthly")
    job = client.query(query)
    job.result()
    logging.info("dws_module_interaction_monthly 刷新完成, 处理月份: %s", months)


if __name__ == "__main__":
    log_path = configure_logging()
    logging.info("日志文件: %s", log_path)

    months = get_module_interaction_months_to_process()
    if not months:
        logging.info("没有新的 dws_module_interaction_monthly 月份需要处理")
    else:
        run_dws_module_interaction_monthly(months)
