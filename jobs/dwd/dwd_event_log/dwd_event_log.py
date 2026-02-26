import pandas as pd
import json
from google.cloud import bigquery
from datetime import datetime, timezone
import logging
import hashlib
import base64
import struct
from typing import Optional

# 记录脚本开始时间
start_time = datetime.now(timezone.utc)

# 配置日志记录
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

# 项目和数据集配置
PROJECT_ID = "my-project-8584-jetonai"
DATASET_ID = "decom"  # 数据库名为decom
client = bigquery.Client(project=PROJECT_ID)

# 数据集自动选择机制
V3_DECOM_TABLES = set()


def get_v3_decom_tables():
    """
    查询v3_decom数据集中的所有表名
    """
    try:
        query = "SELECT DISTINCT table_name FROM `v3_decom.INFORMATION_SCHEMA.TABLES`"
        results = client.query(query).result()
        return {row.table_name for row in results}
    except Exception as e:
        logging.warning(f"查询v3_decom表名失败: {e}，将使用decom数据集")
        return set()


def get_dataset_for_table(table_name, original_dataset="decom"):
    """
    判断表应该使用哪个数据集
    只处理 decom/v3_decom 的区分，其他数据集保持不变

    Args:
        table_name: 表名
        original_dataset: 原始数据集名称，默认为 "decom"

    Returns:
        如果原始数据集是 "decom" 且表存在于 v3_decom，返回 "v3_decom"
        否则返回原始数据集
    """
    if original_dataset == "decom" and table_name in V3_DECOM_TABLES:
        return "v3_decom"
    return original_dataset


# 初始化：查询v3_decom表名
logging.info(f"开始处理: dwd_event_log")
logging.info("正在初始化数据集选择机制...")
V3_DECOM_TABLES = get_v3_decom_tables()
logging.info(f"v3_decom数据集包含 {len(V3_DECOM_TABLES)} 个表")

# 动态获取各表的数据集
ds_user_login_ways = get_dataset_for_table("user_login_ways", "decom")
# ods层日志表为ods_event_log
ds_ods_event_log = get_dataset_for_table("ods_event_log", "decom")
ds_oss_key_process_log = get_dataset_for_table("oss_key_process_log", "decom")
ds_user_info = get_dataset_for_table("user_info", "decom")

# 修改查询语句,只处理未分析的数据
decom_initial_query = f"""
WITH domain_tenant_mapping AS (
    SELECT DISTINCT domain,
        tenant_code
    FROM `{PROJECT_ID}.{ds_user_login_ways}.user_login_ways`
    WHERE deleted = 0
        AND domain IS NOT NULL
        AND domain != ''
)
SELECT dtl.*,
    domain_tenant_mapping.tenant_code
FROM `{PROJECT_ID}.{ds_ods_event_log}.ods_event_log` dtl
    LEFT JOIN domain_tenant_mapping ON NET.HOST(dtl.prop_url) = domain_tenant_mapping.domain
WHERE dtl.oss_key IN (
        SELECT oss_key
        FROM `{PROJECT_ID}.{ds_oss_key_process_log}.oss_key_process_log`
        WHERE analyze_state = 0
            AND oss_key LIKE '%.log%'
    )
    AND (
        dtl.prop_user_id IS NULL
        OR dtl.prop_user_id = ''
        OR dtl.prop_user_id NOT IN (
            -- 测试用户
            SELECT user_id
            FROM `{PROJECT_ID}.{ds_user_info}.user_info`
            WHERE email IN (
                    'shijianjie@valleysound.xyz',
                    'shijianjie126@gmail.com',
                    'jies5093@gmail.com',
                    'bitcoke001@163.com',
                    'bitcoke002@163.com',
                    'risingsunxuri@gmail.com',
                    'wwh785944@gmail.com',
                    'zgxx@snapmail.cc'
                )
        )
    )
    AND dtl.prop_version_type = "ai_fashion"
"""


def safe_json_stringify(value):
    """
    安全地将值转换为JSON字符串，用于pandas DataFrame
    """
    if value is None or pd.isna(value):
        return None
    if isinstance(value, str):
        # 如果已经是字符串，直接返回
        return value
    if isinstance(value, (dict, list)):
        # 如果是对象或数组，转换为JSON字符串
        return json.dumps(value, ensure_ascii=False)
    # 其他类型转换为字符串
    return str(value)


INVITE_CODE_CODE_MASK = 873645731
_BASE62_ALPHABET = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
_BASE62_INDEX = {ch: i for i, ch in enumerate(_BASE62_ALPHABET)}


def _base62_decode(input_str: str) -> Optional[bytes]:
    try:
        value = 0
        for ch in input_str:
            if ch not in _BASE62_INDEX:
                return None
            value = value * 62 + _BASE62_INDEX[ch]
        # minimal bytes, big-endian
        length = max(1, (value.bit_length() + 7) // 8)
        return value.to_bytes(length, byteorder="big", signed=False)
    except Exception:
        return None


def invite_code_to_user_id(invite_code: str) -> Optional[str]:
    """
    Decode Base64 invite code to userId using Java-compatible logic:
    - Base64 decode (tolerate missing padding)
    - If starts with '6', strip prefix, Base62-decode, then use the bytes
    - Interpret first 8 bytes as big-endian signed long
    - XOR with INVITE_CODE_CODE_MASK
    - Return as string (signed 64-bit)
    """
    try:
        if not invite_code:
            return None
        s = invite_code.strip()
        if s.startswith("6"):
            # Special handling: Base62 decode after removing the '6' prefix
            b62 = _base62_decode(s[1:])
            if b62 is None:
                return None
            decoded = b62
        else:
            padding = (-len(s)) % 4
            s_padded = s + ("=" * padding)
            try:
                decoded = base64.b64decode(s_padded, validate=False)
            except Exception:
                decoded = base64.urlsafe_b64decode(s_padded)
        # 若不足8字节，则在高位（前面）补零（保留前导零字节语义）
        if len(decoded) < 8:
            decoded = (b"\x00" * (8 - len(decoded))) + decoded
        long_value = struct.unpack(">q", decoded[:8])[0]
        result = (long_value ^ INVITE_CODE_CODE_MASK) & 0xFFFFFFFFFFFFFFFF
        if result >= (1 << 63):
            result -= 1 << 64
        return str(result)
    except Exception:
        return None


def transform_data(query: str) -> pd.DataFrame:
    """
    转换数据结构，将JSON数组展开为多行，保留所有原始字段
    """
    # 读取数据
    logging.info("开始执行BigQuery查询...")
    query_job = client.query(query)
    logging.info("查询执行完成，正在获取结果...")

    # 获取查询结果并转换为pandas DataFrame
    df = query_job.to_dataframe()

    logging.info(f"获取到 {len(df)} 条记录，正在转换为DataFrame...")
    logging.info("DataFrame转换完成，开始处理数据...")

    # 将logAt_timestamp转换为TIMESTAMP类型（BigQuery兼容）
    if "logAt_timestamp" in df.columns:
        df["logAt_timestamp"] = pd.to_datetime(df["logAt_timestamp"])

    # 创建结果列表
    result_data = []

    # 逐行处理数据，使用itertuples提高性能
    total_rows = len(df)
    for index, row in enumerate(df.itertuples(), 1):
        if index % 1000 == 0:
            logging.info(f"正在处理数据 {index}/{total_rows}...")
        try:
            # 处理 args_spu - 拆分为 spu 列表
            args_spu_list = row.args_spu

            if (
                args_spu_list is None
                or pd.isna(args_spu_list)
                or str(args_spu_list).strip() == ""
                or str(args_spu_list).strip() == "null"
                or str(args_spu_list).strip() == "[]"
            ):
                args_spu_list = []
            elif isinstance(args_spu_list, str):
                if args_spu_list.startswith("["):
                    args_spu_list = json.loads(args_spu_list)
                else:
                    if args_spu_list.startswith('"') and args_spu_list.endswith('"'):
                        args_spu_list = args_spu_list[1:-1]
                    args_spu_list = [args_spu_list]
            else:
                args_spu_list = [str(args_spu_list)]

            # 去重 spu
            seen_spu = set()
            unique_spu_list = []
            for spu in args_spu_list:
                if spu not in seen_spu:
                    seen_spu.add(spu)
                    unique_spu_list.append(spu)
            args_spu_list = unique_spu_list

            # 处理 args_post - 拆分为 post 列表
            args_post_list = row.args_post

            if (
                args_post_list is None
                or pd.isna(args_post_list)
                or str(args_post_list).strip() == ""
                or str(args_post_list).strip() == "null"
                or str(args_post_list).strip() == "[]"
            ):
                args_post_list = []
            elif isinstance(args_post_list, str):
                if args_post_list.startswith("["):
                    args_post_list = json.loads(args_post_list)
                else:
                    if args_post_list.startswith('"') and args_post_list.endswith('"'):
                        args_post_list = args_post_list[1:-1]
                    args_post_list = [args_post_list]
            else:
                args_post_list = [str(args_post_list)]

            # 去重 post
            seen_post = set()
            unique_post_list = []
            for post in args_post_list:
                if post not in seen_post:
                    seen_post.add(post)
                    unique_post_list.append(post)
            args_post_list = unique_post_list

            # 如果两者都为空，创建一条记录（两个都为 None）
            if not args_spu_list and not args_post_list:
                expand_list = [(None, None)]
            else:
                # 独立展开：每个 spu 一行（post_code=None），每个 post 一行（product_code=None）
                # 例如: spu=[A,B], post=[1,2] → [(A,None), (B,None), (None,1), (None,2)]
                expand_list = [(spu, None) for spu in args_spu_list] + [
                    (None, post) for post in args_post_list
                ]

            # 缓存JSON字段的处理结果，避免重复计算
            cached_ext = safe_json_stringify(row.ext)
            cached_args = safe_json_stringify(row.args)
            cached_invite_user_id = (
                invite_code_to_user_id(row.prop_share_code)
                if row.prop_share_code
                else None
            )

            # 为每个 (product_code, post_code) 组合创建一行数据
            for idx, (product_code, post_code) in enumerate(expand_list):
                # 创建包含指定原始字段的字典
                row_dict = {
                    "event_name": row.event_name.lower(),  # 标准化小写
                    "logAt_timestamp": row.logAt_timestamp,
                    "session_id": row.session_id,
                    "prop_device_id": row.prop_device_id,
                    "prop_user_id": row.prop_user_id,
                    "prop_os": row.prop_os,
                    "prop_url": row.prop_url,
                    "prop_params": row.prop_params,
                    "prop_app_type": row.prop_app_type,
                    "prop_ua": row.prop_ua,
                    "ext": cached_ext,  # 使用缓存的JSON字段
                    "ext_productCode": row.ext_productCode,
                    "product_code": product_code,  # 来自 args_spu
                    "post_code": post_code,  # 来自 args_post
                    "args": cached_args,  # 使用缓存的JSON字段
                    "args_page_key": row.args_page_key,
                    "args_session_duration": row.args_session_duration,
                    "args_title": row.args_title,
                    "args_href": row.args_href,
                    "args_from": row.args_from,
                    "args_module": row.args_module,
                    "args_spu": row.args_spu,
                    "oss_create_at": row.oss_create_at,
                    "oss_key": row.oss_key,
                    "tenant_code": row.tenant_code,
                    "prop_share_code": row.prop_share_code,
                    "invite_user_id": cached_invite_user_id,  # 使用缓存的结果
                    "country": row.country,
                    "prop_version_type": row.prop_version_type,
                    "args_star": row.args_star,
                    "args_magazine": row.args_magazine,
                    "args_brand": row.args_brand,
                    "args_post": row.args_post,
                    "args_topic": row.args_topic,
                    "ext_recommend": row.ext_recommend,
                    "args_sku": row.args_sku,
                    "args_blogger": row.args_blogger,
                    "args_progress": row.args_progress,
                }

                # 生成一个hash值，包含更多唯一标识字段确保唯一性
                unique_string = f"{row.oss_key}_{row.prop_user_id}_{row.session_id}_{row.logAt_timestamp}_{product_code}_{post_code}_{row.event_name}_{idx}"
                row_dict["hash_id"] = hashlib.md5(unique_string.encode()).hexdigest()

                row_dict["update_time"] = pd.to_datetime(start_time)

                # 将字段hash放在第一列(使其能作为unique key)
                row_dict = {"hash_id": row_dict["hash_id"], **row_dict}

                result_data.append(row_dict)

        except Exception as e:
            logging.error(f"处理行时出错: {e}")
            # 构造与正常行相同结构的 dict，避免 schema 不一致
            row_dict = {
                "hash_id": hashlib.md5(
                    f"{row.oss_key}_{row.prop_user_id}_{row.session_id}_{row.logAt_timestamp}_error_{index}".encode()
                ).hexdigest(),
                "event_name": row.event_name,
                "logAt_timestamp": row.logAt_timestamp,
                "session_id": row.session_id,
                "prop_device_id": row.prop_device_id,
                "prop_user_id": row.prop_user_id,
                "prop_os": row.prop_os,
                "prop_url": row.prop_url,
                "prop_params": row.prop_params,
                "prop_app_type": row.prop_app_type,
                "prop_ua": row.prop_ua,
                "ext": safe_json_stringify(row.ext),
                "ext_productCode": row.ext_productCode,
                "product_code": None,
                "post_code": None,
                "args": safe_json_stringify(row.args),
                "args_page_key": row.args_page_key,
                "args_session_duration": row.args_session_duration,
                "args_title": row.args_title,
                "args_href": row.args_href,
                "args_from": row.args_from,
                "args_module": row.args_module,
                "args_spu": row.args_spu,
                "oss_create_at": row.oss_create_at,
                "oss_key": row.oss_key,
                "tenant_code": row.tenant_code,
                "prop_share_code": row.prop_share_code,
                "invite_user_id": None,
                "country": row.country,
                "prop_version_type": row.prop_version_type,
                "args_star": row.args_star,
                "args_magazine": row.args_magazine,
                "args_brand": row.args_brand,
                "args_post": row.args_post,
                "args_topic": row.args_topic,
                "ext_recommend": row.ext_recommend,
                "args_sku": row.args_sku,
                "args_blogger": row.args_blogger,
                "args_progress": row.args_progress,
                "update_time": pd.to_datetime(start_time),
            }
            result_data.append(row_dict)

    # 创建最终的DataFrame
    result = pd.DataFrame(result_data)

    # 去重处理：如果存在重复的hash_id，保留最新的记录
    if not result.empty:
        initial_count = len(result)
        result = result.drop_duplicates(subset=["hash_id"], keep="last")
        final_count = len(result)
        if initial_count != final_count:
            logging.warning(
                f"发现重复hash_id，已去重：{initial_count} -> {final_count} 条记录"
            )

    return result


def update_oss_key_process_log_with_recreate(
    client, project_id, dataset_id, oss_keys_to_update
):
    """
    通过重建表的方式更新 oss_key_process_log 表，解决Streaming Buffer问题
    """
    if not oss_keys_to_update:
        logging.info("没有 oss_key 需要更新状态")
        return False

    original_table_id = f"{project_id}.{dataset_id}.oss_key_process_log"
    backup_table_id = (
        f"{project_id}.{dataset_id}.oss_key_process_log_backup"  # 固定的备份表名
    )
    new_table_id = f"{project_id}.{dataset_id}.oss_key_process_log_new"

    try:
        # 1. 删除旧的备份表（如果存在）
        try:
            client.delete_table(backup_table_id)
            logging.info(f"已删除旧的备份表 {backup_table_id}")
        except Exception as e:
            # 如果备份表不存在，忽略错误
            logging.debug(f"备份表不存在或删除失败: {e}")

        # 2. 备份原始表（覆盖旧的备份）
        logging.info("开始备份 oss_key_process_log 表...")
        client.copy_table(original_table_id, backup_table_id).result()
        logging.info(f"已备份表到 {backup_table_id}（覆盖旧备份）")

        # 3. 创建新表结构（与原表相同）
        logging.info("创建新表...")
        # 获取原表的结构
        original_table = client.get_table(original_table_id)

        # 创建新表（与原表结构相同）
        new_table = bigquery.Table(new_table_id, schema=original_table.schema)
        new_table = client.create_table(new_table, exists_ok=True)
        logging.info(f"已创建新表 {new_table_id}")

        # 4. 从原表复制所有数据到新表，但更新指定oss_key的状态
        logging.info("复制数据并更新状态...")
        # 构建oss_key列表字符串，确保正确格式化
        oss_keys_list = ", ".join([f"'{key}'" for key in oss_keys_to_update])

        # 复制数据并更新处理的oss_key对应analyze_state的状态
        copy_and_update_query = f"""
        INSERT INTO `{new_table_id}`
        SELECT * REPLACE (
            CASE
                WHEN oss_key IN ({oss_keys_list})
                THEN 1
                ELSE analyze_state
            END as analyze_state
        )
        FROM `{original_table_id}`
        """

        copy_job = client.query(copy_and_update_query)
        copy_job.result()
        logging.info("数据复制和状态更新完成")

        # 5. 删除原始表
        logging.info("删除原始表...")
        client.delete_table(original_table_id)
        logging.info("原始表已删除")

        # 6. 将新表重命名为原始表名
        logging.info("重命名新表...")
        client.copy_table(new_table_id, original_table_id).result()
        client.delete_table(new_table_id)  # 删除临时表
        logging.info("新表已重命名为原始表名")

        logging.info(f"成功更新 {len(oss_keys_to_update)} 个 oss_key 的处理状态")
        return True

    except Exception as e:
        logging.error(f"重建 oss_key_process_log 表时出错: {e}")
        # 尝试清理可能创建的临时表
        try:
            client.delete_table(new_table_id)
        except Exception:
            pass
        raise


def safe_update_oss_key_process_log(client, project_id, dataset_id, oss_keys):
    """
    安全地更新 oss_key_process_log 表，直接使用表重建方式
    """
    if not oss_keys:
        logging.info("没有 oss_key 需要更新状态")
        return False

    try:
        # 直接使用表重建方式更新状态
        success = update_oss_key_process_log_with_recreate(
            client, project_id, dataset_id, oss_keys
        )
        return success
    except Exception as e:
        logging.error(f"更新 oss_key_process_log 表时出错: {e}")
        raise


def rollback_dwd_event_log(client, project_id, dataset_id, oss_keys):
    """
    回滚 dwd_event_log 表中指定 oss_key 的数据
    当 oss_key_process_log 更新失败时调用此函数
    """
    if not oss_keys:
        logging.info("没有 oss_key 需要回滚")
        return

    try:
        # 构建 oss_key 列表的 SQL 条件
        oss_keys_condition = ", ".join([f"'{key}'" for key in oss_keys])

        # 删除 dwd_event_log 表中对应的记录
        delete_query = f"""
        DELETE FROM `{project_id}.{dataset_id}.dwd_event_log`
        WHERE oss_key IN ({oss_keys_condition})
        """

        delete_job = client.query(delete_query)
        delete_job.result()
        logging.info(
            f"已回滚 {delete_job.num_dml_affected_rows} 条记录从 dwd_event_log 表"
        )

    except Exception as e:
        logging.error(f"回滚 dwd_event_log 表数据时出错: {e}")
        raise


try:
    # 处理数据
    result = transform_data(decom_initial_query)

    if result.empty:
        logging.info("没有新数据需要转换")
        exit(0)

    # 从结果中提取需要处理的 oss_keys
    oss_keys_to_process = result["oss_key"].unique().tolist()
    total_oss_keys = len(oss_keys_to_process)

    logging.info(f"发现 {total_oss_keys} 个新的 oss_key 需要处理")
    logging.info(f"需要处理的 oss_key 列表: {oss_keys_to_process}")

    # 收集所有唯一的 logAt_date，用于分区修剪
    result["logAt_date"] = pd.to_datetime(result["logAt_timestamp"]).dt.date
    logAt_dates = result["logAt_date"].unique().tolist()
    logAt_dates_str = [str(d) for d in logAt_dates]

    logging.info(f"发现 {len(logAt_dates)} 个不同的日期需要处理")
    logging.info(f"需要处理的日期列表: {logAt_dates_str}")

    # 删除临时字段 logAt_date，避免插入到数据库
    result = result.drop(columns=["logAt_date"])

    target_table = "dwd_event_log"

    # 创建临时表名
    utc_time = datetime.now(timezone.utc)
    temp_table_id = (
        f"{PROJECT_ID}.{DATASET_ID}.temp_{target_table}_{int(utc_time.timestamp())}"
    )

    # 将结果上传到临时表
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")

    load_job = client.load_table_from_dataframe(
        result, temp_table_id, job_config=job_config
    )
    load_job.result()  # 等待上传完成

    logging.info(f"已创建临时表: {temp_table_id}")

    # 使用 DELETE + INSERT 策略替代 MERGE，以充分利用分区修剪
    # 构建日期过滤条件用于分区修剪
    logAt_dates_formatted = ", ".join([f"'{d}'" for d in logAt_dates_str])

    # 步骤1: DELETE - 只删除相关分区和 hash_id 的数据（利用分区修剪）
    delete_query = f"""
    DELETE FROM `{PROJECT_ID}.{DATASET_ID}.{target_table}`
    WHERE DATE(logAt_timestamp) IN ({logAt_dates_formatted})
    AND hash_id IN (SELECT hash_id FROM `{temp_table_id}`)
    """

    logging.info("执行 DELETE 操作...")
    delete_job = client.query(delete_query)
    delete_job.result()  # 等待删除完成
    logging.info(f"DELETE 完成，影响了 {delete_job.num_dml_affected_rows} 行")

    # 步骤2: INSERT - 直接插入新数据
    insert_query = f"""
    INSERT INTO `{PROJECT_ID}.{DATASET_ID}.{target_table}`
    (hash_id, event_name, logAt_timestamp, session_id, prop_device_id, prop_user_id,
     prop_os, prop_url, prop_params, prop_app_type, prop_ua, ext, ext_productCode,
     product_code, post_code, args, args_title, args_page_key, args_session_duration, args_href, args_from, args_module, args_spu,
     oss_create_at, oss_key, tenant_code, prop_share_code, invite_user_id, country, update_time,
     prop_version_type, args_star, args_magazine, args_brand, args_post, args_topic,
     ext_recommend, args_sku, args_blogger, args_progress)
    SELECT
        hash_id, event_name, logAt_timestamp, session_id, prop_device_id, prop_user_id,
        prop_os, prop_url, prop_params, prop_app_type, prop_ua,
        PARSE_JSON(ext) as ext,
        ext_productCode,
        product_code,
        post_code,
        PARSE_JSON(args) as args,
        args_title,
        args_page_key,
        SAFE_CAST(args_session_duration AS NUMERIC) AS args_session_duration,
        args_href,
        args_from,
        args_module,
        args_spu,
        oss_create_at, oss_key, tenant_code, prop_share_code,
        invite_user_id, country, update_time,
        prop_version_type, args_star, args_magazine, args_brand, args_post, args_topic,
        ext_recommend, args_sku, args_blogger, args_progress
    FROM `{temp_table_id}`
    """

    logging.info("执行 INSERT 操作...")
    insert_job = client.query(insert_query)
    insert_job.result()  # 等待插入完成
    logging.info(f"INSERT 完成，插入了 {insert_job.num_dml_affected_rows} 行")

    # 删除临时表
    client.delete_table(temp_table_id)
    logging.info(f"临时表 {temp_table_id} 已删除")

    # 获取已处理的oss_keys
    processed_oss_keys = result["oss_key"].unique().tolist()

    # 使用表重建方式更新处理状态，避免Streaming Buffer问题
    update_success = False
    try:
        update_success = safe_update_oss_key_process_log(
            client, PROJECT_ID, ds_oss_key_process_log, processed_oss_keys
        )
        if update_success:
            logging.info(f"已更新 {len(processed_oss_keys)} 个 oss_key 的处理状态")
        else:
            logging.warning("未能成功更新 oss_key_process_log 表的状态")
    except Exception as update_err:
        logging.error(f"更新 oss_key_process_log 表时出错: {update_err}")
        # 更新失败，需要回滚 dwd_event_log 表中的数据
        try:
            rollback_dwd_event_log(client, PROJECT_ID, DATASET_ID, processed_oss_keys)
            logging.info("已回滚 dwd_event_log 表中的数据")
        except Exception as rollback_err:
            logging.error(f"回滚数据时也出错: {rollback_err}")
        # 重新抛出原始错误
        raise update_err

    logging.info(f"成功处理 {len(result)} 条记录到 {target_table} 表")

except Exception as err:
    logging.error(f"BigQuery操作出错: {err}")
    # 如果临时表存在，尝试删除
    if "temp_table_id" in locals():
        try:
            client.delete_table(temp_table_id)
        except Exception:
            pass
finally:
    logging.info("BigQuery操作完成")

    end_time = datetime.now(timezone.utc)
    elapsed_time = end_time - start_time
    logging.info(f"脚本运行总时间: {elapsed_time.total_seconds() / 60} 分钟")
