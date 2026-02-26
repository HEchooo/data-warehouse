import datetime
from google.cloud import bigquery


def get_bigquery_client(project_id=None):
    """获取BigQuery客户端"""
    if project_id:
        client = bigquery.Client(project=project_id)
    else:
        client = bigquery.Client()
    return client


def get_current_datetime():
    """获取当前日期时间字符串"""
    return datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def insert_into_bigquery(
    bigquery_client, rows_data, dataset_id="decom", table_id="decom_track_log"
):
    """
    批量插入数据到BigQuery

    Args:
        bigquery_client: BigQuery客户端
        rows_data: 要插入的数据行列表
        dataset_id: 数据集ID
        table_id: 表ID
    """
    if not rows_data:
        print("No data to insert into BigQuery")
        return

    project_id = bigquery_client.project
    table_ref = f"{project_id}.{dataset_id}.{table_id}"

    try:
        errors = bigquery_client.insert_rows_json(table_ref, rows_data)
        if errors:
            print(f"❌ BigQuery插入错误: {errors}")
            raise Exception(f"BigQuery insertion errors: {errors}")
        else:
            print(f"✅ 成功插入 {len(rows_data)} 行数据到BigQuery表 {table_ref}")
    except Exception as e:
        print(f"❌ BigQuery插入失败: {e}")
        raise


def format_gcs_key(gcs_key):
    """
    格式化GCS键值，移除不需要的前缀

    Args:
        gcs_key: 原始GCS键值

    Returns:
        格式化后的键值
    """
    # 移除常见的前缀
    prefixes_to_remove = ["/home/echooo/decom-prod-client-log/", "archived/"]

    formatted_key = gcs_key
    for prefix in prefixes_to_remove:
        if formatted_key.startswith(prefix):
            formatted_key = formatted_key[len(prefix) :]

    return formatted_key


def validate_log_file_path(gcs_key):
    """
    验证日志文件路径是否有效

    Args:
        gcs_key: GCS键值

    Returns:
        bool: 是否为有效的日志文件路径
    """
    # 检查是否为archived目录下的.log文件
    if not gcs_key.startswith("archived/") or not gcs_key.endswith(".log"):
        return False

    # 检查路径格式是否正确 (archived/YYYY-MM-DD/HH_MM.x.x.x.x.x.log)
    parts = gcs_key.split("/")
    if len(parts) < 3:
        return False

    # 验证日期格式
    try:
        date_part = parts[1]
        datetime.datetime.strptime(date_part, "%Y-%m-%d")
        return True
    except ValueError:
        return False


def parse_timestamp_from_gcs_key(gcs_key):
    """
    从GCS键值解析时间戳

    Args:
        gcs_key: GCS键值，格式如 archived/2025-09-05/09_51.0.172.20.0.4.log

    Returns:
        int: Unix时间戳（毫秒）
    """
    try:
        parts = gcs_key.split("/")
        if len(parts) >= 3:
            date_part = parts[1]  # 2025-09-05
            filename = parts[2]  # 09_51.0.172.20.0.4.log
            time_part = filename.split(".")[0]  # 09_51

            datetime_str = f"{date_part} {time_part}"
            dt = datetime.datetime.strptime(datetime_str, "%Y-%m-%d %H_%M")
            return int(dt.timestamp() * 1000)
    except Exception as e:
        print(f"Error parsing timestamp from GCS key {gcs_key}: {e}")

    # 如果解析失败，返回当前时间戳
    return int(datetime.datetime.now().timestamp() * 1000)


def create_bigquery_row(log_entity, log_day, ctime, gcs_key):
    """
    创建BigQuery行数据

    Args:
        log_entity: 日志实体对象
        log_day: 日志日期
        ctime: 创建时间
        gcs_key: GCS键值

    Returns:
        dict: BigQuery行数据
    """
    clean_gcs_key = format_gcs_key(gcs_key)

    return {
        "logAt": int(log_entity.logAt),
        "event_name": log_entity.event_name,
        "logAt_timestamp": log_entity.logAt_timestamp.isoformat(),
        "logAt_day": log_day,
        "session_id": log_entity.session_id,
        "prop_device_id": log_entity.properties.device_id,
        "prop_user_id": log_entity.properties.user_id,
        "prop_os": log_entity.properties.os,
        "prop_url": log_entity.properties.url,
        "prop_params": log_entity.properties.params,
        "prop_app_type": log_entity.properties.app_type,
        "prop_ua": log_entity.properties.ua,
        "prop_share_code": log_entity.properties.share_code,
        "ext": json.dumps(log_entity.ext) if log_entity.ext else "{}",
        "ext_productCode": (
            json.dumps(log_entity.ext.get("productCode", None))
            if log_entity.ext
            else "null"
        ),
        "args": (
            json.dumps(log_entity.args) if isinstance(log_entity.args, dict) else "{}"
        ),
        "args_page_key": (
            log_entity.args.get("page_key", None)
            if isinstance(log_entity.args, dict)
            else None
        ),
        "args_title": (
            log_entity.args.get("title", None)
            if isinstance(log_entity.args, dict)
            else None
        ),
        "args_href": (
            log_entity.args.get("href", None)
            if isinstance(log_entity.args, dict)
            else None
        ),
        "args_from": (
            log_entity.args.get("from", None)
            if isinstance(log_entity.args, dict)
            else None
        ),
        "args_module": (
            log_entity.args.get("module", None)
            if isinstance(log_entity.args, dict)
            else None
        ),
        "oss_create_at": ctime,
        "oss_key": clean_gcs_key,
        "country": log_entity.country,
    }


# 导入json模块用于上面的函数
import json
