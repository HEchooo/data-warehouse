import json
import os
from datetime import datetime
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from google.cloud import storage
from google.cloud import bigquery
import functions_framework
import applog

# 初始化客户端
storage_client = storage.Client()
bigquery_client = bigquery.Client()


def normalize_numeric_for_bigquery(value):
    """将输入值规范化为BigQuery NUMERIC可接受的字符串（最多9位小数）"""
    if value is None or value == "":
        return None

    try:
        numeric_value = Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        return None

    if not numeric_value.is_finite():
        return None

    normalized_value = numeric_value.quantize(
        Decimal("0.000000001"), rounding=ROUND_HALF_UP
    )
    return format(normalized_value, "f")


@functions_framework.http
def process_log_file(request):
    """Cloud Run entry point for processing Cloud Storage events"""
    try:
        # Parse the Cloud Storage event from Pub/Sub
        event_data = request.get_json()

        # Extract bucket and object information
        bucket_name = event_data.get("bucket")
        object_name = event_data.get("name")

        if not bucket_name or not object_name:
            print("Missing bucket or object name in event")
            return "Missing bucket or object name", 400

        print(f"Processing file - bucket: {bucket_name}, key: {object_name}")

        # 验证文件路径和格式
        if not object_name.startswith("archived/") or not object_name.endswith(".log"):
            print(f"Skip non-archived log file: {object_name}")
            return f"Skip non-archived log file: {object_name}", 200

        # 下载文件内容
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(object_name)

        # 检查文件是否存在
        if not blob.exists():
            print(f"File does not exist: {object_name}")
            return f"File does not exist: {object_name}", 200

        file_content = blob.download_as_text()

        # 解析日志文件
        log_list = get_log_entity_list_from_content(file_content, object_name)

        if not log_list:
            print(f"No valid log entities found in file: {object_name}")
            return f"No valid log entities found in file: {object_name}", 200

        # 解析日志日期
        log_day = parse_log_day_from_gcs_key(object_name)

        # 获取文件创建时间
        ctime = int(blob.time_created.timestamp()) if blob.time_created else 0

        # 保存到BigQuery
        save_log_list(log_list, bigquery_client, log_day, ctime, object_name)

        # 记录处理日志
        save_oss_key_process_log(bigquery_client, object_name)

        print(
            f"✅ Successfully processed {len(log_list)} log entities from {object_name}"
        )
        return f"Successfully processed {len(log_list)} log entities", 200

    except Exception as e:
        print(f"❌ Error processing file: {str(e)}")
        return f"Error: {str(e)}", 500


def get_log_entity_list_from_content(file_content, gcs_key):
    """从文件内容解析日志实体列表"""
    print(f"Begin parsing log entities from GCS key: {gcs_key}")
    log_list = []

    lines = file_content.strip().split("\n")

    for i, line in enumerate(lines, 1):
        if not line.strip():  # 跳过空行
            continue

        try:
            log_entity = applog.decode_json(line, gcs_key)
            log_list.append(log_entity)
        except Exception as e:
            print(f"Error parsing line {i} in {gcs_key}: {e}")
            continue

    print(f"Total {len(log_list)} log entities parsed from {gcs_key}")
    return log_list


def save_log_list(log_list, bigquery_client, log_day, ctime, gcs_key):
    """
    将日志列表保存到BigQuery

    Args:
        log_list: 日志实体列表
        bigquery_client: BigQuery客户端
        log_day: 日志日期
        ctime: 创建时间
        gcs_key: GCS键值
    """
    # 清理GCS键值，移除路径前缀
    print(f"Saving {len(log_list)} log entities to BigQuery, gcs_key={gcs_key}")

    # BigQuery表配置
    project_id = "my-project-8584-jetonai"
    dataset_id = "decom"
    table_id = "ods_event_log"
    table_ref = f"{project_id}.{dataset_id}.{table_id}"

    # 准备数据行
    rows_to_insert = []
    # extrac oss_key such 'archived/2025-11-12/020.172.30.0.4.log' the date part and cast to bigyquery date type
    oss_key_date = None
    try:
        oss_key_date = datetime.strptime(gcs_key.split("/")[1], "%Y-%m-%d").date()
    except Exception as e:
        print(f"Error extracting oss_key_date from gcs_key {gcs_key}: {e}")
        oss_key_date = datetime.strptime("2024-11-01", "%Y-%m-%d").date()

    for log in log_list:
        try:
            # 构建BigQuery行数据
            row = {
                "logAt": int(log.logAt),
                "event_name": log.event_name,
                "logAt_timestamp": log.logAt_timestamp.isoformat(),
                "logAt_day": log_day,
                "session_id": log.session_id,
                "prop_device_id": log.properties.device_id,
                "prop_user_id": log.properties.user_id,
                "prop_os": log.properties.os,
                "prop_url": log.properties.url,
                "prop_params": log.properties.params,
                "prop_app_type": log.properties.app_type,
                "prop_ua": log.properties.ua,
                "prop_share_code": log.properties.share_code,
                "prop_timezone": log.properties.timezone,
                "ext": json.dumps(log.ext),
                "ext_productCode": json.dumps(log.ext.get("productCode", None)),
                "args": json.dumps(log.args) if isinstance(log.args, dict) else "{}",
                "args_session_duration": (
                    normalize_numeric_for_bigquery(log.args.get("session_duration", None))
                    if isinstance(log.args, dict)
                    else None
                ),
                "args_page_key": (
                    log.args.get("page_key", None)
                    if isinstance(log.args, dict)
                    else None
                ),
                "args_title": (
                    log.args.get("title", None) if isinstance(log.args, dict) else None
                ),
                "args_href": (
                    log.args.get("href", None) if isinstance(log.args, dict) else None
                ),
                "args_from": (
                    log.args.get("from", None) if isinstance(log.args, dict) else None
                ),
                "args_module": (
                    log.args.get("module", None) if isinstance(log.args, dict) else None
                ),
                "args_spu": json.dumps(log.args.get("spu", None)),
                "oss_create_at": ctime,
                "oss_key": gcs_key,
                "oss_key_date": oss_key_date.isoformat(),
                "country": log.country,
                "prop_version_type": log.properties.version_type,
                "args_star": json.dumps(log.args.get("star", None)),
                "args_magazine": json.dumps(log.args.get("magazine", None)),
                "args_brand": json.dumps(log.args.get("brand", None)),
                "args_post": json.dumps(log.args.get("post", None)),
                "args_topic": json.dumps(log.args.get("topic", None)),
                "ext_recommend": json.dumps(log.ext.get("recommend", None)),
                "args_sku": json.dumps(log.args.get("sku", None)),
                "args_blogger": json.dumps(log.args.get("blogger", None)),
                "args_progress": json.dumps(log.args.get("progress", None)),
            }

            rows_to_insert.append(row)

            # 每1000行批量插入
            if len(rows_to_insert) >= 1000:
                try:

                    errors = bigquery_client.insert_rows_json(
                        table_ref, rows_to_insert, skip_invalid_rows=True
                    )
                    if errors:
                        print(f"❌ 插入数据时出现错误: {errors} gcs_key={gcs_key}")
                    else:
                        print(
                            f"✅ 成功插入 {len(rows_to_insert)} 行数据到BigQuery gcs_key={gcs_key}"
                        )
                    rows_to_insert.clear()
                except Exception as e:
                    print(f"❌ 批量插入数据失败: {e} gcs_key={gcs_key}")
                    rows_to_insert.clear()

        except Exception as e:
            print(f"❌ 处理日志实体时出错: {e} gcs_key={gcs_key}")
            continue

    # 插入剩余的数据
    if rows_to_insert:
        try:
            errors = bigquery_client.insert_rows_json(
                table_ref, rows_to_insert, skip_invalid_rows=True
            )
            if errors:
                print(f"❌ 插入剩余数据时出现错误: {errors} gcs_key={gcs_key}")
            else:
                print(
                    f"✅ 成功插入剩余 {len(rows_to_insert)} 行数据到BigQuery gcs_key={gcs_key}"
                )
        except Exception as e:
            print(f"❌ 插入剩余数据失败: {e} gcs_key={gcs_key}")

    print(f"✅ 完成保存 {len(log_list)} 个日志实体到BigQuery gcs_key={gcs_key}")


def parse_log_day_from_gcs_key(gcs_key):
    """从GCS键值解析日志日期"""
    # GCS键值格式: archived/2025-09-05/09_51.0.172.20.0.4.log
    try:
        parts = gcs_key.split("/")
        if len(parts) >= 2:
            return parts[1]  # 返回日期部分，如 "2025-09-05"
        return datetime.now().strftime("%Y-%m-%d")
    except Exception as e:
        print(f"Error parsing log day from GCS key {gcs_key}: {e}")
        return datetime.now().strftime("%Y-%m-%d")


def save_oss_key_process_log(bigquery_client, gcs_key):
    """
    保存GCS键处理日志到BigQuery

    Args:
        bigquery_client: BigQuery客户端
        gcs_key: GCS键值
    """
    clean_gcs_key = gcs_key.replace("archived/", "")
    print(f"Saving GCS key process log: {clean_gcs_key}")

    # BigQuery表配置
    project_id = "my-project-8584-jetonai"
    dataset_id = "decom"
    table_id = "oss_key_process_log"
    table_ref = f"{project_id}.{dataset_id}.{table_id}"

    # 准备数据行
    row = {
        "oss_key": gcs_key,
        "record_state": 1,
        "analyze_state": 0,
        "device_info_analyze_state": 0,
        "first_pay_state": 0,
    }

    try:
        errors = bigquery_client.insert_rows_json(table_ref, [row])
        if errors:
            print(f"❌ 保存GCS键处理日志失败: {errors}")
        else:
            print(f"✅ 成功保存GCS键处理日志: {gcs_key}")
    except Exception as e:
        print(f"❌ 保存GCS键处理日志时出错: {e}")


if __name__ == "__main__":
    # 本地测试用
    import sys

    if len(sys.argv) > 1:
        test_event = {"bucket": "decom-prod-client-log", "name": sys.argv[1]}

        class MockRequest:
            def get_json(self):
                return test_event

        result = process_log_file(MockRequest())
        print(f"Test result: {result}")
