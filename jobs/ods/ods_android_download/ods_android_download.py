# Android下载数据自动化
# encoding: utf8
import pandas as pd
import logging
import time
from io import StringIO
from google.cloud import storage
from google.cloud import bigquery

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# GCS 配置 (Google Play Console 导出数据的存储桶)
BUCKET_NAME = "pubsite_prod_5101881267809973114"
PACKAGE_NAME = "com.alvinclub.app.mall"

# # gcs 桶服务账号（本地测试路径）
# GCS_SERVICE_ACCOUNT = "/Users/sinn/Documents/Work/ai-fashion/jobs/ods/ods_android_download/tidy-access-396909-b58d1fb35fce.json"

# gcs 桶服务账号（服务器生产路径）
GCS_SERVICE_ACCOUNT = "/home/echooo/sinn_project/tidy-access-396909-b58d1fb35fce.json"

# BigQuery 配置
PROJECT_ID = "my-project-8584-jetonai"
DECOM_DATASET = "decom"

# # bigquery 服务账号（本地测试路径）
# BQ_SERVICE_ACCOUNT = (
#     "/Users/sinn/Documents/BigQuerykey/my-project-8584-jetonai-2bafc5bf74da.json"
# )

# bigquery 服务账号（服务器生产路径）
BQ_SERVICE_ACCOUNT = (
    "/home/echooo/sinn_project/my-project-8584-jetonai-2bafc5bf74da.json"
)

# 初始化客户端
storage_client = storage.Client.from_service_account_json(GCS_SERVICE_ACCOUNT)
bigquery_client = bigquery.Client.from_service_account_json(
    BQ_SERVICE_ACCOUNT, project=PROJECT_ID
)


def create_temp_table(target_table_id, temp_table_id):
    create_query = f"""
        CREATE TABLE `{temp_table_id}` AS
        SELECT * FROM `{target_table_id}`
        WHERE 1 = 0
    """
    bigquery_client.query(create_query).result()
    logger.info(f"已创建临时表: {temp_table_id}")


def drop_table_if_exists(table_id):
    drop_query = f"DROP TABLE IF EXISTS `{table_id}`"
    bigquery_client.query(drop_query).result()
    logger.info(f"已删除表: {table_id}")


def build_android_changed_table(
    target_table_id, temp_table_id, changed_table_id, min_date, max_date
):
    changed_query = f"""
        CREATE TABLE `{changed_table_id}`
        PARTITION BY date AS
        SELECT * FROM `{target_table_id}`
        WHERE date < '{min_date}' OR date > '{max_date}'
        UNION ALL
        SELECT * FROM `{temp_table_id}`
    """
    bigquery_client.query(changed_query).result()
    logger.info(f"已构建变更临时表: {changed_table_id}")


def drop_and_rename_table(source_table_id, target_table_id):
    target_table_name = target_table_id.split(".")[-1]
    drop_table_if_exists(target_table_id)
    rename_query = f"ALTER TABLE `{source_table_id}` RENAME TO {target_table_name}"
    bigquery_client.query(rename_query).result()
    logger.info(f"已将 {source_table_id} 重命名为 {target_table_id}")


def list_available_months():
    """列出 GCS 存储桶中可用的月份数据文件"""
    bucket = storage_client.bucket(BUCKET_NAME)
    blobs = bucket.list_blobs(prefix="stats/installs/")

    months = set()
    for blob in blobs:
        # 文件名格式: installs_com.alvinclub.app.mall_202601_country.csv
        if blob.name.endswith("_country.csv"):
            parts = blob.name.split("_")
            if len(parts) >= 3:
                months.add(parts[-2])  # 获取月份部分如 202601

    return sorted(months)


def fetch_download_data(year_month: str) -> pd.DataFrame:
    """
    从 GCS 获取指定月份的下载量数据
    :param year_month: 年月，格式如 "202601"
    :return: pandas DataFrame
    """
    file_name = f"stats/installs/installs_{PACKAGE_NAME}_{year_month}_country.csv"
    bucket = storage_client.bucket(BUCKET_NAME)
    blob = bucket.blob(file_name)

    if not blob.exists():
        logger.error(f"文件不存在: {file_name}")

    logger.info(f"正在读取 GCS 文件: gs://{BUCKET_NAME}/{file_name}")

    # 直接从 GCS 读取文件内容，无需下载到本地
    content = blob.download_as_text(encoding="utf-16")

    # 使用 pandas 解析 CSV
    df = pd.read_csv(StringIO(content))
    logger.info(f"成功读取 {len(df)} 条记录")

    return df


def android_download(year_month: str):
    """
    获取 Android 下载数据并插入 BigQuery
    :param year_month: 年月，格式 "YYYYMM"
    """
    # 从 GCS 获取数据
    df = fetch_download_data(year_month)
    if df is None or df.empty:
        logger.warning(f"没有获取到 {year_month} 的数据")
        return

    def clean_value(val):
        return None if pd.isna(val) else val

    def clean_int(val):
        return int(val) if pd.notna(val) else None

    rows_to_insert = [
        {
            "date": row["Date"],
            "package_name": clean_value(row.get("Package name")),
            "country": clean_value(row.get("Country")),
            "daily_device_installs": clean_int(row.get("Daily Device Installs")),
            "daily_device_uninstalls": clean_int(row.get("Daily Device Uninstalls")),
            "daily_device_upgrades": clean_int(row.get("Daily Device Upgrades")),
            "total_user_installs": clean_int(row.get("Total User Installs")),
            "daily_user_installs": clean_int(row.get("Daily User Installs")),
            "daily_user_uninstalls": clean_int(row.get("Daily User Uninstalls")),
            "active_device_installs": clean_int(row.get("Active Device Installs")),
            "install_events": clean_int(row.get("Install events")),
            "update_events": clean_int(row.get("Update events")),
            "uninstall_events": clean_int(row.get("Uninstall events")),
        }
        for _, row in df.iterrows()
    ]

    if not rows_to_insert:
        logger.warning(f"{year_month} 没有数据")
        return

    logger.info(f"准备插入 {len(rows_to_insert)} 条记录")

    table_id = f"{PROJECT_ID}.{DECOM_DATASET}.ods_android_download"
    staging_table_id = f"{table_id}_tmp_{year_month}_{int(time.time())}"
    changed_table_id = f"{table_id}_changed_{year_month}_{int(time.time())}"
    dates = [r["date"] for r in rows_to_insert]
    min_date, max_date = min(dates), max(dates)
    swapped = False

    try:
        create_temp_table(table_id, staging_table_id)

        errors = bigquery_client.insert_rows_json(staging_table_id, rows_to_insert)
        if errors != []:
            logger.error(f"写入临时表时遇到错误: {errors}")
            return
        logger.info(f"{len(rows_to_insert)} 条记录成功写入临时表")

        build_android_changed_table(
            table_id, staging_table_id, changed_table_id, min_date, max_date
        )
        drop_and_rename_table(changed_table_id, table_id)
        swapped = True
        logger.info(f"已替换 {min_date} 到 {max_date} 的分区数据（drop + rename）")
    finally:
        drop_table_if_exists(staging_table_id)
        if not swapped:
            logger.warning(f"保留变更临时表用于排查: {changed_table_id}")


if __name__ == "__main__":
    # 先列出可用月份
    logger.info("正在获取可用月份列表...")
    months = list_available_months()
    logger.info(f"可用月份: {months}")

    if months:
        # 使用最新的月份
        year_month = months[-1]
        logger.info(f"执行月份: {year_month}")
        android_download(year_month)
    else:
        logger.error("没有找到可用的数据文件")
