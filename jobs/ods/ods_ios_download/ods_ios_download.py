# ios下载数据自动化
# encoding: utf8
import time
import requests
import jwt
import gzip
import pandas as pd
import logging
from io import StringIO
from datetime import datetime, timezone, timedelta
from google.cloud import bigquery

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# # apple key 路径（本地测试路径）
# KEY_FILE = "/Users/sinn/Documents/Work/ai-fashion/jobs/ods/ods_ios_download/AuthKey_ZPUB8KVQ8R.p8"

# apple key 路径（服务器生产路径）
KEY_FILE = "/home/echooo/sinn_project/AuthKey_ZPUB8KVQ8R.p8"

KEY_ID = "ZPUB8KVQ8R"
ISSUER_ID = "fd1b6046-57d3-43d7-a95a-b87a74360441"

# 读取私钥文件
with open(KEY_FILE, "r") as f:
    private_key = f.read()

# 生成JWT
headers = {"alg": "ES256", "kid": KEY_ID, "typ": "JWT"}
payload = {
    "iss": ISSUER_ID,
    "iat": int(time.time()),
    "exp": int(time.time()) + 1200,
    "aud": "appstoreconnect-v1",
}

token = jwt.encode(payload, private_key, algorithm="ES256", headers=headers)

# 设置请求头
headers = {"Authorization": f"Bearer {token}"}


url = "https://api.appstoreconnect.apple.com/v1/salesReports"

# 项目和数据集配置
PROJECT_ID = "my-project-8584-jetonai"
DECOM_DATASET = "decom"

client = bigquery.Client(project=PROJECT_ID)


def query_table(dataset_id, query):
    """
    执行查询，支持不同数据集
    :param dataset_id: 数据集ID (mall 或 decom)
    :param query: SQL 查询语句
    :return: 查询结果
    """
    # 创建完整的数据集引用
    dataset_ref = f"{PROJECT_ID}.{dataset_id}"
    job_config = bigquery.QueryJobConfig(default_dataset=dataset_ref)

    return client.query(query, job_config=job_config).result()


def ios_download(start_timestamp, end_timestamp):
    start_date = datetime.strptime(start_timestamp, "%Y-%m-%d")
    end_date = datetime.strptime(end_timestamp, "%Y-%m-%d")
    date_range = [
        start_date + timedelta(days=x) for x in range((end_date - start_date).days + 1)
    ]

    for report_date in date_range:
        # 下载销售报告
        params = {
            "filter[frequency]": "DAILY",
            "filter[reportDate]": report_date.strftime("%Y-%m-%d"),
            "filter[reportSubType]": "SUMMARY",
            "filter[reportType]": "SALES",
            "filter[vendorNumber]": "92840935",
        }

        try:
            response = requests.get(url, headers=headers, params=params)
            logger.info(f"{report_date} - HTTP {response.status_code}")

            # 如果返回 404，说明该日期暂无数据，跳过
            if response.status_code == 404:
                logger.warning(f"{report_date} 暂无数据，跳过")
                continue

            data = gzip.decompress(response.content).decode("utf")

            df = pd.read_csv(StringIO(data), delimiter="\t")

            # 定义函数：将 NaN 转为 None
            def clean_value(val):
                if pd.isna(val):
                    return None
                return val

            rows_to_insert = []
            for _, row in df.iterrows():
                rows_to_insert.append(
                    {
                        "provider": clean_value(row.get("Provider")),
                        "provider_country": clean_value(row.get("Provider Country")),
                        "sku": clean_value(row.get("SKU")),
                        "developer": clean_value(row.get("Developer")),
                        "title": clean_value(row.get("Title")),
                        "version": clean_value(row.get("Version")),
                        "product_type_identifier": int(row["Product Type Identifier"]),
                        "units": int(row["Units"]),
                        "developer_proceeds": (
                            float(row["Developer Proceeds"])
                            if pd.notna(row["Developer Proceeds"])
                            else None
                        ),
                        "begin_date": datetime.strptime(
                            row["Begin Date"], "%m/%d/%Y"
                        ).strftime("%Y-%m-%d"),
                        "end_date": datetime.strptime(
                            row["End Date"], "%m/%d/%Y"
                        ).strftime("%Y-%m-%d"),
                        "customer_currency": clean_value(row.get("Customer Currency")),
                        "country_code": clean_value(row.get("Country Code")),
                        "currency_of_proceeds": clean_value(
                            row.get("Currency of Proceeds")
                        ),
                        "apple_identifier": (
                            str(row["Apple Identifier"])
                            if pd.notna(row["Apple Identifier"])
                            else None
                        ),
                        "customer_price": (
                            float(row["Customer Price"])
                            if pd.notna(row["Customer Price"])
                            else None
                        ),
                        "promo_code": clean_value(row.get("Promo Code")),
                        "parent_identifier": clean_value(row.get("Parent Identifier")),
                        "subscription": clean_value(row.get("Subscription")),
                        "period": clean_value(row.get("Period")),
                        "category": clean_value(row.get("Category")),
                        "cmb": clean_value(row.get("CMB")),
                        "device": clean_value(row.get("Device")),
                        "supported_platforms": clean_value(
                            row.get("Supported Platforms")
                        ),
                        "proceeds_reason": clean_value(row.get("Proceeds Reason")),
                        "preserved_pricing": clean_value(row.get("Preserved Pricing")),
                        "client": clean_value(row.get("Client")),
                        "order_type": clean_value(row.get("Order Type")),
                    }
                )

            logger.info(f"准备插入 {len(rows_to_insert)} 条记录")

            # 使用 delete + insert 模式导入数据
            table_id = f"{PROJECT_ID}.{DECOM_DATASET}.ods_ios_download"
            begin_date_str = report_date.strftime("%Y-%m-%d")

            # 先删除该日期的旧数据
            delete_query = f"""
                DELETE FROM `{table_id}`
                WHERE begin_date = '{begin_date_str}'
            """
            client.query(delete_query).result()
            logger.info(f"已删除 {begin_date_str} 的旧数据")

            # 插入新数据
            if rows_to_insert:
                errors = client.insert_rows_json(table_id, rows_to_insert)
                if errors == []:
                    logger.info(f"{len(rows_to_insert)} 条记录成功插入 {table_id}")
                else:
                    logger.error(f"插入数据时遇到错误: {errors}")

        except Exception as e:
            logger.error(f"{report_date} - {e}")


# 获取当前 UTC 时间
utc_now = datetime.now(timezone.utc)

# 计算 today-7 到 today 的日期范围
start_date = (utc_now - timedelta(days=7)).date().strftime("%Y-%m-%d")
end_date = utc_now.date().strftime("%Y-%m-%d")

logger.info(f"执行日期范围: {start_date} 到 {end_date}")
ios_download(start_date, end_date)
