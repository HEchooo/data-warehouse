# 小红书投放金额数据自动化
import mysql.connector
import requests
import json
from datetime import datetime, timezone, timedelta

with open('/home/ec2-user/ec2-user/python_connect_doris/xiaohongshu_cost/token.json', 'r', encoding='utf-8') as f:
    token = json.load(f)
    refresh_token = token['data']['refresh_token']

# 设置要请求的URL
refresh_token_url = 'https://adapi.xiaohongshu.com/api/open/oauth2/refresh_token'

# 设置请求的头部信息
refresh_token_headers = {
    'Content-Type': 'application/json',
}

# 设置请求的数据
refresh_token_data = {
    "app_id": 694,
    "secret": "m9SOmGOsblBnxEbF",
    "refresh_token": refresh_token
}

# 发送POST请求
refresh_token_response = requests.post(refresh_token_url, headers=refresh_token_headers, json=refresh_token_data)

# 打印响应内容
print(f"刷新token的接口输出结果：{refresh_token_response.text}")

if refresh_token_response.status_code == 200:
    with open("/home/ec2-user/ec2-user/python_connect_doris/xiaohongshu_cost/token.json", "w", encoding="utf-8") as file:
        file.write(refresh_token_response.text)
        print("token.json文件更新成功！")

with open('/home/ec2-user/ec2-user/python_connect_doris/xiaohongshu_cost/token.json', 'r', encoding='utf-8') as f:
    token = json.load(f)
    access_token = token['data']['access_token']

# 离线报表
offline_data_url = 'https://adapi.xiaohongshu.com/api/open/jg/data/report/offline/account'

# # 实时报表
# realtime_data_url = 'https://adapi.xiaohongshu.com/api/open/jg/data/report/realtime/account'


get_data_headers = {
    'Content-Type': 'application/json',
    'Access-Token': access_token
}


def get_offline_data(filter_date):
    get_data = {
        "advertiser_id": 1531289,
        "start_date": filter_date,
        "end_date": filter_date,
        "split_columns": ["countryName"],
    }

    # 发送POST请求
    get_data_response = requests.post(offline_data_url, headers=get_data_headers, json=get_data)

    # 打印响应内容
    get_data_text = get_data_response.text

    get_datas = json.loads(get_data_text)['data']['data_list']

    for get_data in get_datas:
        country_name = get_data['country_name']
        fee = get_data['fee']
        impression = get_data['impression']
        click = get_data['click']

        print(f"{filter_date} 国家：{country_name}，总消费：{fee}，总展现：{impression}，总点击：{click}")

        print("点击率 = 总点击 / 总展现\n平均点击价格 = 总消费/总展现\n")

        # 连接到Doris数据库
        connection = mysql.connector.connect(
            host='172.8.1.50',
            port=8032,
            user='da',
            password='vb5g#&*h76',
            database='mall'
        )

        # 创建游标对象
        cursor = connection.cursor()

        # 准备插入数据的 SQL 模板
        insert_query = """INSERT INTO xiaohongshu_ad VALUES (%s, %s, %s, %s, %s);"""

        cursor.execute(insert_query, (filter_date, country_name, fee, impression, click))


# utc昨天日期
utc_now = datetime.now(timezone.utc)

yesterday_utc = utc_now - timedelta(days=1)

# 提取日期部分
yesterday_date = yesterday_utc.date().strftime('%Y-%m-%d')

get_offline_data(yesterday_date)