import json
import datetime
import os
import time


class Properties:
    def __init__(
        self,
        device_id,
        user_id,
        os,
        url,
        params,
        app_type,
        ua,
        share_code,
        version_type,
    ):
        self.device_id = device_id
        self.user_id = user_id
        self.os = os
        self.url = url
        self.params = params
        self.app_type = app_type
        self.ua = ua
        self.share_code = share_code
        self.version_type = version_type

    def __str__(self):
        return f"device_id={self.device_id}, user_id={self.user_id}, os={self.os}, url = {self.url}, params = {self.params}, app_type = {self.app_type}, ua = {self.ua}, share_code = {self.share_code}"


class AppTrackLogEntity:
    def __init__(
        self, logAt, event_name, session_id, properties: Properties, ext, args, country
    ):
        self.logAt = logAt
        self.event_name = event_name
        self.session_id = session_id
        self.properties = properties
        self.ext = ext
        self.logAt_timestamp = datetime.datetime.fromtimestamp(int(logAt) / 1000)
        self.args = args
        self.country = country

    def __str__(self):
        return f"logAt: {self.logAt}, session_id: {self.session_id}, event_name: {self.event_name}, properties: {self.properties}, ext: {self.ext}, logAt_timestamp: {self.logAt_timestamp}"


def get_unix_timestamp(file_path):
    """从文件路径获取Unix时间戳"""
    file_name = os.path.basename(file_path)  # such as: 00_06.0.172.8.1.200.log
    directory_path = os.path.dirname(file_path)
    # 提取目录名称（日期格式）
    date_directory = os.path.basename(directory_path)  # such as 2024-06-28
    date_time_str = (
        date_directory + " " + file_name.split(".")[0]
    )  # such as 2024-06-28 00_06
    timeobj = datetime.datetime.strptime(date_time_str, "%Y-%m-%d %H_%M")
    return int(timeobj.timestamp()) * 1000


def decode_json(json_str, file_path):
    """解析JSON字符串为AppTrackLogEntity对象"""
    if not json_str.strip():
        raise ValueError("Empty JSON string")

    data_dict = json.loads(json_str)

    # 解析event_name
    event_name = data_dict.get("event_name")

    # 解析properties
    prop_dict = data_dict.get("properties", {})

    device_id = prop_dict.get("device_id")
    user_id = prop_dict.get("user_id")
    os = prop_dict.get("os")
    url = prop_dict.get("url")
    params = prop_dict.get("params")
    app_type = prop_dict.get("app_type")
    ua = prop_dict.get("ua")
    share_code = prop_dict.get("share_code")
    version_type = prop_dict.get("version_type")

    # 处理share_code长度限制
    if share_code and len(share_code) > 32:
        print(
            f"Warning: share_code length exceeds 32 characters in file {file_path}, truncating to 32 characters."
        )
        share_code = share_code[:32]

    properties = Properties(
        device_id, user_id, os, url, params, app_type, ua, share_code, version_type
    )

    # 解析ext
    ext = data_dict.get("ext", {})

    # 解析logAt
    logAt = data_dict.get("logAt")
    if logAt is None:
        # 获取当前时间的时间戳（单位为秒）
        current_timestamp = time.time()
        # 转换为毫秒
        logAt = int(current_timestamp * 1000)

    # 解析session_id
    session_id = data_dict.get("session_id")

    # 解析args
    args = data_dict.get("args", {})

    # 解析country
    country = data_dict.get("country")

    return AppTrackLogEntity(
        logAt, event_name, session_id, properties, ext, args, country
    )
