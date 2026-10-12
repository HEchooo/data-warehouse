"""仅供本地 Superset Demo，元数据库和随机密钥都位于被忽略的 logs 目录。"""
import os
from pathlib import Path

DATA_DIR = os.environ["EVER_PRETTY_SUPERSET_HOME"]
SECRET_KEY = (Path(DATA_DIR) / "secret_key").read_text().strip()
SQLALCHEMY_DATABASE_URI = "sqlite:///" + str(Path(DATA_DIR) / "superset.db")
APP_NAME = "ECN · Ever Pretty Demo"
TALISMAN_ENABLED = False  # 本地仅监听 127.0.0.1，以 HTTP 预览。
WTF_CSRF_ENABLED = True
ENABLE_PROXY_FIX = False
SESSION_COOKIE_SECURE = False
SQLALCHEMY_TRACK_MODIFICATIONS = False
WTF_CSRF_TIME_LIMIT = 86400
CACHE_CONFIG = {"CACHE_TYPE": "SimpleCache", "CACHE_DEFAULT_TIMEOUT": 300}
DATA_CACHE_CONFIG = {"CACHE_TYPE": "SimpleCache", "CACHE_DEFAULT_TIMEOUT": 300}
FEATURE_FLAGS = {"DASHBOARD_NATIVE_FILTERS": True}
BABEL_DEFAULT_LOCALE = "zh"
LANGUAGES = {"en": {"flag": "us", "name": "English"}, "zh": {"flag": "cn", "name": "中文"}}
