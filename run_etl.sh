#!/bin/bash

# AI Fashion ETL 定时任务脚本
# 执行顺序: ODS -> DWD -> DWS -> ADS

# 配置
PROJECT_DIR="/home/echooo/sinn_project/ai-fashion"
VENV_PYTHON="/usr/local/bin/python3.12"
LOG_DIR="/home/echooo/sinn_project/ai-fashion/logs"

# 创建日志目录
mkdir -p "$LOG_DIR"

# 日志文件
LOG_FILE="$LOG_DIR/etl_$(date +%Y%m%d).log"

# 记录开始时间
echo "========================================" >> "$LOG_FILE"
echo "ETL 开始: $(date '+%Y-%m-%d %H:%M:%S')" >> "$LOG_FILE"
echo "========================================" >> "$LOG_FILE"
echo "" >> "$LOG_FILE"

# 切换到项目目录
cd "$PROJECT_DIR" || exit 1

# ========================================
# ODS 层 ETL
# ========================================
echo "========================================" >> "$LOG_FILE"
echo "[$(date '+%H:%M:%S')] 开始执行 ODS ETL" >> "$LOG_FILE"
echo "========================================" >> "$LOG_FILE"

if "$VENV_PYTHON" "$PROJECT_DIR/ods_ios_download.py" >> "$LOG_FILE" 2>&1; then
    echo "[$(date '+%H:%M:%S')] ODS iOS 下载 ETL 成功" >> "$LOG_FILE"
else
    echo "[$(date '+%H:%M:%S')] ODS iOS 下载 ETL 失败，退出" >> "$LOG_FILE"
    echo "ETL 结束: $(date '+%Y-%m-%d %H:%M:%S')" >> "$LOG_FILE"
    exit 1
fi

if "$VENV_PYTHON" "$PROJECT_DIR/ods_android_download.py" >> "$LOG_FILE" 2>&1; then
    echo "[$(date '+%H:%M:%S')] ODS Android 下载 ETL 成功" >> "$LOG_FILE"
    echo "" >> "$LOG_FILE"
else
    echo "[$(date '+%H:%M:%S')] ODS Android 下载 ETL 失败，退出" >> "$LOG_FILE"
    echo "ETL 结束: $(date '+%Y-%m-%d %H:%M:%S')" >> "$LOG_FILE"
    exit 1
fi

# ========================================
# DWD 层 ETL
# ========================================
echo "========================================" >> "$LOG_FILE"
echo "[$(date '+%H:%M:%S')] 开始执行 DWD ETL" >> "$LOG_FILE"
echo "========================================" >> "$LOG_FILE"
if "$VENV_PYTHON" "$PROJECT_DIR/dwd_event_log.py" >> "$LOG_FILE" 2>&1; then
    echo "" >> "$LOG_FILE"
    echo "[$(date '+%H:%M:%S')] DWD ETL 成功" >> "$LOG_FILE"
    echo "" >> "$LOG_FILE"
else
    echo "" >> "$LOG_FILE"
    echo "[$(date '+%H:%M:%S')] DWD ETL 失败，退出" >> "$LOG_FILE"
    echo "ETL 结束: $(date '+%Y-%m-%d %H:%M:%S')" >> "$LOG_FILE"
    exit 1
fi

if "$VENV_PYTHON" "$PROJECT_DIR/dwd_download.py" >> "$LOG_FILE" 2>&1; then
    echo "" >> "$LOG_FILE"
    echo "[$(date '+%H:%M:%S')] DWD 下载 ETL 成功" >> "$LOG_FILE"
    echo "" >> "$LOG_FILE"
else
    echo "" >> "$LOG_FILE"
    echo "[$(date '+%H:%M:%S')] DWD 下载 ETL 失败，退出" >> "$LOG_FILE"
    echo "ETL 结束: $(date '+%Y-%m-%d %H:%M:%S')" >> "$LOG_FILE"
    exit 1
fi

# ========================================
# DWS 层 ETL
# ========================================
echo "========================================" >> "$LOG_FILE"
echo "[$(date '+%H:%M:%S')] 开始执行 DWS ETL" >> "$LOG_FILE"
echo "========================================" >> "$LOG_FILE"
if "$VENV_PYTHON" "$PROJECT_DIR/dws_daily.py" >> "$LOG_FILE" 2>&1; then
    echo "" >> "$LOG_FILE"
    echo "[$(date '+%H:%M:%S')] DWS ETL 成功" >> "$LOG_FILE"
    echo "" >> "$LOG_FILE"
else
    echo "" >> "$LOG_FILE"
    echo "[$(date '+%H:%M:%S')] DWS ETL 失败，退出" >> "$LOG_FILE"
    echo "ETL 结束: $(date '+%Y-%m-%d %H:%M:%S')" >> "$LOG_FILE"
    exit 1
fi

# ========================================
# ADS 层 ETL
# ========================================
echo "========================================" >> "$LOG_FILE"
echo "[$(date '+%H:%M:%S')] 开始执行 ADS ETL" >> "$LOG_FILE"
echo "========================================" >> "$LOG_FILE"
if "$VENV_PYTHON" "$PROJECT_DIR/ads_daily.py" >> "$LOG_FILE" 2>&1; then
    echo "" >> "$LOG_FILE"
    echo "[$(date '+%H:%M:%S')] ADS ETL 成功" >> "$LOG_FILE"
    echo "" >> "$LOG_FILE"
else
    echo "" >> "$LOG_FILE"
    echo "[$(date '+%H:%M:%S')] ADS ETL 失败" >> "$LOG_FILE"
    echo "ETL 结束: $(date '+%Y-%m-%d %H:%M:%S')" >> "$LOG_FILE"
    exit 1
fi

# 记录结束时间
echo "========================================" >> "$LOG_FILE"
echo "ETL 全部完成: $(date '+%Y-%m-%d %H:%M:%S')" >> "$LOG_FILE"
echo "========================================" >> "$LOG_FILE"
echo "" >> "$LOG_FILE"

exit 0
