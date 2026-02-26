# Decom Track Log Cloud Run 服务

这是一个基于Google Cloud Run的服务，用于处理Cloud Storage事件驱动的日志文件处理，将日志数据写入BigQuery。

## 功能特性

- 🚀 **事件驱动**: 通过Cloud Storage事件自动触发处理
- 📊 **BigQuery集成**: 自动将解析的日志数据写入BigQuery
- 🔄 **批量处理**: 支持大文件的批量数据插入
- 📝 **日志跟踪**: 记录处理状态到专门的日志表
- 🛡️ **错误处理**: 完善的异常处理和日志记录

## 架构说明

```
Cloud Storage (archived/*.log) 
    ↓ (事件触发)
Eventarc Trigger 
    ↓ (HTTP请求)
Cloud Run Service 
    ↓ (解析并写入)
BigQuery (decom.decom_track_log)
```

## 文件结构

```
decom-track-log-cloudrun/
├── main.py                 # 主要的Cloud Run处理函数
├── applog.py              # 日志解析相关类和函数
├── utils.py               # 工具函数
├── requirements.txt       # Python依赖
└── README.md             # 本文档
```

## 快速开始

### 1. 环境准备

确保你有以下权限和工具：
- Google Cloud SDK (`gcloud`)
- Docker
- 对GCP项目的Cloud Run、BigQuery、Cloud Storage权限

### 2. 配置项目【注意CICD流程线功能没验证，目前直接复制代码到Cloud Console编辑更新】

```bash
# 设置项目ID
export PROJECT_ID="your-project-id"
export REGION="us-central1"
export BUCKET_NAME="your-bucket-name"

# 认证
gcloud auth login
gcloud config set project $PROJECT_ID
```

### 3. 验证部署

上传测试文件到Cloud Storage：
```bash
# 创建测试日志文件
echo '{"event_name":"test_event","logAt":1694419200000,"session_id":"test_session","properties":{"device_id":"test_device","user_id":"test_user","os":"web"},"ext":{},"args":{},"country":"US"}' > test.log

# 上传到触发目录
gsutil cp test.log gs://decom-prod-client-log/archived/test/test.log
```

查看处理日志：
```bash
gcloud logging read 'resource.type=cloud_run_revision AND resource.labels.service_name=decom-track-log-processor' --limit=10
```

### 4. 初始化部分 oss_key 的数据

```sql
-- 以 2025-12-30 为示例，初始化该日期的 oss_key 数据
-- 备份原始表
CREATE TABLE IF NOT EXISTS decom.decom_track_log_backup
LIKE decom.decom_track_log;

CREATE TABLE IF NOT EXISTS decom.oss_key_process_log_backup
LIKE decom.oss_key_process_log;

CREATE TABLE IF NOT EXISTS decom.decom_track_log_algorithm_backup
LIKE decom.decom_track_log_algorithm;

-- 初始化除 2025-12-30 以外的 oss_key 数据
INSERT INTO decom.decom_track_log_backup
SELECT * FROM decom.decom_track_log
WHERE oss_key NOT LIKE '%2025-12-30%';

INSERT INTO decom.oss_key_process_log_backup
SELECT * FROM decom.oss_key_process_log
WHERE oss_key NOT LIKE '%2025-12-30%';

INSERT INTO decom.decom_track_log_algorithm_backup
SELECT * FROM decom.decom_track_log_algorithm
WHERE oss_key NOT LIKE '%2025-12-30%';

-- 删除原始表
DROP TABLE decom.decom_track_log;
DROP TABLE decom.oss_key_process_log;
DROP TABLE decom.decom_track_log_algorithm;

-- 重命名备份表为原始表
ALTER TABLE decom.decom_track_log_backup RENAME TO decom.decom_track_log;
ALTER TABLE decom.oss_key_process_log_backup RENAME TO decom.oss_key_process_log;
ALTER TABLE decom.decom_track_log_algorithm_backup RENAME TO decom.decom_track_log_algorithm;

-- 重建 key 文件，使触发器接收任务
gsutil cp -r "gs://decom-prod-client-log/archived/2025-12-30" "gs://decom-prod-client-log/tmp/2025-12-30"
gsutil rm -r "gs://decom-prod-client-log/archived/2025-12-30"
gsutil cp -r "gs://decom-prod-client-log/tmp/2025-12-30" "gs://decom-prod-client-log/archived/2025-12-30"
gsutil rm -r "gs://decom-prod-client-log/tmp/2025-12-30"
```

### 5. 误删 key 文件的还原操作

```bash
# 1. 确保 GCS 开启了 versioning
gsutil versioning get gs://decom-prod-client-log

# 2. 因为每个 key 理论上只有一个版本，所以将老版本的文件直接 cp 到原路径中
# 执行脚本为 decom-track-log-cloudrun/restore_versions.sh
```
## BigQuery表结构

### decom.decom_track_log
主要的日志数据表，包含以下字段：
- `logAt`: 日志时间戳（毫秒）
- `event_name`: 事件名称
- `logAt_timestamp`: 格式化的时间戳
- `logAt_day`: 日志日期
- `session_id`: 会话ID
- `prop_*`: 属性字段（device_id, user_id, os等）
- `ext`: 扩展数据（JSON）
- `args`: 参数数据（JSON）
- `oss_create_at`: 文件创建时间
- `oss_key`: 文件路径
- `country`: 国家代码

### decom.oss_key_process_log
处理状态跟踪表：
- `oss_key`: 文件路径
- `record_state`: 记录状态（1=已处理）
- `analyze_state`: 分析状态（0=未分析）

## 配置说明

### 环境变量
- `GCP_PROJECT`: GCP项目ID（可选，默认使用当前项目）
- `PORT`: 服务端口（默认8080）

### 文件过滤规则
服务只处理满足以下条件的文件：
1. 路径以 `archived/` 开头
2. 文件扩展名为 `.log`
3. 文件路径晚于 `archived/2025-09-05/09_51.0.172.20.0.4.log`

## 监控和调试

### 查看服务日志
```bash
gcloud logging read 'resource.type=cloud_run_revision AND resource.labels.service_name=decom-track-log-processor' --limit=50
```

### 查看触发器状态
```bash
gcloud eventarc triggers list --location=$REGION
```

### 测试单个文件处理
```bash
# 本地测试
python main.py gs://your-bucket/archived/2025-09-11/test.log
```

## 故障排除

### 常见问题

1. **触发器未激活**
   - 检查Eventarc API是否启用
   - 验证服务账号权限
   - 确认存储桶名称正确

2. **BigQuery插入失败**
   - 检查表结构是否匹配
   - 验证数据格式是否正确
   - 确认BigQuery权限

3. **文件下载失败**
   - 检查Cloud Storage权限
   - 验证文件路径格式
   - 确认文件存在

### 日志级别
- ✅ 成功操作
- ❌ 错误信息
- ⚠️ 警告信息
- 📋 信息日志

## 性能优化

- **批量插入**: 每1000行批量插入BigQuery
- **内存管理**: 逐行处理避免大文件内存溢出
- **并发控制**: Cloud Run配置适当的并发数
- **超时设置**: 3600秒超时适应大文件处理

## 安全考虑

- 使用IAM服务账号进行身份验证
- 最小权限原则配置权限
- 敏感数据不记录到日志
- 使用HTTPS进行所有通信

## 扩展功能

可以考虑添加的功能：
- 数据验证和清洗
- 重试机制
- 死信队列
- 指标监控
- 告警通知

## 联系支持

如有问题，请查看：
1. Cloud Run服务日志
2. Eventarc触发器状态
3. BigQuery作业历史
4. Cloud Storage访问日志