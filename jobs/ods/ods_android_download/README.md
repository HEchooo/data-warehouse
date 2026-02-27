# Android 下载量数据

从 Google Play Console 导出的安装统计数据。

## 数据源

- 存储位置: Google Cloud Storage (`gs://pubsite_prod_5101881267809973114/stats/installs/`)
- 文件格式: CSV (UTF-16 编码)
- 文件命名: `installs_{package_name}_{YYYYMM}_country.csv`

## 字段说明

| 字段名 | 类型 | 说明 |
|--------|------|------|
| date | DATE | 日期 |
| package_name | VARCHAR(100) | 应用包名 |
| country | VARCHAR(10) | 国家/地区代码 (ISO 3166-1 alpha-2) |
| daily_device_installs | INT | 每日设备安装数 - 当天新安装该应用的设备数量 |
| daily_device_uninstalls | INT | 每日设备卸载数 - 当天卸载该应用的设备数量 |
| daily_device_upgrades | INT | 每日设备升级数 - 当天更新该应用到新版本的设备数量 |
| total_user_installs | INT | 累计用户安装数 - 安装过该应用的总用户数（不含卸载） |
| daily_user_installs | INT | 每日用户安装数 - 当天新安装该应用的用户数量 |
| daily_user_uninstalls | INT | 每日用户卸载数 - 当天卸载该应用的用户数量 |
| active_device_installs | INT | 活跃设备安装数 - 当前安装该应用的设备总数 |
| install_events | INT | 安装事件数 - 当天发生的安装事件总数（含重新安装） |
| update_events | INT | 更新事件数 - 当天发生的应用更新事件数 |
| uninstall_events | INT | 卸载事件数 - 当天发生的卸载事件总数 |

## Device 与 User 的区别

- **Device**: 以设备为单位统计，同一用户的多台设备分别计数
- **User**: 以 Google 账号为单位统计，同一用户只计一次

## 运行方式

```bash
python ods_android_download.py
```

默认获取当前月份的数据并插入 BigQuery。

## 依赖

```bash
pip install pandas google-cloud-storage google-cloud-bigquery
```
