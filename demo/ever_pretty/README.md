# Ever Pretty 本地 Superset Demo

使用本仓库 Python → BigQuery → Superset 链路。本地元数据库单独放在 `logs/ever_pretty_demo/superset/`；不会操作线上 Superset。

## 安装和启动

在仓库根目录执行（Python 3.11）：

```bash
uv venv --python 3.11 logs/ever_pretty_demo/.venv
uv pip install --python logs/ever_pretty_demo/.venv/bin/python -r demo/ever_pretty/requirements.txt
python3 scripts/run_ever_pretty_demo.py --init
python3 scripts/run_ever_pretty_demo.py
```

本地地址：`http://127.0.0.1:8089/`。本地登录凭据保存在 `logs/ever_pretty_demo/superset/login.json`，不要提交或复制到文档。初始化完成后仅需执行最后一条启动命令。

BigQuery 表和 `logs/ever_pretty_demo/preview.json` 准备好后，另开终端配置本地看板：

```bash
logs/ever_pretty_demo/.venv/bin/python -B scripts/setup_ever_pretty_superset.py
```

看板地址：`http://127.0.0.1:8089/superset/dashboard/ever-pretty-video-daily/`。配置脚本仅访问本地实例，并验证图表实际查询结果。

## BigQuery 表

目标表：`my-project-8584-jetonai.decom.ads_daily_ever_pretty_video`。

名单单点来源：`env/ever_pretty_accounts.json`。从 `2026-09-01` 发布的视频开始，一视频一天一行；默认处理到美东当天，未结束自然日有明确标记。

```bash
uv run --with google-cloud-bigquery python -B jobs/ads/ads_daily_ever_pretty_video.py --dry-run
uv run --with google-cloud-bigquery python -B jobs/ads/ads_daily_ever_pretty_video.py --output logs/ever_pretty_demo/preview.json
```

首次创建需要具有建表权限的 ADC，可通过 `GOOGLE_APPLICATION_CREDENTIALS` 指向仓库外配置。显式传 `--write` 才会创建新表：先建唯一 staging、校验逐日覆盖和指标、再改名。目标已存在时 `--write` 拒绝覆盖。用户明确调整本 Demo 范围时可执行 `--rebuild-demo`：先建唯一 staging、验证全部视频每日覆盖及既有主键不丢失、保存七天备份，再替换固定 Demo 表；不刷新上游。

## 查看

本地看板默认展示整体累计播放折线图、播放记录增量柱状图、当天采集视频数，以及点赞和评论趋势。博主 → 平台 → 视频标题为级联单选筛选，按需聚焦，不平铺全部博主或视频。累计值不能跨日求和；首次采集的增量为空，后续差值表示相邻记录变化，不能保证全部增长发生于采集当天。未采集用空值；沿用旧快照单独标注；YouTube 分享为空。

## 部署边界

当前仅新增 Demo 表和本地文件，未修改主 `env/etl_config.json` 或服务器 cron，未发布线上 Superset。用户验收后再沿仓库交接流程部署：提交代码 → 服务器拉取 → 为本主题配置经确认的安全刷新任务 → Superset API 注册 dataset / chart / dashboard 并回查。`--rebuild-demo` 是显式 Demo 回填工具，不能直接当作周期刷新任务启用。
