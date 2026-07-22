# 【Social Content】疑似限流账号监控踩坑记录

## dense T+1 状态不能直接代表真实 T+1 观测

- `dws_social_video_state_daily_v2` 会在某天没有真实采集时沿用最近一次累计值，适合日末状态查询，但不等于 T+1 当天一定采到了平台数据
- 实际抽查发现部分 YouTube 发布日期的 T+1 真实快照覆盖率只有约 67%～80%；如果直接使用 dense state，会把采集缺失误判为播放低迷
- 疑似限流中间表应直接以 `dws_social_video_snapshot_daily_v2` 为驱动，只让 T+1 当天存在真实快照的视频进入最近发布次数和周均发布量计算

## Lark 卡片推送：单卡片最多 5 个 table 组件

- 现象：ECN Lark 机器人（`jobs/ads/ads_daily_ecn_lark_push.py`）把三个账号级榜单加成 table 后发送失败，返回 `code=11246 / ErrCode: 11310; card table number over limit`
- 原因：Lark 交互卡片单张卡片最多放 5 个 `table` 组件（配置多语言时每语言各 5 个），原 ECN 卡片已占满 5 张表，再加即超限
- 处理：业务方要求保留 table 展示，因此在**同一个脚本内构建并发送两张卡片**（`send_lark_message` 调两次、同一 webhook），第二张卡片承载三个账号播放波动榜，5 表上限按卡片独立计数；不需要新增脚本
- 备选（不需要 table 时）：把榜单改成 `markdown` 文本渲染，不占 table 配额

## Lark 卡片 markdown 标题：`#` 越少字号越大，`###` 比正文还小

- 现象：模块标题用 `### xxx` 渲染出来比下面 `**加粗**` 正文还小
- 原因：Lark 卡片 markdown 的标题字号规则是「`#` 数量越少字号越大」，`###`（h3）已偏小
- 处理：模块大标题用 `#`（h1）最大号；需要更克制的层级再往下调 `##`/`###`
