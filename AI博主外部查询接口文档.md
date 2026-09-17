# AI Blogger External Lookup API

版本日期：2026-09-16

本文档描述两个对外查询接口：

1. 查询平台账号绑定的 AI 博主联盟归属。
2. 查询平台视频首次进入投流发送的时间。

## Base URL

请使用生产环境现有 ECN API 域名：

```text
https://<ECN_API_HOST>
```

实际路径均带 `/api/v1` 前缀。

## 鉴权

两个接口都使用统一外部调用鉴权方式：`X-API-Key` 请求头。

生产配置项：

```text
ACCOUNT_CHANNEL_API_KEY
```

请求头：

```http
X-API-Key: <ACCOUNT_CHANNEL_API_KEY>
```

安全说明：真实生产 key 属于密钥，不能写入接口文档或对话记录。请由有生产配置权限的人员从生产环境 `ACCOUNT_CHANNEL_API_KEY` 配置项中获取，并替换上面的占位符。

鉴权失败返回：

```json
{
  "detail": "Invalid api_key"
}
```

如果服务端没有配置 key，返回：

```json
{
  "detail": "ACCOUNT_CHANNEL_API_KEY 未配置"
}
```

## 通用约定

- 请求方法均为 `GET`。
- 不需要 JSON request body。
- 参数通过 query string 传入。
- `platform` 只允许以下值：
  - `tiktok`
  - `youtube`
  - `instagram`
- `account_identifier` 支持带或不带 `@`，匹配时会去掉开头的 `@`，并忽略大小写。
- `video_id` 会去掉首尾空格后匹配平台视频 ID。
- 时间字段返回 ISO 8601 格式。

## 1. 查询账号联盟归属

### Endpoint

```http
GET /api/v1/open-api/account-affiliation
```

### Query 参数

| 参数 | 类型 | 必填 | 说明 |
|---|---|---:|---|
| `platform` | string | 是 | 平台。可选值：`tiktok`、`youtube`、`instagram`。 |
| `account_identifier` | string | 是 | 平台账号名、handle 或账号展示名。长度 1 到 300。可带 `@`。 |

### Request Body

无。

### 请求示例

```bash
curl -G 'https://<ECN_API_HOST>/api/v1/open-api/account-affiliation' \
  -H 'X-API-Key: <ACCOUNT_CHANNEL_API_KEY>' \
  --data-urlencode 'platform=tiktok' \
  --data-urlencode 'account_identifier=@MadeBySimone'
```

### 成功返回示例

```json
{
  "platform": "tiktok",
  "account_identifier": "@MadeBySimone",
  "ai_kol_id": "11111111-1111-4111-8111-111111111111",
  "binding_source": "kol_platform_account",
  "platform_account_id": "22222222-2222-4222-8222-222222222222",
  "account_channel_reservation_id": null,
  "legacy_account_id": null,
  "affiliation": "affiliate_us",
  "affiliation_label": "联盟美国"
}
```

联盟墨西哥示例：

```json
{
  "platform": "youtube",
  "account_identifier": "sonny_winter_7days",
  "ai_kol_id": "33333333-3333-4333-8333-333333333333",
  "binding_source": "account_channel_reservation",
  "platform_account_id": null,
  "account_channel_reservation_id": "44444444-4444-4444-8444-444444444444",
  "legacy_account_id": "55555555-5555-4555-8555-555555555555",
  "affiliation": "affiliate_mexico",
  "affiliation_label": "联盟墨西哥"
}
```

### 字段说明

| 字段 | 类型 | 说明 |
|---|---|---|
| `platform` | string | 查询的平台。 |
| `account_identifier` | string | 本次请求传入的账号标识。 |
| `ai_kol_id` | UUID | 匹配到的 AI 博主 ID。 |
| `binding_source` | string | 实际命中的绑定来源。可能为 `kol_platform_account` 或 `account_channel_reservation`。 |
| `platform_account_id` | UUID 或 null | 新平台账号绑定记录 ID。 |
| `account_channel_reservation_id` | UUID 或 null | 旧账号渠道绑定记录 ID。 |
| `legacy_account_id` | UUID 或 null | AI 博主关联的旧账号 ID。 |
| `affiliation` | string | 机器可读联盟归属。可能为 `affiliate_us` 或 `affiliate_mexico`。 |
| `affiliation_label` | string | 中文联盟标签。可能为 `联盟美国` 或 `联盟墨西哥`。 |

### 404 返回示例

账号未找到：

```json
{
  "detail": {
    "code": "platform_account_not_found",
    "message": "未找到平台 tiktok 下绑定该账号标识的 AI 博主"
  }
}
```

账号存在，但没有联盟标签：

```json
{
  "detail": {
    "code": "affiliation_label_not_found",
    "message": "该 AI 博主没有联盟标签"
  }
}
```

账号存在，但不是联盟美国或联盟墨西哥：

```json
{
  "detail": {
    "code": "affiliation_not_supported",
    "message": "该 AI 博主不是联盟美国或联盟墨西哥"
  }
}
```

### 409 返回示例

同一平台账号标识匹配到多个不同 AI 博主时返回：

```json
{
  "detail": {
    "code": "platform_account_ambiguous",
    "message": "平台 instagram 下该账号标识匹配到多个 AI 博主"
  }
}
```

说明：如果同一个 AI 博主同时存在旧渠道绑定记录和同步后的新平台账号记录，系统会按同一个 `ai_kol_id` 去重，不会返回 409。

## 2. 查询视频首次投流发送时间

### Endpoint

```http
GET /api/v1/open-api/videos/promotion-entry
```

### Query 参数

| 参数 | 类型 | 必填 | 说明 |
|---|---|---:|---|
| `platform` | string | 是 | 平台。可选值：`tiktok`、`youtube`、`instagram`。 |
| `video_id` | string | 是 | 平台视频 ID。长度 1 到 300。 |

### Request Body

无。

### 请求示例

```bash
curl -G 'https://<ECN_API_HOST>/api/v1/open-api/videos/promotion-entry' \
  -H 'X-API-Key: <ACCOUNT_CHANNEL_API_KEY>' \
  --data-urlencode 'platform=tiktok' \
  --data-urlencode 'video_id=7678536245286341895'
```

### 成功返回示例

```json
{
  "ads_first_sent_at": "2026-09-15T08:42:00Z"
}
```

### 字段说明

| 字段 | 类型 | 说明 |
|---|---|---|
| `ads_first_sent_at` | datetime | 该视频首次开始投流发送的时间。 |

### 404 返回示例

未找到待投放记录：

```json
{
  "detail": {
    "code": "pending_list_record_not_found",
    "message": "未找到平台 tiktok 视频 ID 对应的待投放记录"
  }
}
```

已进入待投放列表，但没有投流发送记录：

```json
{
  "detail": {
    "code": "ads_delivery_not_found",
    "message": "该视频已进入待投放列表，但没有投流发送记录"
  }
}
```

已进入待投放列表，但尚未开始投流发送：

```json
{
  "detail": {
    "code": "ads_delivery_not_started",
    "message": "该视频已进入待投放列表，但尚未开始投流发送"
  }
}
```

### 409 返回示例

同一平台视频 ID 对应多条待投放记录时返回：

```json
{
  "detail": {
    "code": "pending_list_record_ambiguous",
    "message": "平台 tiktok 视频 ID 对应多条待投放记录"
  }
}
```

## HTTP 状态码汇总

| 状态码 | 场景 |
|---:|---|
| 200 | 查询成功。 |
| 401 | `X-API-Key` 缺失或错误。 |
| 404 | 未找到匹配账号、视频，或账号没有可返回的联盟标签，或视频未开始投流。 |
| 409 | 输入匹配到多条互相冲突的数据。 |
| 422 | 参数校验失败，例如 `platform` 不在允许值内。 |
| 500 | 服务端租户配置错误。 |
| 503 | 服务端未配置 `ACCOUNT_CHANNEL_API_KEY`。 |

