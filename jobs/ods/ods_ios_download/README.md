# iOS 销售报告字段说明

本文档说明 `ods_ios_download` 表中各字段的含义，数据来源于 Apple App Store Connect Sales Report API。

---

## 数据表字段列表

| 字段名 | 数据类型 | 说明 |
|--------|----------|------|
| `report_date` | DATE | 报告日期（YYYY-MM-DD）|
| `provider` | STRING | 提供商 |
| `provider_country` | STRING | 提供商国家代码 |
| `sku` | STRING | 应用 SKU 标识符 |
| `developer` | STRING | 开发者法定名称 |
| `title` | STRING | 应用名称 |
| `version` | STRING | 应用版本号 |
| `product_type_identifier` | INTEGER | 产品类型标识符（见下方枚举值）|
| `units` | INTEGER | 下载/更新/购买数量 |
| `developer_proceeds` | FLOAT | 开发者收益（客户价格 - 税 - Apple 佣金）|
| `begin_date` | STRING | 报告开始日期（MM/DD/YYYY，太平洋时间 PT）|
| `end_date` | STRING | 报告结束日期（MM/DD/YYYY，太平洋时间 PT）|
| `customer_currency` | STRING | 客户支付货币代码 |
| `country_code` | STRING | 用户 Apple ID 所属国家代码（ISO 3166-1 alpha-2）|
| `currency_of_proceeds` | STRING | 收益结算货币代码 |
| `apple_identifier` | STRING | Apple ID，应用的唯一数字标识 |
| `customer_price` | FLOAT | 客户支付价格，0.0 表示免费 |
| `promo_code` | STRING | 促销码 |
| `parent_identifier` | STRING | 父产品标识符（用于应用套装）|
| `subscription` | STRING | 订阅信息 |
| `period` | STRING | 报告周期 |
| `category` | STRING | App Store 应用分类 |
| `cmb` | STRING | 自定义消息总线 |
| `device` | STRING | 设备类型 |
| `supported_platforms` | STRING | 支持的平台 |
| `proceeds_reason` | STRING | 收益原因 |
| `preserved_pricing` | STRING | 保留定价信息 |
| `client` | STRING | 客户端信息 |
| `order_type` | STRING | 订单类型 |

---

## 枚举值说明

### product_type_identifier（产品类型标识符）

这是最重要的分类字段，表示交易类型：

| 标识符 | 类型 | 说明 |
|--------|------|------|
| **1** | 免费或付费应用 | 首次下载（iOS、iPadOS、visionOS、watchOS）|
| **3** | 重新下载 | 用户重新安装之前下载过的应用 |
| **7** | 更新 | 应用更新（免费）|
| **1T** | iPad 应用 | iPad 专属应用 |
| **1-B** | 应用套装 | iOS 应用套装（App Bundle）|
| **F1-B** | 应用套装 | Mac 应用套装 |
| **IA1** | 应用内购买 | In-App Purchase（iOS、iPadOS、visionOS）|
| **IA1-M** | 应用内购买 | In-App Purchase（Mac）|
| **IA3** | 恢复购买 | 非消耗性应用内购买恢复 |
| **IA9** | 应用内购买 | 非续订订阅 |

> **注意**：统计「新增下载量」时应只统计 `product_type_identifier = 1`，排除更新（7）和重新下载（3）。

### device（设备类型）

| 值 | 说明 |
|----|------|
| `iPhone` | iPhone 设备 |
| `iPad` | iPad 设备 |

### supported_platforms（支持平台）

| 值 | 说明 |
|----|------|
| `iOS` | iOS 平台 |

### provider（提供商）

| 值 | 说明 |
|----|------|
| `APPLE` | Apple 官方 |

### provider_country（提供商国家）

| 值 | 说明 |
|----|------|
| `US` | 美国 |

---

## 常用查询示例

### 统计新增下载量（首次下载）
```sql
SELECT SUM(units) as new_downloads
FROM ods_ios_download
WHERE product_type_identifier = 1
```

### 统计更新量
```sql
SELECT SUM(units) as updates
FROM ods_ios_download
WHERE product_type_identifier = 7
```

### 按国家统计新增下载
```sql
SELECT country_code, SUM(units) as total
FROM ods_ios_download
WHERE product_type_identifier = 1
GROUP BY country_code
ORDER BY total DESC
```

### 按日期和设备统计下载
```sql
SELECT report_date, device, SUM(units) as total
FROM ods_ios_download
WHERE product_type_identifier = 1
GROUP BY report_date, device
ORDER BY report_date DESC
```

### 查询某天的原始明细数据
```sql
SELECT *
FROM ods_ios_download
WHERE report_date = '2026-02-23'
ORDER BY units DESC
```

---

## 数据示例分析

以报告中的一行数据为例：

| 字段 | 值 |
|------|-----|
| report_date | 2026-02-23 |
| product_type_identifier | 1 |
| units | 3 |
| country_code | US |
| customer_currency | USD |
| device | iPhone |
| customer_price | 0.0 |
| category | Shopping |

**解读**：
- 这是 3 次**首次下载**（product_type_identifier = 1）
- 来自美国用户（country_code = US）
- 使用 iPhone 下载（device = iPhone）
- 免费应用（customer_price = 0.0）
- 购物类应用（category = Shopping）

---

## 时区说明

报告基于**太平洋时间（PT）**，一天包含从凌晨 12:00 到晚上 11:59（PT）的交易。

---

## 参考文档

- [Apple Developer - Product type identifiers](https://developer.apple.com/help/app-store-connect/reference/reporting/product-type-identifiers/)
- [Apple Developer - Sales and Trends metrics](https://developer.apple.com/help/app-store-connect/reference/reporting/sales-and-trends-metrics-and-dimensions/)
- [Apple Developer - Financial report fields](https://developer.apple.com/help/app-store-connect/reference/financial-report-fields/)
