---
name: superset
description: 通过 Superset REST API 直接创建或修改 dataset、chart、dashboard、dashboard filter 与图表说明。只要用户提到用代码操作 Superset、创建或修改 dataset/chart/dashboard/filter、给图表或字段加口径说明、排查这些对象的 API/权限/路径问题，就使用这个 skill。这个 skill 只依赖 `SKILL.md` 里的执行协议，不依赖项目脚本，也不要退回浏览器 UI 自动化。
---

# Superset API 执行协议

这个 skill 只覆盖这几类流程：

- 登录并确认身份
- 查找、创建、修改 dataset
- 查找、创建、修改 chart，包括图表说明 `description`
- 查找、创建、修改 dashboard
- 查找、创建、修改 dashboard native filter

除此之外的内容，例如导入导出资产、SQL Lab、项目专用 builder、registry、CLI，不属于这个 skill 的范围。

## 核心原则

1. 先定义“对象 + 动作”，再找接口。
2. 先看实例自己的 OpenAPI，不要凭记忆猜 path、method、payload。
3. 任何修改前都先读取现状，不要盲写。
4. 对象有依赖顺序，通常按 `dataset -> chart -> dashboard -> filter` 处理。
5. 这个 skill 不依赖本地脚本。直接使用可用的 HTTP 请求方式完成操作即可。
6. 写完之后必须回查验证，不要只看写请求返回成功。

## 执行前检查

开始任何操作前，先做这几件事：

1. 读取项目内 `env/superset.json`。
2. 不要把账号、密码、token 写进日志、文档、memory 或最终汇报。
3. 不要把 `env/superset.json` 里的账号密码直接拼进 shell 命令字符串，尤其不要写进 `curl -d '...'`、`python -c '...'` 这一类会经过 shell 解释的片段。
4. 如果密码里包含 `$`、`$(`、反引号、引号、空格、`!` 等特殊字符，shell 可能发生变量展开、命令替换或转义破坏，导致“手动登录成功，但脚本登录失败”的假象。
5. 优先用“先读取 JSON，再由 Python `requests` 或其他结构化 HTTP 客户端发请求”的方式传递凭据，避免 shell 插值。
6. 先验证实例 URL、登录方式、当前身份是否成立。
7. 只有在认证通过后，才继续 dataset/chart/dashboard/filter 相关操作。

如果连当前用户信息都拿不到，不要继续写资源接口。

## 行为到接口族

### 1. 登录、鉴权、身份确认

当目标是下面这些行为时，优先找安全与身份接口：

- 登录
- 获取 access token
- 确认当前用户
- 排查 401 / 403
- 排查 CSRF / 会话问题

对应接口族：

- `security/login`
- `me`
- CSRF 或安全辅助接口

### 2. 查询资源现状

当目标是下面这些行为时，优先找资源查询接口：

- 看对象是否存在
- 列出对象
- 查看详情
- 获取已有对象结构，作为后续修改依据

对应接口族：

- `/api/v1/<resource>/`
- `/api/v1/<resource>/<id>`

常见资源：

- `database`
- `dataset`
- `chart`
- `dashboard`

### 3. 创建或更新 dataset

当目标是下面这些行为时，优先找 dataset 接口：

- 注册物理表到 Superset
- 修正 database / schema / table 绑定
- 修正时间列、列定义、指标定义

对应接口族：

- `dataset`
- 必要时联动 `database`

判断要点：

- dataset 是 chart 和 dashboard 的前置依赖。
- chart 失败时，先回查 dataset 是否存在、字段是否可见、指标是否可用。

### 4. 创建或更新 chart

当目标是下面这些行为时，优先找 chart 接口：

- 新建图表
- 更新查询参数、可视化配置、标题
- 复制现有图表结构后微调

对应接口族：

- `chart`

判断要点：

- chart 依赖 dataset。
- payload 较长时，先读取已有 chart 详情作为结构参考，不要从零猜字段。
- dashboard / chart 命名不要机械复用历史前缀。创建前先回看当前主题文档、已有 dashboard 名称和业务域；像 Makepod 这类独立域名主题，不要默认套 `【AI-Fashion】` 前缀。

### 5. 创建或更新 dashboard

当目标是下面这些行为时，优先找 dashboard 接口：

- 新建 dashboard
- 更新标题、slug、布局、native filters
- 将多个 chart 组织进一个 dashboard

对应接口族：

- `dashboard`

判断要点：

- dashboard 通常依赖 chart 已存在。
- 如果目标是“改看板”，先分清要改的是 dashboard 自身元信息，还是其中 chart 的配置。
- 用户没有明确要求时，创建和更新 dashboard 都不要写 `roles`。对照已有看板时只借鉴布局、筛选器和图表，不要把访问角色一起抄过来。改布局、图表或筛选器时，更新请求里也不要带 `roles`，否则一次改图会把权限再写回去。
- 只写 `position_json` 里的 `CHART-*` 节点**不足以**让图表渲染：还必须建立 chart ↔ dashboard 的归属关系，否则前端报 `There is no chart definition associated with this component, could it have been deleted?`，看板上是一个空白容器。
  - 建立方式：`POST /api/v1/chart/` 时带 `dashboards: [<dashboard_id>]`，或事后 `PUT /api/v1/chart/{pk}` 补 `dashboards`；同时在 `params` 与 `query_context.form_data` 里写入同样的 `dashboards` 数组。
  - 先建 dashboard 再回填 chart 归属时，`POST /api/v1/dashboard/` 拿不到还不存在的 chart 关系，所以这一步必须显式补，不会自动推导。
  - 验收必须用 `GET /api/v1/dashboard/{id}/charts`：返回空数组就是归属没建成，此时 `position_json` 看起来完全正常，只看写请求返回 200 会漏掉这个问题。
  - `position_json` 的 `CHART-*.meta.uuid` 是 UI 编辑时生成的可选字段，当前实例的 chart API 不暴露 `uuid`，用 API 建看板时可以不写，不影响渲染。

### 6. 创建或更新 dashboard filter

当目标是下面这些行为时，优先找 dashboard 相关的 filter 配置字段或接口：

- 新建 dashboard native filter
- 修改 filter 默认值、可选项、作用范围
- 调整 filter 与 chart 的联动关系

判断要点：

- filter 通常属于 dashboard 元信息的一部分，而不是独立于 dashboard 的一级资源。
- 处理 filter 前，先确认目标 dashboard 已存在，且目标 chart 已挂到该 dashboard 上。
- 修改 filter 后，回查 dashboard 元信息，确认 filter 配置和作用对象都正确。

## 标准执行链路

真正落地时，按这个顺序推进：

1. 校验配置与认证。
2. 用 OpenAPI 确认目标 endpoint、字段和请求方法。
3. `GET` 读取现状，确认对象与依赖。
4. 按依赖顺序执行写操作：通常是 `dataset -> chart -> dashboard -> filter`。
5. 每完成一步就回查一次，确认新对象或新配置已经生效。
6. 如果是修改已有对象，优先最小化修改，不要把整份未知结构重写一遍。

不要跳过第 2 步和第 3 步直接构造写请求，这通常会导致无效调试。

## 关键接口模板

下面只写相对路径模板，不写实例域名。实际请求时，应从 `env/superset.json` 中读取实例地址，再拼接相对路径；如果实例存在前缀路径，以实例 OpenAPI 和实际返回为准。

### 通用探查

- `GET /api/v1/_openapi`
  用来确认当前实例有哪些 endpoint、字段名和请求结构。

### 登录与身份

- `POST /api/v1/security/login`
  登录获取 token。
- `GET /api/v1/me/`
  校验当前 token 对应的用户身份。
- `GET /api/v1/security/csrf_token/`
  部分写操作或特定部署下可能需要先取 CSRF token。

注意：

- 写接口不要只带 Bearer token；当前实例已验证可用的做法是同时带 `JWT + session cookie + CSRF token`。
- `GET /api/v1/me/` 在当前实例上不一定稳定返回完整身份信息；判断登录是否成立时，除了看 `me`，还要看受保护资源读取是否成功。
- 排查登录失败时，先排除“凭据在 shell 层被改写”的可能，再怀疑账号密码本身。特别是密码包含 `$(` 这类片段时，直接 shell 拼接很容易被当成命令替换。
- 如果必须走命令行，不要把明文密码内联到 shell 字符串里；优先让解释器直接读取 `env/superset.json`，再在进程内构造登录请求。

### dataset

- `GET /api/v1/dataset/`
  列出 dataset，常用于查重和筛选目标对象。
- `POST /api/v1/dataset/`
  创建 dataset。
- `GET /api/v1/dataset/{id}`
  读取单个 dataset 详情。
- `PUT /api/v1/dataset/{id}`
  修改已有 dataset。

注意：

- 当前实例的 dataset refresh 使用 `PUT /api/v1/dataset/{id}/refresh`；部分网关下需要显式发送空 body，例如 `-d ''`。

### chart

- `GET /api/v1/chart/`
  列出 chart，常用于查重和读取候选对象。
- `POST /api/v1/chart/`
  创建 chart。
- `GET /api/v1/chart/{id}`
  读取单个 chart 详情。
- `PUT /api/v1/chart/{id}`
  修改已有 chart。

注意：

- chart 修改时，要一起处理展示参数和实际查询配置；常见落点是 `params` 与 `query_context`，回查 datasource 时也要同时核对这两处。
- 当前实例里，涉及 Y 轴标题的 ECharts 系图表配置是通过 `PUT /api/v1/chart/{pk}` 一并更新的，不存在单独的 Y 轴标题接口；相关字段落在 `params` JSON 与 `query_context` JSON 的 `form_data` 中。
- 当需求涉及 `y_axis_title` 的新增、修改或保留时，默认同时规范这两个字段：`y_axis_title_margin = 20`、`y_axis_title_position = Top`；不要只改其中一处。
- 如果目标口径需要子查询、额外 join 或重算字段，不要硬塞进 chart 自定义 SQL 字段；优先放到 dataset SQL 或虚拟 dataset。
- 如果图表是固定分桶、固定 legend 顺序的 ECharts 柱状图 / 堆积图，优先在 dataset 提供稳定排序字段或“排序键 + 展示标签”两列，不要依赖 `post_processing(rename/pivot)` 先拼 `01|bucket` 再隐藏前缀；这个实例里该做法容易出现 dataframe 字段不匹配，或前端 tooltip / legend 顺序漂移。
- 如果必须把排序信息和展示标签合在一个字段里，优先使用“前端不可见但可稳定比较”的前缀方案；写完后必须实测返回顺序，而不是只看 SQL `ORDER BY`。
- 如果 `table` 图表在 edit chart 页面里已经给 `all_columns[].label` 配了中文，但 dashboard 里仍显示英文列名，优先同时检查 `params` 与 `query_context` 的列配置；当前实例实测可用的修正方式是给每个 `sqlExpression` 末尾补一个空格，再回查 dashboard 展示。
- 如果 `table` 图表展示数值字段，默认同时在 `params.column_config` 与 `query_context.form_data.column_config` 写入格式：绝对值使用 `d3NumberFormat = ".2f"`，比例 / 率 / 占比使用 `d3NumberFormat = ".2%"`。

图表说明是 chart 的顶层字段 `description`，不是 `params` 或 `column_config` 里的一项。当前实例是 Superset 4.0.2，展示方式如下：

- 看板上默认收起。打开图表右上角 `...`，选 `Show chart description`（中文界面是「显示图表描述」），正文出现在标题下面。
- 写入内容是 Markdown。`GET /api/v1/chart/{id}` 只返回原始 `description`。渲染后的 HTML 在 `GET /api/v1/dashboard/{id}/charts` 的 `description_markeddown`。验收展示是否生效，回查这个字段，不要只看 chart 详情。
- 只改说明时，`PUT /api/v1/chart/{pk}` 只传 `{"description": "..."}`。请求里没写的字段不会被清空；没传 `owners` 时保留原 owner。不要为了改说明重写 `params` 或 `query_context`。
- 写完回查三件事：`description` 与写入一致；`params`、`query_context`、`datasource_id`、`dashboards`、`owners` 没变；dashboard charts 的 `description_markeddown` 里有渲染后的正文。
- Dataset 列的 `description` 在这个版本不会出现在表格列头上，列头悬停只提示按住 Shift 多列排序。用户要给看看板的人看口径时，写图表说明，不要写列 Description。

### dashboard

- `GET /api/v1/dashboard/`
  列出 dashboard，常用于查重。
- `POST /api/v1/dashboard/`
  创建 dashboard。
- `GET /api/v1/dashboard/{id}`
  读取单个 dashboard 详情。
- `PUT /api/v1/dashboard/{id}`
  修改已有 dashboard。

### dashboard filter

dashboard native filter 往往不是独立一级资源，通常跟随 dashboard 元信息一起读写，因此优先按下面的思路处理：

- `GET /api/v1/dashboard/{id}`
  先读取 dashboard 当前元信息，确认 filter 配置现状。
- `PUT /api/v1/dashboard/{id}`
  通过 dashboard 更新接口回写 filter 配置。

判断要点：

- 常见情况下，filter 配置会落在 dashboard 元信息字段中，例如 `json_metadata` 下的 `native_filter_configuration`。
- 具体字段名和嵌套结构可能因版本不同而变化，因此必须先读取现有 dashboard 详情，再做最小修改。
- filter 的 `scope.excluded` 必须写数字 chart ID（如 `689`），不能写 `position_json` 里的 layout key（如 `"CHART-xxx-3"`）；写成字符串不会报错，但前端匹配不上，scoping 会静默失效，UI 里表现为所有 panel 全选。
- 修改 scope 时，同步把 `chartsInScope` 更新为对应的数字 chart ID 列表，保持两处一致。

## 各流程的最小工作流

### 登录

1. 读取 `env/superset.json`。
2. 在进程内解析 JSON 后再构造登录请求，不要先把密码插进 shell 命令字符串。
3. 调用登录接口拿 token。
4. 保留登录会话 cookie；如果后面有写操作，再取 CSRF token。
5. 调用当前用户接口或任一受保护资源接口，确认认证真正可用。

### dataset

1. 先查同 schema、同 table_name 的 dataset 是否已存在。
2. 不存在则创建；存在则读取详情后按需修改。
3. 如果改的是 dataset SQL、字段或指标，必要时调用 refresh 更新列信息。
4. 确认 database、schema、table、时间列、字段与指标定义符合预期。

### chart

1. 先确认 dataset 已存在。
2. 如已有同目标 chart，先读取现状，不要盲目重复创建。
3. 创建或修改时，同时更新展示参数与实际查询配置；对长 payload，优先先落本地文件再发请求。
4. 如果图表使用 ECharts 系列配置，且涉及 `y_axis_title`，则在 `params` 和 `query_context.form_data` 中同时写入 `y_axis_title_margin = 20` 与 `y_axis_title_position = Top`，避免一处生效、一处仍保留旧值。
5. 如果图表依赖固定 bucket / series 顺序，写入前先决定排序策略：优先 dataset 内排序字段，其次再考虑展示标签附带前缀；不要把“展示名隐藏”和“排序稳定”都押在 chart post-processing 上。
6. 写入后回查 chart 详情，并核对 datasource、关键字段与查询配置是否一致。如果这次只改了图表说明，按上面的图表说明验收，不要重跑查询验证。
7. 优先调用 `POST /api/v1/chart/data`，直接传 chart 当前 `query_context` 做真实查询验证；这个实例里 `GET /api/v1/chart/{pk}/data/` 可能在 chart 已可用时仍返回 dataframe / post-processing 相关错误，不能单独作为验收依据。
8. 如果是分桶图，除了看请求是否 `200`，还要检查返回数据里的 bucket / series 顺序是否符合预期，再结束。

### dashboard

1. 先确认需要挂载的 chart 已存在。
2. 如已有同名 dashboard，先读取其元信息、布局、图表引用关系。
3. 创建或修改时，明确是在变更标题、布局，还是在增删 chart。用户没有明确要求时，请求体不要包含 `roles`。
4. 写入后回查 dashboard 元信息。回查时确认 `roles` 没有被这次写入改掉。

### dashboard filter

1. 先确认目标 dashboard 已存在。
2. 先读取 dashboard 当前 filter 配置与作用范围。
3. 新建或修改 filter 时，只改目标 filter，不要误伤其他 filter 配置。
4. 写入后回查 dashboard 元信息，确认 filter 已绑定到正确的 chart 范围。

## 失败归因

遇到失败时，按下面的层次判断：

- `401`：优先看登录态、token、会话、CSRF。
- `403`：优先看角色权限、资源权限、动作是否被允许。
- `404`：区分是 endpoint 不存在，还是资源对象不存在。
- `400` / `422`：优先看 payload、字段名、必填项、类型或嵌套结构。
- `409`：优先看重复创建、对象冲突、状态冲突。
- `500`：优先视为服务端或依赖异常，但仍先排除明显错误请求体。

额外判断：

- 如果 chart 保存成功但查询失败，优先检查 chart 自定义 SQL 字段是否包含子查询，或是否把本该放在 dataset 的逻辑塞进了 chart。
- 如果写接口只在读接口正常的前提下失败，优先回查是否缺少 session cookie 或 CSRF，而不是先怀疑 endpoint 写错。

汇报阻塞时，至少明确：

- 对象是什么
- 动作是什么
- 调的是哪个 endpoint
- 返回了哪个 HTTP 状态码
- 当前判断属于认证、权限、路径、字段校验、依赖缺失，还是服务端错误

不要只说“Superset API 失败了”。

## 不要做的事

默认不要做下面这些事：

- 不先读取现状就直接写对象
- dataset 还没准备好就去创建 chart
- chart 还没准备好就去拼 dashboard
- 修改一个 filter 时把整份 dashboard 配置全量重写
- 用户没明确要求时，在创建或更新 dashboard 的请求里写 `roles`，或从已有看板抄访问角色
- 只更新 chart 的展示参数，却忽略实际查询配置是否也要同步
- 涉及 `y_axis_title` 时，只改 `params.y_axis_title_margin` / `params.y_axis_title_position`，却漏掉 `query_context.form_data` 里的同名字段
- 把需要子查询或 join 的逻辑硬塞进 chart 自定义 SQL 字段
- 在 filter 的 `scope.excluded` 里写 layout key 字符串，而不是数字 chart ID
- 写接口时只带 token，不带会话 cookie 或 CSRF
- 把口径说明写进 dataset 列的 `description`，指望 4.0.2 的表格列头显示出来
- 只改图表说明时重写整份 `params` / `query_context`
