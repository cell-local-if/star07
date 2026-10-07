# Quota Service — 公开契约（baseline）

多租户**限流/配额**服务：每个租户键（`key`）一个令牌桶，按时间线性补充；本次基线只实现最小可用子集，
后续任务在此契约之上继续建设（见项目 goal）。

## 运行

```bash
PYTHONPATH=src python3 -m quota.app --port 18893
PYTHONPATH=src python3 -m unittest discover -s tests -v
```

- Python 3.12，**仅标准库**。`127.0.0.1`，端口由 `--port` 指定。
- 时间源：单调时钟（测试注入假时钟 ⇒ 补充量的计算可复现）。状态在进程内存中。

## 时钟语义（确定性）

- 每个公开操作在进入额度临界区（同一把锁）后**只采样一次时钟**，本次操作内的补充、到期结算、
  预留创建与重配全部使用这同一个有效时刻。
- 有效时刻 = `max(本次读数, 历史最大读数)`（高水位线）：时钟前进时按真实经过秒数与当前
  `refill_per_second` 线性补充并封顶；时钟停住时补充量为 0；读数回拨时视为时间停留在水位线——
  不凭空补充额度、不提前结算或退款预留，时钟恢复后回拨区间也不会被重复计入经过时间。
- 水位线只影响时间推进，不改变 key/cost/ttl/容量/速率的校验结果与既有错误分类。

## 接口

### `GET /health`
`200 {"status":"ok"}`

### `PUT /v1/limits/{key}`
请求体：`{"capacity": <int 1..1000000>, "refill_per_second": <number > 0>}` → `200 {"key": ..., "limit": {...}}`
- 重新配置时**保留已用额度**（新桶的初始令牌 = `min(旧令牌, 新 capacity)`）。
- 未知字段/非法值 ⇒ `400 invalid_request`。

### `POST /v1/check`
请求体：`{"key": <string>, "cost": <int 1..1000000，缺省 1>}`
- 允许：`200 {"allowed": true, "remaining": <int 向下取整>, "capacity": <int>}`（并扣减令牌）。
- 超限：**`429`** `{"error":{"code":"over_quota",...}}`，并带 **`Retry-After`**（秒，浮点；按当前 `refill_per_second` 计算并向上取整到毫秒，至少足够补足本次 `cost`，与 `POST /v1/reservations` 同一口径）。
- 未配置的 key ⇒ `404 not_found`；`cost` 非法（含布尔值）⇒ `400 invalid_request`。

### `POST /v1/hierarchies/check`
面向**组织到租户**的层级即时扣减：复用 `PUT /v1/limits/{key}` 配置的既有令牌桶，不另建配置协议。
请求体：`{"keys": [<string>, ...], "cost": <int 1..1000000，缺省 1>}`。
- `keys` 为 **2..20 个互不重复**的非空字符串（每个均须通过既有 key 规则），按输入顺序表示上级到末级；
  `cost` 规则与 `POST /v1/check` 相同（非布尔整数，缺省 1）。
- 与既有额度操作**共用同一临界区、同一次时钟采样**：先按既有规则对每层结算到期预留并补充令牌，
  再同时判断每层是否足以承担本次 `cost`。
- 全部足够：`200 {"allowed": true, "cost": <int>, "layers": [{"key": ..., "remaining": <int 向下取整>, "capacity": <int>}, ...]}`，
  `layers` 按 `keys` 输入顺序，`remaining` 为扣减后的值；每层账本追加一条 `source` 为 `"hierarchy_check"`、
  `reservation_id` 为 `null` 的事件（`effective_at` 等字段同即时消耗口径），`used` 按 `cost` 增加。
- 任一层不足：**整体拒绝**，各层不扣减、不落账 ⇒ `429 over_quota`；`Retry-After` 取所有不足层
  各自按补充速率补齐 deficit 所需时间的**最大值**，沿用毫秒向上取整格式。
- 体含未知或缺失字段、`keys`/`cost` 非法、层重复 ⇒ `400 invalid_request`；结构合法但含未配置层
  ⇒ `404 not_found`，`message` 指明输入顺序中第一个未配置的 key（`invalid_request` 先于 `not_found`）。
- 并发下**只能全成或全败**：不出现部分扣减或部分落账；失败除既有惰性过期结算外不改变 `used`；
  时钟停住与回拨仍遵循高水位线口径。

### `POST /v1/reservations`
预留（占用但不记为已消耗）。请求体：`{"key": <string 非空 ≤200 字符>, "cost": <int 1..1000000，缺省 1>, "ttl_seconds": <int 1..86400，缺省 60>}`。
- 成功：`200 {"reservation_id": <进程内唯一的不透明字符串>, "key": ..., "cost": <int>, "remaining": <int>, "capacity": <int>, "ttl_seconds": <int>}`。
  预留立即扣减桶内令牌，`GET /v1/limits/{key}` 的 `remaining` 立即反映扣减，但 **`used` 不变**（`used` 只统计已接受的即时消耗）。
- **自动过期（惰性、确定）**：从预留创建时刻起经过单调时间 `ttl_seconds` 即失效（到期边界取大于等于）；重配同一 key 不延长有效期。
  同一 key 的下一次检查（`POST /v1/check`）、读取（`GET /v1/limits/{key}`）、预留、重配（`PUT`），或对该预留本身的回滚开始时，
  先把已到期预留的 `cost` 返还：返还前先按时间补充令牌，再按该 key 当前 `capacity` 封顶；每个预留只返还一次，
  时钟读数不前进时不额外补充。返还后 `remaining` 立即恢复，`used` 仍只统计即时消耗。
- 与 `check` 共用同一把锁、同一套原子额度判断：并发的检查、预留、到期释放与回滚不会超卖，也不会双重返还。
- 未配置的 key ⇒ `404 not_found`；key/cost/`ttl_seconds` 非法（含布尔值；`ttl_seconds` 须为 1..86400 的整数）⇒ `400 invalid_request`，
  且不创建预留、不扣减令牌。
- 令牌不足 ⇒ `429 over_quota` 并带 **`Retry-After`**（秒，浮点；按当前 `refill_per_second` 计算并向上取整到毫秒，至少足够补足本次 `cost`）。

### `DELETE /v1/reservations/{reservation_id}`
对**尚未过期且未回滚**的预留执行**一次**撤销：把预留扣掉的 `cost` 放回该 key 当前桶，并按当前 `capacity` 封顶（返还前也先按时间补充）。
- 成功：`200 {"reservation_id": ..., "rolled_back": true, "remaining": <int>, "capacity": <int>}`；
  返还后 `remaining` 立即反映且不超过 `capacity`。
- 重复撤销、未知预留、已自动过期的预留、路径不匹配（段数不对/别的资源）⇒ `404 not_found`；过期释放后再次回滚不会继续增加令牌。
- 预留期间允许继续检查与重新配置同一 key；重新配置仍保留已用额度（`min(旧令牌, 新 capacity)`）并采用新容量与补充速率，
  预留占用的额度在重配前后都被保留，回滚时返还到按新配置运行的桶中；重配不改变预留的到期时刻。

### `POST /v1/reservations/{reservation_id}/consume`
把**尚未过期且未撤销**的预留确认为实际消耗。请求体**必须是空 JSON 对象 `{}`**（无任何字段）。
- 命中时先按既有规则结算该 key 已到期的预留（到期返还），再确认目标预留：目标预留占用的 `cost`
  **不返还也不二次扣减**（令牌在预留时已扣），而是一次计入 `used`。
- 成功：`200 {"reservation_id": ..., "consumed": true, "remaining": <int>, "capacity": <int>, "used": <int>}`；
  `remaining` 与同一时刻 `GET /v1/limits/{key}` 的口径一致，`used` 含本笔消耗。
- **幂等**：`reservation_id` 即幂等键，重复 consume 仍返回 `200`，JSON 字段值与首次响应完全一致，`used` 不重复增加。
- 目标预留时刻等于或超过 `expires_at`：先按既有规则返还，consume 返回 `404 not_found`，之后任何时刻都不再落账。
- 未知、已过期、已撤销、已确认的预留对 DELETE 或 consume 的交叉访问均为 `404 not_found`；已确认预留不可撤销，
  后续时间推进也不会退款。
- 缺少或非法 `Content-Length`、非法 UTF-8 JSON、非对象 JSON（如 `[]`、`null`、`5`）、对象含未知字段
  ⇒ `400 invalid_request`，且不改变令牌、预留与 `used`；路径段数不对或末段不是 `consume`、方法不匹配 ⇒ 先返回 `404 not_found`。
- 重配同一 key 后确认：落账 `cost` 仍按预留创建时数值，`remaining`/`capacity` 采用新容量与补充速率的当前值。
- 与 check/reserve/expire/rollback 共用同一把锁、同一套原子额度判断：不会超卖、退款后又落账或重复累计 `used`。

### `POST /v1/hierarchies/reservations`
**跨层预留**：在层级即时扣减同一套键上做一次全有或全无的预留。请求体：
`{"keys": [<string>, ...], "cost": <int 1..1000000，缺省 1>, "ttl_seconds": <int 1..86400，缺省 60>}`，
`keys`/`cost`/`ttl_seconds` 规则分别与 `hierarchies/check` 和单键预留相同。
- 成功：`200 {"reservation_id": ..., "keys": [...], "cost": <int>, "ttl_seconds": <int>, "layers": [{"key": ..., "remaining": <int>, "capacity": <int>}, ...]}`，
  `layers` 按 `keys` 输入顺序；每层立即扣 `cost`（`remaining` 立即反映）但 **`used` 不变、账本不写**。
- 与单键预留、层级扣减**共用同一临界区与同一次时钟采样**：先按输入顺序确认每层均已配置
  （否则 `404 not_found`，`message` 指明第一个未配置的 key），再对每层结算到期的单层与跨层预留并补充令牌，
  最后同时判断。任一层不足 ⇒ 整体 `429 over_quota`，各层不扣减、不落账；`Retry-After` 取所有不足层
  各自 deficit 补齐时间的**最大值**，毫秒向上取整三位小数。
- 非法输入（缺 `keys`、未知字段、`keys`/`cost`/`ttl_seconds` 非法）⇒ `400 invalid_request`，不创建预留、不扣减。
- 跨层预留按创建时刻 `+ ttl_seconds` **惰性过期**（边界取大于等于）；任何额度入口开始时一并结算到期的
  单层与跨层预留，一个跨层预留过期时其**所有层在同一临界区内各返还一次**并按各自当前 `capacity` 封顶；
  重配任一层不延长 TTL。

### `DELETE /v1/hierarchies/reservations/{reservation_id}`
撤销一个未过期、未确认的跨层预留：把 `cost` 退回**全部层**（各层先按时间补充，再按各自当前 `capacity` 封顶）。
- 成功：`200 {"reservation_id": ..., "rolled_back": true, "layers": [{"key": ..., "remaining": <int>, "capacity": <int>}, ...]}`。
- 重复撤销、未知标识、已过期、已确认的预留，以及把单键预留标识用到本路由 ⇒ `404 not_found`；
  已确认预留不退款。全部层在同一临界区内返还，不存在部分退款。

### `POST /v1/hierarchies/reservations/{reservation_id}/consume`
确认一个跨层预留为实际消耗。请求体**必须是空 JSON 对象 `{}`**。
- 命中时先对每层按既有规则结算到期预留，再确认：预留的 `cost` 在创建时已扣，此处**不再扣令牌、不退款**，
  每层 `used` 各增加一次，并各落一条 `source` 为 `"hierarchy_reservation_consume"`、`reservation_id` 为该标识的账本事件。
- 成功：`200 {"reservation_id": ..., "consumed": true, "layers": [{"key": ..., "remaining": <int>, "capacity": <int>}, ...]}`；
  重配后确认的，`cost` 取创建时数值，`remaining`/`capacity` 取新配置当前值。
- **幂等**：重复 consume 原样重放首次响应，不重复落账、不重复增加 `used`。
- 未知、已过期、已撤销、已确认后交叉访问 DELETE，或把单键预留标识用到本路由 ⇒ `404 not_found`；
  路径段数不对、末段不是 `consume`、方法不匹配 ⇒ 先 `404` 且不读体；坏 JSON、未知字段、非空体 ⇒ `400 invalid_request`。

### `GET /v1/limits/{key}`
`200 {"limit": {...}, "remaining": <int>, "used": <int>}`（读取也会先按时间补充，体现当前余量；未确认的预留不计入 `used`）。

### `GET /v1/ledgers/{key}`
**只读配额账本**，供计费对账：给出形成了实际消耗的每一笔事件，以及与 `GET /v1/limits/{key}` 的 `used` 完全一致的合计。
- 查询参数（可选）：`events=<int 1..1000>`，按 `seq` 升序返回最近的若干条事件；缺省 `100`。
- 成功：`200 {"key": ..., "totals": {"accepted_count": <int>, "accepted_cost": <int>}, "events": [...]}`。
  `totals.accepted_cost` 恒等于同一状态下 `GET /v1/limits/{key}` 的 `used`；`accepted_count` 为事件总数（不受 `events` 窗口影响）。
- 每条事件固定为 `{"seq": <int 从 1 起每 key 连续递增>, "source": "check"|"hierarchy_check"|"reservation_consume"|"hierarchy_reservation_consume", "reservation_id": <string|null>, "cost": <int>, "remaining": <int>, "capacity": <int>, "effective_at": <float>}`：
  - 成功的 `check`、成功的层级 `hierarchies/check`（每层一条）、成功的（首次）consume 与成功的（首次）跨层 consume（每层一条）各生成且只生成一条；`check` 事件的 `reservation_id` 为 `null`，
    consume 事件使用原预留标识。重复 consume 幂等重放、不追加；令牌不足、回滚、过期结算与各类校验失败均不生成事件。
  - `cost` 为实际记账数；`remaining`/`capacity` 取记账完成临界区内的数值；`effective_at` 为产生该事件的公开操作
    在同一临界区内采样的有效时刻（高水位线口径）。
- 账本读取**只加锁拷贝，不采样时钟、不补充、不到期结算、不落账**：时钟停住或回拨时不会借读取提前退款、补充或落账；
  状态仍是纯进程内状态，无持久化或跨进程承诺。
- 未配置的 key ⇒ `404 not_found`；`events` 不是 1..1000 内的整数字面量、参数重复或出现任何未知查询参数
  ⇒ `400 invalid_request`（查询校验先于 key 的 404）；路径段数不对或方法不匹配 ⇒ `404 not_found`。

### `PUT /v1/windows/{key}`
与令牌桶**彼此独立**的精确滑动窗口限流：创建或更新窗口。请求体只接受
`{"window_seconds": <int 1..3600>, "max_events": <int 1..1000000>}`（两者均须为非布尔整数，不允许缺省、
浮点数或布尔值，不允许未知字段）。
- 成功：`200 {"key": ..., "window": {"window_seconds": <int>, "max_events": <int>}}`。
- **热更新保留已准入历史**：新配置在响应时即生效；缩短 `window_seconds` 立即淘汰窗外事件；
  降低 `max_events` **不追溯撤销**已准入请求，只拒绝后续请求。
- 无效配置、未知字段、非法 key（与令牌桶同一 key 规则：非空 ≤200 字符的字符串）⇒ `400 invalid_request`，
  且不改任何状态。

### `POST /v1/windows/{key}/check`
请求体**必须是空 JSON 对象 `{}`**（非空对象、非对象、畸形 JSON 或缺 `Content-Length` ⇒ `400 invalid_request`）。
- 只统计窗口内**已成功准入**的请求：未达到 `max_events` 时在同一临界区内原子计入本次，
  返回 `200 {"allowed": true, "used": <int>, "remaining": <int>, "limit": <int>, "window_seconds": <int>}`；
  `used` 包含本次，`remaining = max_events - used`，`limit = max_events`。
- 窗口满：**`429`** `{"error":{"code":"over_quota",...}}` 并带 **`Retry-After`**——最早有效事件离开窗口
  所需的精确秒数（`最早事件时刻 + window_seconds - 当前有效时刻`），与令牌桶同一口径：按毫秒**向上取整**、
  保留三位小数（亚毫秒也给 `0.001`，永不为 `0.000`）；被拒请求不计入。
- 未知窗口 ⇒ `404 not_found`；路径段数不对、末段不是 `check` 或方法不匹配 ⇒ **先**返回 `404 not_found`
  且不读请求体。

### `GET /v1/windows/{key}`
返回 check 同一有效时刻下的同一快照：`200 {"window": {...}, "used": <int>, "remaining": <int>}`
（先淘汰窗外事件再计数；未知窗口 ⇒ `404 not_found`）。

#### 滑动窗口的时间语义（确定性）
- 有效时刻沿用令牌桶的**单调时钟高水位线**口径，与令牌桶共用同一把锁、同一次 `_tick`：每个公开操作
  进入锁后只采样一次；时钟停住时淘汰截止线不变、不淘汰事件；读数回拨视为停留在水位线，恢复后
  回拨区间不会被重复计入（已准入事件不会因此提前或延后离开）。
- 事件在 `effective_at + window_seconds` 处到期，即 `effective_at <= now - window_seconds` 即为窗外：
  **边界取大于等于，恰在边界的旧事件先淘汰**，然后才判断本次准入。
- 并发 check 与 PUT 更新在同一把锁下串行：既不会超过 `max_events`，也不会重复计数或漏计。

#### 与令牌桶的隔离
- 窗口只存进程内存，与同名令牌桶 key **相互隔离**：`PUT /v1/windows/k` 不创建 `/v1/limits/k`，反之亦然。
- 窗口准入**不进入** `GET /v1/limits/{key}` 的 `used`，也不写入 `/v1/ledgers/{key}` 账本；令牌桶的
  check、配置、预留、consume、rollback、状态与账本的公开行为完全不变。

## 错误语义

```json
{"error": {"code": "invalid_request|not_found|over_quota|internal_error", "message": "<可读说明>"}}
```

优先级：`Content-Length` 校验先于读体；路由不匹配先于体校验；`invalid_request` 先于 `not_found`/`over_quota`。

## 未实现（后续任务候选，非固定题单）

漏桶、跨实例一致、热点键、降级与熔断、配置热更新的原子切换、
可观测性与压测基线。
