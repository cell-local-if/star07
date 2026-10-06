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
- 超限：**`429`** `{"error":{"code":"over_quota",...}}`，并带 **`Retry-After`**（秒，浮点，够补足 `cost` 的时间）。
- 未配置的 key ⇒ `404 not_found`；`cost` 非法（含布尔值）⇒ `400 invalid_request`。

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

### `GET /v1/limits/{key}`
`200 {"limit": {...}, "remaining": <int>, "used": <int>}`（读取也会先按时间补充，体现当前余量；未确认的预留不计入 `used`）。

### `GET /v1/limits/{key}/ledger`
只读计费对账账本：按发生顺序列出该 key 已接受的使用，调用方无需从当前令牌状态反推历史。
- 成功的 `POST /v1/check` 落一条 `source: "check"` 事件；预留**首次**通过 consume 确认时落一条
  `source: "reservation_consume"` 事件。被拒绝的检查、未确认的预留、到期返还、撤销与重复 consume 均不落账。
- 每个事件：`{"sequence": <int 从 1 连续递增>, "source": ..., "cost": <本次落账值>,
  "used_after": <落账后该 key 的 used>, "event_id": <永久唯一的不透明字符串>, "occurred_at": <单调时间源浮点秒>}`。
  事件一旦落账即冻结，不随新事件或时间推进变化。
- 查询参数仅接受可选 `after`：只返回 `sequence` 严格大于它的事件（缺省等同 `after=0`）。
  响应 `200 {"key": ..., "events": [...], "next_after": <int>}`：`events` 按 `sequence` 升序、最多 100 条；
  有结果时 `next_after` 为最后一条的 `sequence`（即下一页的游标），无结果时等于传入的 `after`。
- 读取同样先按既有规则结算该 key 已到期的预留（返还不落账）；重配 key 后账本与序号连续保留；
  运行期内分页结果稳定，进程重启允许从空账本开始。
- key 未配置 ⇒ `404 not_found`；路径或方法不匹配 ⇒ 先 `404 not_found`；未知查询参数，或 `after`
  不是非负 ASCII 十进制整数（含正负号、小数点、指数、空白或非 ASCII 数字）或超过 `2147483647`
  ⇒ `400 invalid_request`，且不改变任何状态。

## 错误语义

```json
{"error": {"code": "invalid_request|not_found|over_quota|internal_error", "message": "<可读说明>"}}
```

优先级：`Content-Length` 校验先于读体；路由不匹配先于体校验；`invalid_request` 先于 `not_found`/`over_quota`。

## 未实现（后续任务候选，非固定题单）

滑动窗口/漏桶、分层配额、跨实例一致、热点键、降级与熔断、配置热更新的原子切换、
时钟偏斜处理、可观测性与压测基线。
