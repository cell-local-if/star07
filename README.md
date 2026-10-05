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
请求体：`{"key": <string>, "cost": <int ≥1，缺省 1>}`
- 允许：`200 {"allowed": true, "remaining": <int 向下取整>, "capacity": <int>}`（并扣减令牌）。
- 超限：**`429`** `{"error":{"code":"over_quota",...}}`，并带 **`Retry-After`**（秒，浮点，够补足 `cost` 的时间）。
- 未配置的 key ⇒ `404 not_found`；`cost` 非法 ⇒ `400 invalid_request`。

### `GET /v1/limits/{key}`
`200 {"limit": {...}, "remaining": <int>, "used": <int>}`（读取也会先按时间补充，体现当前余量）。
`used` 只累计 `check` 已接受的消耗，不受预留/回滚影响。

### `POST /v1/reservations`
请求体同 `check`：`{"key": <string>, "cost": <int ≥1，缺省 1>}`。
- 成功：`200 {"reservation_id": <进程内唯一不透明字符串>, "key", "cost", "remaining", "capacity}`，
  并立即原子扣减令牌（与 `check` 共用同一把锁，并发不超卖）；查询该 key 的 `remaining` 立即反映扣减。
- 错误语义与 `check` 一致：非法 key/cost ⇒ `400`，未配置 key ⇒ `404`，令牌不足 ⇒ `429` + `Retry-After`。

### `DELETE /v1/reservations/{reservation_id}`
- 成功：`200 {"reservation_id", "rolled_back": true, "remaining", "capacity}`；
  把预留的 `cost` 放回该 key 当前桶，按**当前** capacity 封顶（预留期间重新配置不影响回滚）。
- 同一预留只能回滚一次：重复撤销、未知 id、路径不匹配 ⇒ `404 not_found`。

## 错误语义

```json
{"error": {"code": "invalid_request|not_found|over_quota|internal_error", "message": "<可读说明>"}}
```

优先级：`Content-Length` 校验先于读体；路由不匹配先于体校验；`invalid_request` 先于 `not_found`/`over_quota`。

## 未实现（后续任务候选，非固定题单）

滑动窗口/漏桶、分层配额、跨实例一致、热点键、降级与熔断、配额账本与计费对账、配置热更新的原子切换、
时钟偏斜处理、可观测性与压测基线。
