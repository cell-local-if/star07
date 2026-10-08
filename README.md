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
- **配置版本 revision 与 ETag**：每个 key 第一次成功创建时 revision 为 **1**，此后每次成功 PUT 都 **+1**
  （即使新旧配置完全相同也递增）。成功 PUT 与 `GET /v1/limits/{key}` 的响应都带头
  **`ETag: "<revision>"`**（双引号包裹的当前 revision 十进制字符串，如 `ETag: "3"`）。响应 JSON 的字段与
  取值口径不变；ETag **只**反映配置 revision，不代表 token、`used`、账本或窗口版本。
- **乐观并发控制（可选 `If-Match`）**：请求可带头 `If-Match: "<revision>"`。该头**只能是一个**带双引号、
  不含符号和空白的十进制正整数；与该 key 当前 revision 相同才允许更新，不同则返回
  **`409`** `{"error":{"code":"revision_conflict","message":"If-Match revision does not match current configuration"}}`，
  且不修改限额、令牌、`used`、预留、账本、决策计数或时钟水位线（冲突在锁内判断、先于一切状态变更）。
  缺少 `If-Match` 时仍按基线**无条件更新**。
- key 尚未配置时，即使携带合法的 `If-Match` 也返回 `404 not_found`（不会被误创建）。请求体非法或
  `If-Match` 格式错误（如 `3`、`"0"`、`*`、`W/"3"`、含空白、多个标签/重复头）均返回 `400 invalid_request`；
  二者都在**进入锁之前**校验，失败时不产生部分配置或任何部分状态变化（也不推进时钟水位线）。
- 重新配置时**保留已用额度**：安装新配置前，先按**旧** `refill_per_second` 把上一有效时刻到本次重配时刻
  之间的等待补足（按旧 capacity 封顶），再按**新** capacity 封顶（即 `min(旧速率补足后的令牌, 新 capacity)`）；
  容量扩大不会把桶补满，新速率也不追溯作用于重配前的等待。重配开始时已到期的预留在同一临界区内按
  "先旧速率补充、再返还各自 cost、最后以新 capacity 封顶"的顺序结算；未到期预留继续占用。
- 未知字段/非法值 ⇒ `400 invalid_request`。

### `POST /v1/check`
请求体：`{"key": <string>, "cost": <int 1..1000000，缺省 1>}`
- 允许：`200 {"allowed": true, "remaining": <int 向下取整>, "capacity": <int>}`（并扣减令牌）。
- 超限：**`429`** `{"error":{"code":"over_quota",...}}`，并带 **`Retry-After`**（秒，浮点；按当前 `refill_per_second` 计算并向上取整到毫秒，至少足够补足本次 `cost`，与 `POST /v1/reservations` 同一口径）。
- 未配置的 key ⇒ `404 not_found`；`cost` 非法（含布尔值）⇒ `400 invalid_request`。
- 已配置 key 的合法 `cost` **大于当前 `capacity`** 时永远不可能入桶 ⇒ `400 invalid_request`
  （`message` 说明 cost 超过 capacity），不返回 200/429 也不带 `Retry-After`；该判断与并发 PUT
  在同一临界区内完成（每个请求只见更新前或更新后的单一配置），且在采样时钟之前返回——不推进
  水位线、不结算预留、不补令牌、不扣减、不落账、不计入 check 指标。`cost` 等于 `capacity` 仍按
  正常口径判断。

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
- 任一层（合法 `cost` 不超过该层 capacity 但）令牌不足：**整体拒绝**，各层不扣减、不落账 ⇒ `429 over_quota`；`Retry-After` 取所有不足层
  各自按补充速率补齐 deficit 所需时间的**最大值**，沿用毫秒向上取整格式。
- 体含未知或缺失字段、`keys`/`cost` 非法、层重复 ⇒ `400 invalid_request`；进入同一临界区后先按输入顺序确认**所有**层均已配置，
  结构合法但含未配置层 ⇒ `404 not_found`，`message` 指明输入顺序中第一个未配置的 key（`invalid_request` 先于 `not_found`，
  且配置确认先于容量边界判断）。
- 全部层确认已配置后、采样时钟之前：只要**任一层**的当前 `capacity` 小于合法 `cost`，该请求在该层永远不可能满足
  ⇒ `400 invalid_request`，`message` 指明输入顺序中**第一个**超过容量的 key 及其 capacity，不带 `Retry-After`；
  该判断读取与并发 PUT 同一临界区内的单一配置快照（每个请求只见整套配置更新前或更新后的版本，不混合），
  且不推进水位线、不结算到期预留、不补令牌、不扣减任一层、不写 used 或账本、不计入 hierarchy_check 指标、
  不改变 revision 或 ETag。`cost` 等于任一层 `capacity` 仍是合法请求，继续走上述普通可用性判断（可能 200 或普通 429），
  不因等于容量而提前拒绝。
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
- 已配置 key 的合法 `cost` **大于当前 `capacity`** 时该预留永远不可能成立 ⇒ `400 invalid_request`
  （`message` 说明 cost 超过 capacity），不返回 200/429 也不带 `Retry-After`，与 `POST /v1/check`
  同一口径；该判断与并发 PUT 在同一临界区内完成（每个请求只见更新前或更新后的单一配置），且在采样时钟
  之前返回——不推进水位线、不结算到期预留、不补令牌、不扣减、不创建预留、不落账、不计入 reservation 指标、
  不改变 revision 或 ETag。`cost` 等于 `capacity` 仍按正常可用性口径判断（桶空时仍可能得到普通 429）。
- 其余令牌不足（合法 `cost` 不超过 capacity 但桶内令牌不够）⇒ `429 over_quota` 并带 **`Retry-After`**（秒，浮点；按当前 `refill_per_second` 计算并向上取整到毫秒，至少足够补足本次 `cost`）。
- **创建幂等（可选 `Idempotency-Key` 头）**：请求可带头 `Idempotency-Key: <1..128 个非空白 ASCII 字符>`，该头只属于本路由。
  - 不带该头时行为与基线完全一致：key/cost/ttl 校验、令牌扣减、响应 JSON、`Retry-After`、metrics 与并发语义均不变。
  - 携带该头且**首次创建成功（200）**时，服务把头值与完整请求参数（`key`、`cost`、`ttl_seconds`）及首次 200 响应（含 `reservation_id`、`remaining` 等全部字段）绑定。
  - 之后**参数完全相同**的请求（头值相同）直接重放首次 200 响应：不再扣令牌、不再创建另一条预留、不增加 reservation 指标（既不计 `allowed` 也不计 `over_quota`）、不写账本、不采样时钟（不结算到期预留、不推进水位线）。
  - 头值相同但 `key`、`cost` 或 `ttl_seconds` **任一不同** ⇒ **`409`** `{"error":{"code":"idempotency_conflict",...}}`，不带 `Retry-After`，且不改变令牌、预留、`used`、账本、决策计数与时钟水位线（冲突判断在锁内最前面完成，先于时钟采样）。
  - **并发**到达的相同幂等请求收敛为一次创建：恰好一个请求实际占额并计一次 reservation `allowed`，其余请求得到同一 `reservation_id` 与逐字段相同的响应。
  - 头存在但**为空、重复（多行同名头）、超过 128 字符、含空白字符或含非 ASCII 字符** ⇒ `400 invalid_request`；该校验在**读取请求体之前**完成，故不会读取或采纳请求体而造成部分状态变化，也不发送 `Retry-After`、不推进水位线。
  - 普通 body 错误、未知 key、`cost` 超过 capacity、额度不足继续沿用既有 `400`/`404`/`429` 分类；**未创建成功的请求不绑定幂等键**，之后用同一幂等键的合法请求可正常首次创建。
  - 绑定只属于单键创建：不改变 `reservation_id`、rollback、consume 重放与到期结算。预留生命周期结束（consume、rollback、自动过期）后绑定仍保留：相同的创建重试仍重放第一次的 200 创建响应、不能重新占额；参数不同仍返回 `idempotency_conflict`。
  - 层级预留、check、ledger、ETag/If-Match、窗口、漏桶与既有错误优先级不受影响：幂等键不写账本、不新增 metrics 分类（metrics 形状保持五类不变），其他路由携带该头一律忽略。绑定只存在进程内存，重启清空，不承诺跨实例共享。

### `DELETE /v1/reservations/{reservation_id}`
对**尚未过期且未回滚**的预留执行**一次**撤销：把预留扣掉的 `cost` 放回该 key 当前桶，并按当前 `capacity` 封顶（返还前也先按时间补充）。
- 成功：`200 {"reservation_id": ..., "rolled_back": true, "remaining": <int>, "capacity": <int>}`；
  返还后 `remaining` 立即反映且不超过 `capacity`。
- 重复撤销、未知预留、已自动过期的预留、路径不匹配（段数不对/别的资源）⇒ `404 not_found`；过期释放后再次回滚不会继续增加令牌。
- 预留期间允许继续检查与重新配置同一 key；重新配置仍保留已用额度（先按旧速率补足等待时间，再按
  `min(补足后的令牌, 新 capacity)` 封顶）并采用新容量与补充速率，
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
- 与单键预留、层级扣减**共用同一临界区与同一次时钟采样**：进入临界区后先按输入顺序确认每层均已配置
  （否则 `404 not_found`，`message` 指明第一个未配置的 key，配置确认先于容量边界判断）；全部层确认后、
  采样时钟之前，只要**任一层**当前 `capacity` 小于合法 `cost`，该跨层预留永远不可能成立
  ⇒ `400 invalid_request`，`message` 指明输入顺序中**第一个**超过容量的 key 及其 capacity，不带
  `Retry-After`，也不推进水位线、不结算、不补令牌、不扣减任一层、不创建预留、不写 used 或账本、
  不计入 hierarchy_reservation 指标、不改变 revision 或 ETag（并发 PUT 下每个请求只见整套配置更新前
  或更新后的单一快照）；`cost` 等于任一层 `capacity` 仍继续走普通可用性判断。此后再对每层结算到期的
  单层与跨层预留并补充令牌，最后同时判断。其余情况下任一层令牌不足 ⇒ 整体 `429 over_quota`，各层不扣减、
  不落账；`Retry-After` 取所有不足层各自 deficit 补齐时间的**最大值**，毫秒向上取整三位小数。
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
`200 {"limit": {...}, "remaining": <int>, "used": <int>}` 并带 `ETag: "<revision>"` 头（revision 与本次
状态快照在同一把锁内取得，故 ETag 恒与该快照对应的配置一致；读取也会先按时间补充，体现当前余量；
未确认的预留不计入 `used`）。未配置的 key ⇒ `404 not_found`（无 ETag）。

### `GET /v1/ledgers/{key}`
**只读配额账本**，供计费对账：给出形成了实际消耗的每一笔事件，以及与 `GET /v1/limits/{key}` 的 `used` 完全一致的合计。
- 查询参数（可选）：`events=<int 1..1000>`，按 `seq` 升序返回最近的若干条事件；缺省 `100`。
- 事件明细为有界保存：每个 key 至多保留最近 1000 条。历史超过 1000 条时，返回的事件 `seq` 从大于 1 开始，
  但仍是连续递增、无跳号、无重复的最近尾部（首条 `seq` = `accepted_count` − 返回条数 + 1）。
- 成功：`200 {"key": ..., "totals": {"accepted_count": <int>, "accepted_cost": <int>}, "events": [...]}`。
  `totals.accepted_cost` 恒等于同一状态下 `GET /v1/limits/{key}` 的 `used`；`accepted_count` 为事件总数（不受 `events` 窗口影响）。
  两项合计均覆盖自 Limiter 创建以来的全部成功记账，不随明细裁剪而缩减。
- 每条事件固定为 `{"seq": <int 从 1 起每 key 连续递增>, "source": "check"|"hierarchy_check"|"reservation_consume"|"hierarchy_reservation_consume", "reservation_id": <string|null>, "cost": <int>, "remaining": <int>, "capacity": <int>, "effective_at": <float>}`：
  - 成功的 `check`、成功的层级 `hierarchies/check`（每层一条）、成功的（首次）consume 与成功的（首次）跨层 consume（每层一条）各生成且只生成一条；`check` 事件的 `reservation_id` 为 `null`，
    consume 事件使用原预留标识。重复 consume 幂等重放、不追加；令牌不足、回滚、过期结算与各类校验失败均不生成事件。
  - `cost` 为实际记账数；`remaining`/`capacity` 取记账完成临界区内的数值；`effective_at` 为产生该事件的公开操作
    在同一临界区内采样的有效时刻（高水位线口径）。
- 账本读取**只加锁拷贝，不采样时钟、不补充、不到期结算、不落账**：时钟停住或回拨时不会借读取提前退款、补充或落账；
  状态仍是纯进程内状态，无持久化或跨进程承诺。
- 未配置的 key ⇒ `404 not_found`；`events` 不是 1..1000 内的整数字面量、参数重复或出现任何未知查询参数
  ⇒ `400 invalid_request`（查询校验先于 key 的 404）；路径段数不对或方法不匹配 ⇒ `404 not_found`。

### `GET /v1/ledgers/{key}/reconciliation`
**只读对账报告**：核对该令牌桶 key 的已入账用量、账本合计、明细覆盖与未确认预留。**不接受任何查询参数，
也不读取请求体**；响应**不带 ETag**。
- 入口在锁内**只采样一次时钟**（高水位线口径），按**包含边界**（`created_at + ttl_seconds <= now`）结算
  触及该 key 的到期预留（单键与跨层）后，从同一已结算状态取整张快照；结算**只释放令牌**——不增加
  `used`、不落账、不计 metrics，与其他入口的到期结算语义完全一致。报告**只读不写**：发现不一致时
  如实报告差额，**不回补历史**。
- 成功：`200 {"key": ..., "reconciled": <bool>, "usage": {...}, "holds": {...}, "events": {...}}`，
  顶层只有这五个字段：
  - `usage`：`{"used": <int>, "ledger_accepted_count": <int>, "ledger_accepted_cost": <int>,
    "used_minus_ledger_cost": <int>}`——桶的累计已入账用量与账本全生命周期合计及其差值。
  - `holds`：`{"active_count": <int>, "active_cost": <int>, "single_key_count": <int>,
    "hierarchy_count": <int>}`——结算后仍存活、触及该 key 的预留；每个跨层预留的 `cost`
    对该 key **只计一次**（无论它横跨多少层）。
  - `events`：`{"retained_count": <int>, "retained_cost": <int>, "trimmed_count": <int>,
    "trimmed_cost": <int>, "first_seq": <int|null>, "last_seq": <int|null>}`——`retained` 是可读明细
    （有界尾部），`trimmed` 是累计裁剪明细（accepted 合计 − retained），两者合计覆盖 accepted 总量；
    明细为空时 `first_seq`/`last_seq` 为 `null`。
- `reconciled` 为 `true` 当且仅当：`used == ledger_accepted_cost`、retained 与 trimmed 的数量和 cost
  分别合计等于 accepted 总量、且保留明细的 `seq` 连续并满足 `last_seq == ledger_accepted_count`
  （从未记账的空账本视为一致）。否则仍返回 `200` 与 `reconciled: false` 及各项原始数值与差额。
- 非法 key 或任何查询参数 ⇒ `400 invalid_request`，且不改变任何状态（不采样时钟、不结算、不推进水位线）；
  未配置的 key ⇒ `404 not_found`，不创建任何对象（也不采样时钟）；路径段数不对、末段不是
  `reconciliation` 或方法不匹配 ⇒ `404 not_found`。窗口与漏桶的状态**不进入**核对。


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
请求体只接受**空对象或仅含可选 `cost` 的对象**：`{}`（`cost` 缺省为 **1**）或
`{"cost": <int 1..1000000>}`。`cost` 规则与 `POST /v1/check` 相同（非布尔整数，缺省 1；
布尔值、浮点数、0、超出范围均非法）。空对象仍等价于 `{"cost": 1}`，响应字段不增加。
- 每次成功检查登记一个**加权占用** `(effective_at, cost)`，窗口占用量 `used` 为所有**存活占用的 cost 总和**
  加上**有效窗口预留的 cost 总和**（预留见下文 `POST /v1/windows/{key}/reservations`）。成功检查在**一次原子判定**中先淘汰所有满足
  `effective_at + window_seconds <= 当前有效时刻` 的旧占用、并释放已到期的窗口预留——**边界取大于等于，恰在淘汰边界的历史占用先离开，
  同一时刻的占用作为一批一起释放**——然后才判断：淘汰后的 `used + cost <= max_events` 时把本次 cost
  作为当前有效时刻的占用计入，返回
  `200 {"allowed": true, "used": <int>, "remaining": <int>, "limit": <int>, "window_seconds": <int>}`；
  `used` 含本次，`remaining = max_events - used`，`limit = max_events`。
- 放不下（`used + cost > max_events`）：本次 **cost 不进入历史**，返回 **`429`**
  `{"error":{"code":"over_quota",...}}` 并带 **`Retry-After`**——等待**最早一批**旧占用过期或有效预留释放、
  且该批离开后**累计释放的 cost 首次足以容纳本次请求**的边界时刻（事件与预留在同一条释放时间线上，
  从最旧的存活占用/预留起按同一时刻分批累计，取第一个使 `used - 累计释放 + cost <= max_events` 的批次，
  等待秒数为 `该批释放时刻 - 当前有效时刻`）；同一时刻的占用一起释放。格式与令牌桶 429 同一口径：
  按毫秒**向上取整**、保留三位小数（亚毫秒也给 `0.001`，永不为 `0.000`）；被拒请求不计入占用。
- 已配置窗口的**合法 `cost` 大于当前 `max_events`** 时该请求永远不可能放入 ⇒ **`400 invalid_request`**
  （`message` 说明 cost 超过 max_events），不返回 200/429、不带 `Retry-After`；该判断与并发 PUT 在同一临界区内
  读取单一配置快照（每个请求只见更新前或更新后的一个 `max_events`），且在**采样时钟之前**返回——不推进水位线、
  **不淘汰占用、不改变 used、不计 window_check 指标**。`cost` 等于 `max_events` 仍是合法请求（空窗时可直接放入）。
- 非对象、含未知字段、`cost` 非法（含布尔/浮点/缺省以外形状）、畸形 JSON、缺 `Content-Length`
  ⇒ `400 invalid_request`（均在**进入锁之前**拒绝）；**格式合法**但窗口未知 ⇒ **`404 not_found`**，
  并且**不创建窗口**；非法 key 仍为 `400 invalid_request`（格式校验先于窗口查找）；路径段数不对、末段不是
  `check` 或方法不匹配 ⇒ **先**返回 `404 not_found` 且不读请求体。
- **指标口径**：每次**格式合法**的 POST 检查——无论允许（200）还是 over_quota（429）——都只让
  `window_check` 对应计数加一；`400`（含 cost 超过 max_events 与体格式错误）和 `404` **不计数**。

### `GET /v1/windows/{key}`
返回 check 同一有效时刻下的同一快照：`200 {"window": {...}, "used": <int>, "remaining": <int>}`
（先淘汰窗外占用再计数；`used` 为存活占用的 **cost 总和**加上**有效窗口预留**的 cost 总和，
`remaining = max_events - used`；未知窗口 ⇒ `404 not_found`）。

### `POST /v1/windows/{key}/reservations`
**窗口容量预留**：在滑动窗口内占用 `cost` 额度但不形成事件。请求体只接受
`{}`、`{"cost": <int 1..1000000，缺省 1>}`、`{"ttl_seconds": <int 1..86400，缺省 60>}` 或二者
（非对象、未知字段、非法 cost/ttl、畸形 JSON、缺 `Content-Length` ⇒ `400 invalid_request`，均在进入锁之前拒绝）。
- 成功：`200 {"reservation_id": <进程内唯一的不透明字符串>, "key": ..., "cost": <int>, "ttl_seconds": <int>,
  "used": <int>, "remaining": <int>, "limit": <int>, "window_seconds": <int>}`；
  预留**立即计入** `used` 与 `remaining`，后续窗口检查与状态读取都把有效预留视为容量占用。
- 与窗口检查**共用同一临界区与同一次时钟采样**：先淘汰窗外占用并释放到期预留，再判断
  `used + cost <= max_events`；判断口径、边界（大于等于）与同刻批量语义与窗口检查完全一致。
- **自动过期（惰性、确定）**：预留在 `created_at + ttl_seconds` 释放（边界取大于等于），释放**不形成事件**；
  重配窗口（含缩短 `window_seconds`、降低 `max_events`）**不延长** TTL，也不撤销已有占用。
- 容量不足 ⇒ **`429`** `{"error":{"code":"over_quota",...}}` 并带 **`Retry-After`**：等待**最早一批**
  过期事件**或有效预留**释放后刚好容纳本次 `cost` 的时刻（事件与预留在同一条释放时间线上按同一时刻分批累计，
  取第一个使 `used - 累计释放 + cost <= max_events` 的批次，等待秒数为 `该批释放时刻 - 当前有效时刻`），
  按现有毫秒向上取整规则、保留三位小数；被拒请求不占用容量。
- 已配置窗口的合法 `cost` **大于当前 `max_events`** ⇒ **`400 invalid_request`**（`message` 说明 cost 超过
  max_events），不返回 200/429、不带 `Retry-After`；该判断与并发 PUT 在同一临界区内读取单一配置快照，
  且在**采样时钟之前**返回——不推进水位线、不淘汰占用、不释放预留、不改变 used。`cost` 等于 `max_events`
  仍是合法请求。未知窗口 ⇒ `404 not_found`（不创建窗口）；非法 key ⇒ `400 invalid_request`（格式校验先于查找）。
- 窗口预留**不写账本、不改变 revision 或 ETag、不计入 `GET /v1/metrics` 的任何计数**（metrics 形状保持五类不变）。

### `DELETE /v1/windows/{key}/reservations/{reservation_id}`
撤销一个**未过期**的窗口预留：其 `cost` 立即不再计入占用（释放不形成事件）。
- 成功：`200 {"key": ..., "cost": <int>, "rolled_back": true, "used": <int>, "remaining": <int>,
  "limit": <int>, "window_seconds": <int>}`，`used`/`remaining` 为释放后的当前值。
- 重复撤销、未知标识、**跨 key** 的预留、已过期、已确认的预留，以及把令牌桶/层级预留标识用到本路由
  ⇒ `404 not_found`；未知窗口 ⇒ `404 not_found`；路径段数不对或方法不匹配 ⇒ 先 `404` 且不读体。

### `POST /v1/windows/{key}/reservations/{reservation_id}/consume`
把一个**未过期**的窗口预留确认为普通占用。请求体**必须是空 JSON 对象 `{}`**。
- 命中时先按既有规则淘汰窗外占用并释放到期预留，再确认：预留在**同一有效时刻**转成一条普通占用
  （`effective_at` 为本次操作的有效时刻），此后按 `window_seconds` 滑出窗口；转换**不重新判断容量**
  （该 cost 本就在占用窗口），`used` 与 `remaining` 不因转换本身改变。
- 成功：`200 {"key": ..., "cost": <int>, "consumed": true, "used": <int>, "remaining": <int>,
  "limit": <int>, "window_seconds": <int>}`。
- **幂等**：重复 consume 仍返回 `200`，JSON 字段值与首次响应完全一致，不再产生占用。
- 未知标识、**跨 key** 的预留、已过期、已撤销的预留，以及把令牌桶/层级预留标识用到本路由 ⇒ `404 not_found`；
  未知窗口 ⇒ `404 not_found`；过期后的 consume 不再产生任何占用。
- 窗口预留的创建、回滚、consume 与过期释放均**不写账本、不计 metrics、不改变 revision/ETag**。

### `GET /v1/metrics`
**只读累计决策观测**：返回本进程内 Limiter 创建以来五类额度决策的累计次数，形状固定为
`200 {"metrics": {"decisions": {"check": {"allowed": <int>, "over_quota": <int>}, "hierarchy_check": {...}, "reservation": {...}, "hierarchy_reservation": {...}, "window_check": {...}}}}`。
- 五个名称依次对应：单键即时检查（`POST /v1/check`）、层级即时检查（`POST /v1/hierarchies/check`）、
  单键预留创建（`POST /v1/reservations`）、跨层预留创建（`POST /v1/hierarchies/reservations`）、
  滑动窗口检查（`POST /v1/windows/{key}/check`）。
- 一次成功决策计一次 `allowed`；一次因额度或窗口不足返回 `429` 的决策计一次 `over_quota`。
  层级请求**按整个请求计一次**，不按层数累计。
- 预留的 consume、rollback、过期结算与所有 GET 读取**不计数**；窗口预留的创建、回滚、consume 与过期释放同样**不计数**；参数校验失败（`400`）、
  未配置 key 的 `404`、未知路由与错误方法也**不计数**。
- 计数在同一把锁内随决策精确加一：并发下不丢失、不重复。
- 读取**只加锁拷贝计数**：不采样时钟、不推进高水位线、不结算预留、不补充令牌、不改账本或窗口历史，
  时钟停住或回拨不会改变读取结果。
- 计数只存进程内存并随 Limiter 累计，重启归零，无持久化承诺。
- 本路由**不带查询参数**：带任意查询参数 ⇒ `400 invalid_request`；路径多段、少段或方法不是 GET
  ⇒ `404 not_found`，且错误路径与错误方法不读取请求体。

#### 滑动窗口的时间语义（确定性）
- 有效时刻沿用令牌桶的**单调时钟高水位线**口径，与令牌桶共用同一把锁、同一次 `_tick`：每个公开操作
  进入锁后只采样一次；时钟停住时淘汰截止线不变、不淘汰占用；读数回拨视为停留在水位线，恢复后
  回拨区间不会被重复计入（已准入占用不会因此提前或延后离开）。
- 占用在 `effective_at + window_seconds` 处到期，即 `effective_at <= now - window_seconds` 即为窗外：
  **边界取大于等于，恰在边界的历史占用先淘汰**（同一时刻的占用作为一批一起离开），然后才判断本次准入。
- 并发 check 与 PUT 更新在同一把锁下串行：存活占用的 cost 总和既不会超过 `max_events`，也不会重复计数或漏计。

#### 与令牌桶的隔离
- 窗口只存进程内存，与同名令牌桶 key **相互隔离**：`PUT /v1/windows/k` 不创建 `/v1/limits/k`，反之亦然。
- 窗口准入**不进入** `GET /v1/limits/{key}` 的 `used`，也不写入 `/v1/ledgers/{key}` 账本；令牌桶的
  check、配置、预留、consume、rollback、状态与账本的公开行为完全不变。
- 窗口预留在**独立命名空间**：其 `reservation_id` 不能用于令牌桶/层级预留的 rollback 与 consume（反之亦然，
  交叉访问一律 `404 not_found`），其创建、回滚、consume 与过期释放不写账本、不计 metrics、不改变 revision/ETag。

### `PUT /v1/leaky-buckets/{key}`
与令牌桶、窗口**彼此独立**的漏桶：水随时间以固定速率漏出，注入的请求抬高水位，水位加本次 cost
不超过 `capacity` 才允许。请求体只接受
`{"capacity": <int 1..1000000>, "leak_per_second": <number > 0 且 ≤ 1000000>}`
（`capacity` 为非布尔整数、不允许缺省/浮点/布尔；`leak_per_second` 为非布尔数字、不允许缺省/布尔/
非正数/超过 1000000；不允许未知字段）。
- 成功：`200 {"key": ..., "level": <number 三位小数>, "capacity": <int>, "leak_per_second": <number>}`；
  **无 ETag/If-Match/revision**。
- **首次创建**：`level` 为 **0**。
- **再次 PUT（热更新）**：在同一临界区内先按**旧** `leak_per_second` 漏出上一有效时刻到本次时刻之间的
  水量并以 **0 为下限**，再装入新配置，并以**新 `capacity`** 对存活水量封顶（`min(漏出后水位, 新 capacity)`）；
  新速率**不追溯**重配前的等待，容量缩小把超出部分立即截掉。
- 非法 JSON、字段、key（同一 key 规则）或携带任何查询参数 ⇒ `400 invalid_request`，且**不创建桶、
  不推进时钟水位线**（校验在锁前完成）。

### `POST /v1/leaky-buckets/{key}/check`
请求体必须是只含可选 `cost` 的 JSON 对象：`{}`（`cost` 缺省为 1）或 `{"cost": <int 1..1000000>}`。
`cost` 规则与 `POST /v1/check` 相同（非布尔整数，缺省 1）。
- 进入锁后**只采样一次时钟**，先按**当前** `leak_per_second` 漏出（以 0 为下限），再判断。
- 允许（`level + cost <= capacity`，边界取等号）：计入并 `200 {"allowed": true, "cost": <int>,
  "level": <number>, "capacity": <int>}`；`level` 为**计入后**占用量，保留**三位小数**。
- 放不下：**不计** `cost`（水位不变），返回 **`429`** `{"error":{"code":"over_quota",...}}` 并带
  **`Retry-After`**——补足 `level + cost - capacity` 缺口所需秒数（缺口除以当前 `leak_per_second`），
  与其他 429 同一口径：按毫秒**向上取整**、保留三位小数，亚毫秒至少 `0.001`，永不为 `0.000`。
- 非对象、含未知字段、`cost` 非法（含布尔）、畸形 JSON、缺 `Content-Length`，或携带任何查询参数
  ⇒ `400 invalid_request`，且不抬高水位、不推进时钟水位线；未知桶 ⇒ `404 not_found`；路径段数不对、
  末段不是 `check` 或方法不匹配 ⇒ **先**返回 `404 not_found` 且不读请求体。

### `GET /v1/leaky-buckets/{key}`
返回 check 同一有效时刻下的同一口径：先按当前速率漏出，再返回
`200 {"key": ..., "level": <number 三位小数>, "capacity": <int>, "leak_per_second": <number>}`，
恰好这四个字段（无 ETag）。未知桶 ⇒ `404 not_found`；携带任何查询参数 ⇒ `400 invalid_request`。

#### 漏桶的时间语义（确定性）
- 有效时刻沿用同一把锁、同一单调时钟**高水位线**与同一次 `_tick`：时钟停住时漏出量为 0；读数回拨视为
  停留在水位线、不多漏；时钟恢复后回拨区间不被重复计入（`updated_at` 在每次有效时刻落戳）。
- 并发 check 与热更新在同一把锁下串行：水位恒不超过当前 `capacity`，被拒请求不增加水位。

#### 漏桶与其他子系统的隔离
- 漏桶只存进程内存并在**独立命名空间**：同名的令牌桶、窗口与漏桶互不创建、互不计账。
- 漏桶**不写** `/v1/ledgers/{key}` 账本、**没有** revision/ETag，也**不加入** `GET /v1/metrics` 的五类
  决策计数（metrics 形状保持五类不变）；令牌桶、窗口、预留、层级、账本、revision 与既有接口的 JSON、
  Retry-After、计数和时钟行为完全不变。

## 错误语义

```json
{"error": {"code": "invalid_request|not_found|over_quota|revision_conflict|idempotency_conflict|internal_error", "message": "<可读说明>"}}
```

优先级：`Content-Length` 校验先于读体；路由不匹配先于体校验；`invalid_request` 先于 `not_found`/`over_quota`；
`If-Match`/请求体/`Idempotency-Key` 头的 `invalid_request` 在锁前（`Idempotency-Key` 还在先于读体处）返回，先于 key 的 `not_found`；合法 `If-Match` 对未配置 key 为
`404 not_found`；已配置 key 上版本不符为 `409 revision_conflict`（二者均在锁内、不改变任何状态）。
`POST /v1/reservations` 携带已绑定的 `Idempotency-Key` 时，参数不一致为 `409 idempotency_conflict`，该判断在锁内、采样时钟之前，不改变任何状态。

## 乐观并发控制与兼容性

- revision 计数的是**成功配置写入**：首次创建为 1，之后每次成功 PUT（含配置完全相同的 PUT）+1；
  `400`/`404`/`429`/`409` 都不推进 revision，也不改变 ETag。
- ETag 仅与单键限额配置关联：ledgers、windows、leaky-buckets、metrics、check、reservations、hierarchies 响应均不带 ETag；
  同名窗口、漏桶与令牌桶继续隔离，窗口/漏桶 PUT/GET 不影响同名限额的 revision。
- revision 判断与更新在**同一把锁**内完成；PUT 的 ETag 命名本次安装的 revision，GET 的 ETag 与同一快照的
  状态在同一临界区取出。并发的两个同版本条件 PUT 中恰好一个成功（200，revision+1），另一个 409；后续若用
  新 ETag 重试即可成功。
- 除新增 ETag 响应头与可选 `If-Match` 及其 `409`、`POST /v1/reservations` 可选 `Idempotency-Key` 及其
  `409 idempotency_conflict` 外，不带这些头的 PUT、GET、check、hierarchies、
  reservations、ledgers、windows、leaky-buckets、metrics 的 JSON 形状、状态推进、错误分类与并发保证均不变。
  `Idempotency-Key` 不写账本、不新增 metrics 分类，绑定只存进程内存（重启清空、不跨实例），且不作用于层级预留等其他路由。

## 未实现（后续任务候选，非固定题单）

跨实例一致、热点键、降级与熔断、
可观测性与压测基线。
