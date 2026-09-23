# Differential-Privacy Budget Ledger

基于 **Python · FastAPI · SQLite · asyncio** 的差分隐私预算账本服务，支持
Docker Compose 一键启动。

客户端为数据集登记 epsilon / delta 总预算（cap），然后提交带
`request_id`、`query_type`、`epsilon`、`delta` 的分析申请；服务在一组
Rényi DP（RDP）orders 上累加各次申请的组合消耗，在并发申请下**原子地**
决定预留成功或拒绝超预算，并对重复 `request_id` 做**幂等**处理。提供
预算明细与剩余额度查询。

此外支持两阶段的**预算预留（reservation）**：客户端以
`reservation_id`、`epsilon`、`delta`、`ttl_seconds` 原子地锁定一段预算；
同一 `reservation_id` + 同负载重放幂等、负载冲突返回 `409`。预留可在 TTL
内**确认（confirm）**为一次正式分析申请（沿用同一套 RDP 核算，预算不重复
扣减），也可随时幂等**释放（release）**；超过 TTL 未确认的预留自动过期、
在查询与新建预留/申请时不再占用额度。还提供**只读**的预算释放预测接口：
给定一个未来 UTC 截止时间，按 `expires_at` 分组模拟预留到期并逐刻重算
剩余 epsilon 与活动预留数，全程不改写任何预留状态或数据库。

此外提供数据集之间的 **epsilon 上限转拨（budget transfer）**：
`POST /budget-transfers` 以全局幂等键 `transfer_id` 指定来源 / 目标数据集与
正数 `epsilon_amount`，在同一个 SQLite 写事务内按既有 RDP 口径核算两端的
已授权申请与未过期预留占用，原子地把 epsilon_cap 从来源划到目标；
delta_cap 与已入账申请、预留均不变。来源转出后上限必须仍为**正数**且足以
覆盖现有占用、目标转入后上限不得超过登记上限 **1000**，否则整笔拒绝、两个
上限均不变（拒绝也留痕）。相同 `transfer_id` + 相同负载重试返回原记录而不
再次转拨，异负载返回 `409`。

---

## 1. 快速启动（Docker Compose）

```bash
# 构建并后台启动
docker compose up -d --build

# 查看日志
docker compose logs -f

# 停止
docker compose down

# 停止并删除数据库卷（清空所有账本数据）
docker compose down -v
```

启动后：

| 项目 | 地址 |
| --- | --- |
| API Base URL | `http://localhost:8000` |
| 健康检查 | `GET http://localhost:8000/health` |
| Swagger 交互文档 | `http://localhost:8000/docs` |
| ReDoc 文档 | `http://localhost:8000/redoc` |
| OpenAPI Schema | `http://localhost:8000/openapi.json` |

宿主机端口可用环境变量覆盖：

```bash
LEDGER_PORT=18000 docker compose up -d --build
# 然后访问 http://localhost:18000
```

数据库文件持久化在命名卷 `dp-ledger-data` 内（容器内 `/data/ledger.db`，
SQLite WAL 模式）。

---

## 2. 本地（无 Docker）运行

需要 Python 3.11+：

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# 数据库文件路径（默认 /data/ledger.db，本地运行建议改成当前目录）
export DATABASE_PATH=./ledger.db
uvicorn app.main:app --host 0.0.0.0 --port 8000
```

---

## 3. 配置

所有配置通过环境变量注入（见 `docker-compose.yml`）：

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `DATABASE_PATH` | `/data/ledger.db` | SQLite 数据库文件路径，所在目录需可写 |
| `HOST` | `0.0.0.0` | uvicorn 监听地址 |
| `PORT` | `8000` | 容器内监听端口 |
| `LEDGER_PORT` | `8000` | Compose 映射到宿主机的端口 |
| `SQLITE_BUSY_TIMEOUT_MS` | `10000` | SQLite 写锁等待超时（毫秒） |
| `EPSILON_TOLERANCE` | `1e-12` | 预算比较的浮点容差，避免累计舍入误拒 |

---

## 4. API 一览

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| `POST` | `/datasets/{dataset_id}` | 登记数据集及 epsilon/delta 上限 |
| `GET` | `/datasets/{dataset_id}` | 数据集信息 + 预算明细 |
| `GET` | `/datasets/{dataset_id}/budget` | 仅预算明细（消耗 / 预留 / 剩余 / RDP 曲线） |
| `POST` | `/datasets/{dataset_id}/requests` | 提交分析申请（幂等） |
| `GET` | `/datasets/{dataset_id}/requests` | 全部申请记录（含 granted / rejected） |
| `POST` | `/datasets/{dataset_id}/reservations` | 创建预算预留（幂等，带 TTL） |
| `GET` | `/datasets/{dataset_id}/reservations` | 全部预留记录（含各生命周期状态与时间） |
| `GET` | `/datasets/{dataset_id}/reservations/{reservation_id}` | 查询单个预留 |
| `POST` | `/datasets/{dataset_id}/reservations/{reservation_id}/confirm` | 确认预留 → 分析申请（幂等） |
| `POST` | `/datasets/{dataset_id}/reservations/{reservation_id}/release` | 释放预留（幂等） |
| `GET` | `/datasets/{dataset_id}/budget/release-forecast?deadline=...` | **只读**模拟截止时间前的预留到期释放 |
| `POST` | `/budget-transfers` | 在两个数据集间原子转拨 epsilon 上限（幂等） |
| `GET` | `/health` | 健康检查 |

`dataset_id` / `reservation_id` 允许字母、数字、`.`、`_`、`-`，长度 ≤ 200。

### 4.1 登记数据集

```bash
curl -X POST http://localhost:8000/datasets/sales-2026 \
  -H 'Content-Type: application/json' \
  -d '{
    "epsilon_cap": 1.0,
    "delta_cap": 1e-6,
    "description": "2026 销售数据集"
  }'
```

请求体：

| 字段 | 类型 | 约束 |
| --- | --- | --- |
| `epsilon_cap` | float | `> 0`，总 epsilon 预算 |
| `delta_cap` | float | `>= 1e-20, < 1`，总 delta；同时作为 RDP 转回 `(ε,δ)` 时的目标 δ |
| `description` | string | 可选，≤ 2000 字符 |

重复登记同一 `dataset_id` 返回 `409 dataset_already_exists`。

### 4.2 提交分析申请

```bash
curl -X POST http://localhost:8000/datasets/sales-2026/requests \
  -H 'Content-Type: application/json' \
  -d '{
    "request_id": "req-20260923-0001",
    "query_type": "gaussian_mean",
    "epsilon": 0.2,
    "delta": 1e-8
  }'
```

请求体：

| 字段 | 类型 | 约束 |
| --- | --- | --- |
| `request_id` | string | 必填，客户端生成的幂等键，**同一数据集内唯一** |
| `query_type` | string | 必填，查询类型标签（如 `gaussian_mean`、`histogram`） |
| `epsilon` | float | `> 0`，本次申请的 epsilon |
| `delta` | float | `>= 0, < 1`；`0` 表示纯 ε-DP |

返回 `201` 与裁定结果：

```json
{
  "dataset_id": "sales-2026",
  "request_id": "req-20260923-0001",
  "query_type": "gaussian_mean",
  "epsilon": 0.2,
  "delta": 1e-8,
  "allowed": true,
  "status": "granted",
  "reason": null,
  "epsilon_before": 0.0,
  "epsilon_after": 0.203936,
  "epsilon_remaining": 0.796064,
  "rdp_best_order": 8192.0,
  "idempotent_replay": false,
  "created_at": "2026-09-23T01:10:16.563790+00:00"
}
```

* 预算内 → `allowed=true, status="granted"`，预算被原子预留；
* 超预算 → 仍返回 `201`（请求被正常受理并留痕），但
  `allowed=false, status="rejected"`，`reason` 说明投影消耗，
  **不占用任何预算**；
* 数据集不存在 → `404`。

### 4.3 幂等语义

* 相同 `(dataset_id, request_id)` + **相同负载**重复提交：返回 `200`、
  与第一次完全一致的裁定，`idempotent_replay=true`，预算**只扣一次**；
* 相同 `request_id` 但 `query_type` / `epsilon` / `delta` 不同：
  返回 `409 request_id_conflict`；
* 被拒绝的申请同样幂等：重复提交返回最初的拒绝结果。

### 4.4 查询预算明细与剩余额度

```bash
curl http://localhost:8000/datasets/sales-2026/budget
```

```json
{
  "epsilon_cap": 1.0,
  "delta_cap": 1e-6,
  "epsilon_consumed": 0.203936,
  "epsilon_reserved": 0.0,
  "epsilon_remaining": 0.796064,
  "rdp_best_order": 8192.0,
  "granted_requests": 1,
  "active_reservations": 0,
  "rdp_curve": {
    "2.0": 18.62, "4.0": 6.34, "8.0": 2.83, "16.0": 1.43,
    "32.0": 0.79, "64.0": 0.50, "128.0": 0.35, "512.0": 0.21,
    "4096.0": 0.204
  }
}
```

字段口径：

* `epsilon_consumed`：已授权（直接 granted 或由预留 confirmed）分析申请的
  RDP 投影 epsilon；
* `epsilon_reserved`：**未过期有效预留**在同一组合曲线上额外锁定的投影
  epsilon（相对 `epsilon_consumed` 的增量）；
* `epsilon_remaining`：`epsilon_cap −`（已授权申请与有效预留**一起组合**后
  的投影 epsilon），即此刻还能再授权的额度；
* `active_reservations`：处于 `granted`（已创建未确认/释放/过期）状态的
  预留数量；`rdp_curve` 同时包含已授权申请与有效预留。

### 4.5 查询申请明细

```bash
curl http://localhost:8000/datasets/sales-2026/requests
```

按受理顺序返回所有申请（granted 与 rejected），每条含申请参数、裁定、
理由、申请前后累计 epsilon 与时间戳。

### 4.6 创建预算预留

```bash
curl -X POST http://localhost:8000/datasets/sales-2026/reservations \
  -H 'Content-Type: application/json' \
  -d '{
    "reservation_id": "res-20260923-0001",
    "epsilon": 0.2,
    "delta": 1e-8,
    "ttl_seconds": 600
  }'
```

请求体：

| 字段 | 类型 | 约束 |
| --- | --- | --- |
| `reservation_id` | string | 必填，客户端生成的幂等键，**同一数据集内唯一** |
| `epsilon` | float | `> 0`，预留的 epsilon |
| `delta` | float | `>= 0, < 1`；`0` 表示纯 ε-DP |
| `ttl_seconds` | int | `1 … 7776000`（最长 90 天），预留有效期；过期未确认自动释放 |

服务在**同一个原子事务**内组合「全部已授权申请 + 全部未过期有效预留 +
本次预留」后按 RDP 投影判定，返回 `201`：

```json
{
  "dataset_id": "sales-2026",
  "reservation_id": "res-20260923-0001",
  "epsilon": 0.2,
  "delta": 1e-8,
  "ttl_seconds": 600,
  "allowed": true,
  "status": "granted",
  "reason": null,
  "epsilon_before": 0.0,
  "epsilon_after": 0.203936,
  "epsilon_remaining": 0.796064,
  "rdp_best_order": 8192.0,
  "idempotent_replay": false,
  "created_at": "2026-09-23T01:10:16.563790+00:00",
  "expires_at": "2026-09-23T01:20:16.563790+00:00",
  "expired_at": null,
  "confirmed_at": null,
  "confirmed_request_id": null,
  "released_at": null
}
```

* 预算内 → `allowed=true, status="granted"`，额度立即锁定，
  `expires_at = created_at + ttl_seconds`；
* 超预算 → 仍返回 `201`（正常受理留痕），但 `allowed=false,
  status="rejected"`，`expires_at=null`，**不锁定任何额度**；
* 数据集不存在 → `404`。

预留状态机：`granted → confirmed | released | expired`，`rejected` 为终态。

### 4.7 预留幂等与冲突语义

* 相同 `(dataset_id, reservation_id)` + **相同负载**
  （`epsilon` / `delta` / `ttl_seconds` 全一致）重复提交：返回 `200`、
  与第一次一致的裁定和时间戳，`idempotent_replay=true`，额度**只锁一次**。
  注意 TTL **不会**因重放而延长（沿用首次的 `created_at`/`expires_at`）；
* 相同 `reservation_id` 但 `epsilon` / `delta` / `ttl_seconds` 任一不同：
  返回 `409 reservation_id_conflict`；
* 被拒绝的预留同样幂等。

### 4.8 确认预留（转为分析申请）

```bash
curl -X POST \
  http://localhost:8000/datasets/sales-2026/reservations/res-20260923-0001/confirm \
  -H 'Content-Type: application/json' \
  -d '{"request_id": "req-20260923-0001", "query_type": "gaussian_mean"}'
```

预留锁定的 `(epsilon, delta)` 变为一条 `granted` 分析申请，`query_type`
与正式 `request_id` 在确认时给出；响应体与 `POST /requests` 的
`Verdict` 完全一致（`201`）。

* 预算**不重复扣减**：确认前由预留锁定的组合消耗，确认后改由该申请承载，
  `epsilon_after` 保持不变；
* 确认必须在预留处于 `granted` 且未过期时进行；
  `released` / `expired` / `rejected` → `409`（错误码分别为
  `reservation_released` / `reservation_expired` /
  `reservation_rejected`）；预留不存在 → `404 reservation_not_found`；
* `request_id` 已被其他申请占用（或与同预留上次确认使用的不同）→
  `409 request_id_conflict` / `reservation_confirmed`，预留保持原状；
* 相同确认负载重放 → `200`、`idempotent_replay=true`，只生成一条申请。

### 4.9 释放预留

```bash
curl -X POST \
  http://localhost:8000/datasets/sales-2026/reservations/res-20260923-0001/release
```

* 对 `granted` 预留：置为 `released` 并记录 `released_at`，锁定额度
  **立即释放**，返回 `200`；
* 重复释放已 `released` / 已 `expired` 的预留：幂等返回最终状态
  （`idempotent_replay=true`）；
* `confirmed` / `rejected` 预留不可释放 → `409`；
  预留不存在 → `404`。

### 4.10 TTL 过期

* 预留不会被后台线程主动删除；任何接触预算的路径（查询预留/预算、新建
  申请或预留、确认、释放）都会在同一临界区内把 `expires_at ≤ now` 的
  `granted` 预留惰性标记为 `expired`（记录 `expired_at`），之后它们**不再
  占用额度**——因此过期后新的申请/预留可以使用这部分额度；
* 过期是终态：过期预留不能再确认（`409 reservation_expired`），对其调用
  release 返回幂等结果；
* `GET …/reservations` 返回全部预留及各自的 `status` 与
  `created_at` / `expires_at` / `expired_at` / `confirmed_at` /
  `released_at` 时间戳，便于审计。

### 4.11 预算释放预测（只读）

在不触碰任何预留状态的前提下，预测到某个未来 UTC 时刻为止，TTL 到期会按
什么节奏归还多少预算：

```bash
curl -G http://localhost:8000/datasets/sales-2026/budget/release-forecast \
  --data-urlencode 'deadline=2026-09-24T00:00:00Z'
```

查询参数：

| 参数 | 类型 | 约束 |
| --- | --- | --- |
| `deadline` | string (query) | 必填，**未来的**带时区 ISO-8601 UTC 时间，如 `2026-09-24T00:00:00Z`；也接受 `+02:00` 等其他 UTC 偏移，服务端归一化到 UTC |

模拟口径：

* 从**当前有效**（`status=granted` 且 `expires_at` 晚于现在）的预留出发；
  已拒绝 / 已确认 / 已释放 / 已过期的预留不参与，已授权申请始终计入；
* 按 `expires_at` 分组、按时间升序模拟：在每个到期时点**一次性移除同一
  时刻到期的全部预留**，并用与预算视图完全相同的 RDP 组合口径重新计算
  `epsilon_consumed` / `epsilon_reserved` / `epsilon_remaining` /
  `rdp_best_order` 与 `active_reservations`；
* 恰好在 `deadline` 时刻到期（`expires_at <= deadline`）的预留视为已移除；
  `expires_at` 严格晚于截止时间（或无到期时间）的预留**继续计入**
  `at_deadline`。

```json
{
  "dataset_id": "sales-2026",
  "generated_at": "2026-09-23T01:10:16.563790Z",
  "deadline": "2026-09-24T00:00:00Z",
  "initial": {
    "epsilon_consumed": 0.0,
    "epsilon_reserved": 0.203936,
    "epsilon_remaining": 0.796064,
    "rdp_best_order": 8192.0,
    "active_reservations": 1
  },
  "events": [
    {
      "expires_at": "2026-09-23T01:20:16.563790Z",
      "released_count": 1,
      "released_reservation_ids": ["res-20260923-0001"],
      "epsilon_consumed": 0.0,
      "epsilon_reserved": 0.0,
      "epsilon_remaining": 1.0,
      "rdp_best_order": 8192.0,
      "active_reservations": 0
    }
  ],
  "at_deadline": {
    "epsilon_consumed": 0.0,
    "epsilon_reserved": 0.0,
    "epsilon_remaining": 1.0,
    "rdp_best_order": 8192.0,
    "active_reservations": 0
  }
}
```

字段说明：`initial` 为「此刻」（任何模拟到期之前）的快照；`events` 每个
元素对应一个不同的到期时刻（同刻到期合并为一条，`released_count` 为该刻
释放数量），预算字段为该刻释放**之后**的状态；`at_deadline` 为截止时间
仍然有效（即尚未被模拟移除）的预留继续计入后的最终快照。

该接口**严格只读**：不会执行惰性过期清理、不会 UPDATE 预留行、不开启写
事务，因此即使数据库里存在「已到点但尚未被其他请求清扫」的预留，调用本
接口也不会改变它们的状态——重复调用（相同截止时间）结果稳定。错误情况：

* 数据集不存在 → `404 dataset_not_found`；
* 缺少 / 无法解析 `deadline` → `422`（FastAPI 校验错误）；
* 无时区信息的裸时间 → `422 invalid_forecast_deadline`；
* `deadline` 不晚于当前时间 → `422 invalid_forecast_deadline`。

### 4.12 epsilon 上限转拨（budget transfer）

在两个已登记数据集之间转拨 epsilon 上限，**delta_cap 与所有已入账申请、
预留都不改变**：

```bash
curl -X POST http://localhost:8000/budget-transfers \
  -H 'Content-Type: application/json' \
  -d '{
    "transfer_id": "xfer-20260923-0001",
    "source_dataset_id": "sales-2025",
    "target_dataset_id": "sales-2026",
    "epsilon_amount": 0.25
  }'
```

请求体：

| 字段 | 类型 | 约束 |
| --- | --- | --- |
| `transfer_id` | string | 必填，客户端生成的**全局**幂等键（字母/数字/`.`/`_`/`-`，≤ 200） |
| `source_dataset_id` | string | 必填，被借记（转出）的数据集 |
| `target_dataset_id` | string | 必填，被贷记（转入）的数据集；必须与来源不同 |
| `epsilon_amount` | float | 必填，`> 0`（正数）且 `≤ 1000` |

转拨前，服务对两端**分别**按现有 RDP 口径计算「全部已授权申请 + 全部未
过期有效预留」的组合占用（即预算视图中的 committed epsilon；本调用内也会
惰性清扫到期预留）。成功条件（在同一个 `BEGIN IMMEDIATE` 事务内判定）：

1. 来源转出后 `epsilon_cap − epsilon_amount > 0`（必须保持**正数**）；
2. 来源转出后的新上限仍足以覆盖其当前占用
   （`新 cap ≥ committed epsilon`，允许 `EPSILON_TOLERANCE` 浮点容差）；
3. 目标转入后 `epsilon_cap + epsilon_amount ≤ 1000`（登记上限天花板）。

任一不满足 → 整笔**拒绝**（`allowed=false, status="rejected"`，`reason`
列出所有失败原因），**两个上限均保持不变**，拒绝尝试同样写入转拨记录留痕。
成功 → `allowed=true, status="transferred"`，返回 `201`：

```json
{
  "transfer_id": "xfer-20260923-0001",
  "source_dataset_id": "sales-2025",
  "target_dataset_id": "sales-2026",
  "epsilon_amount": 0.25,
  "allowed": true,
  "status": "transferred",
  "reason": null,
  "source": {
    "dataset_id": "sales-2025",
    "epsilon_cap": 0.75,
    "epsilon_committed": 0.203936,
    "epsilon_remaining": 0.546064
  },
  "target": {
    "dataset_id": "sales-2026",
    "epsilon_cap": 1.25,
    "epsilon_committed": 0.0,
    "epsilon_remaining": 1.25
  },
  "idempotent_replay": false,
  "created_at": "2026-09-23T01:10:16.563790+00:00"
}
```

`source` / `target` 中的 `epsilon_committed` 是转拨时点该端已授权申请与未
过期预留的 RDP 组合占用；`epsilon_remaining = epsilon_cap −
epsilon_committed`（成功时为转拨后的新 cap，拒绝时为原 cap）。随后用
`GET /datasets/{dataset_id}/budget` 即可看到两端的新 `epsilon_cap`，而
`delta_cap`、`granted_requests`、预留状态均与转拨前一致。

幂等与冲突语义：

* 相同 `transfer_id` + **完全相同负载**（来源、目标、金额一致）重试：
  返回 `200` 与**首次的原始记录**（含首次的裁定、两端 cap 快照与
  `created_at`），`idempotent_replay=true`，**不会再次转拨**；被拒绝的
  转拨同样幂等；
* 相同 `transfer_id` 但来源 / 目标 / 金额任一不同：
  返回 `409 transfer_id_conflict`，上限不变；
* 来源或目标数据集不存在 → `404 dataset_not_found`；
* `epsilon_amount ≤ 0`、两端相同、ID 含非法字符等 → `422`。

并发语义：转拨与普通申请 / 预留共用同一进程内写锁与 `BEGIN IMMEDIATE`
临界区，两端 cap 的「校验 + 更新 + 写转拨记录」在同一个 SQLite 写事务内
完成；并发转拨之间、转拨与申请 / 预留之间无论怎样交错，预算都不会透支
（来源新 cap 永远覆盖其占用），且两端 epsilon_cap 之和守恒。

---

## 5. RDP 组合会计（核算口径）

1. 每个申请被视为一个 `(ε, δ)`-DP 机制，转换为所有 Rényi order
   `α ∈ {1.1, 1.2, …, 10.9, 11, …, 63, 128, 256, 512, 1024, 2048, 4096, 8192}`
   上的 RDP 曲线：

   * 近似 DP（`δ > 0`）：`ε_rdp(α) = ε + log(1/δ) / (α − 1)`；
   * 纯 ε-DP（`δ = 0`）：`ε_rdp(α) = ε`（常数曲线）。

2. 组合（串行/并行同理）在每个 order 上逐点相加：
   `ε_rdp_total(α) = Σ_i ε_rdp_i(α)`。

3. 在数据集的目标 δ（`delta_cap`）处转换回 `(ε', δ)`-DP：

   ```
   ε'(δ) = min_α [ ε_rdp_total(α)
                   + (log((α−1)/α) + log((1−δ)/δ)) / (α−1) ]
   ```

4. 准入判定：把**当前累计曲线 + 本次申请/预留**投影后得到 `ε_projected`，
   当且仅当 `ε_projected ≤ epsilon_cap + EPSILON_TOLERANCE` 时授权/锁定。
   这里的「当前累计曲线」= 全部 `granted` 申请 ∪ 全部**未过期**
   `granted` 预留；已释放/已过期预留不参与组合。

5. 确认（confirm）只是把同一条 `(ε, δ)` 从「预留集合」移到「已授权申请
   集合」：确认前后的组合曲线完全相同，因此不发生二次扣减；释放与过期
   则从组合中移除该条预留，额度立即/自动归还。

特殊情形：

* 全部已授权申请与有效预留都是纯 ε-DP 时，直接采用精确的基本组合
  `ε_total = Σ ε_i`，不收取 RDP 转换的保守余量；
* 纯 DP 与近似 DP 混合时，纯 ε 之和作为常数偏移叠加到 RDP 曲线上再统一
  转换。

> 该口径使用的是教科书式、与机制无关的保守上界（Mironov 2017 的
> (ε,δ)-DP→RDP→(ε,δ) 界），不利用高斯机制等的更紧会计；优点是对任意
> `query_type` 都成立、实现简单可审计。`rdp_curve` 字段暴露各 order 的
> 累计 RDP 值，便于核对。

---

## 6. 并发与原子性

* 每个写操作（登记、提交申请、创建/确认/释放预留、epsilon 上限转拨）在进程内
  `asyncio.Lock` 保护下打开一个 SQLite **`BEGIN IMMEDIATE`** 事务：
  「惰性过期清理 → 查重/查状态 → 重算（已授权申请 + 有效预留）组合消耗 →
  判定 → 插入/更新记录」在同一个事务临界区内完成，并发操作被串行化，
  **申请与预留无论怎样并发交错，已授权 + 有效预留的组合消耗都不会超过
  cap**；
* 转拨事务在该临界区内一次完成「两端占用重算 → 正数/覆盖占用/不超 1000
  校验 → 两端 cap UPDATE → 写入 budget_transfers 记录」：校验失败时回滚
  （两个 cap 不变，仅留一条 rejected 记录），并发转拨与普通申请 / 预留
  交错也不会使来源预算透支；
* 预留的确认同样在该临界区内完成「插入 granted 申请 + 预留置
  confirmed」，两步原子，预算不可能被扣两次；
* SQLite 启用 **WAL** 与 `busy_timeout`：读不阻塞写，写冲突排队等待
  而非立即报错；`(dataset_id, request_id)` 与 `(dataset_id,
  reservation_id)` 上各有 `UNIQUE` 约束作为最终防线；
* 拒绝不写入任何消耗，只留一条 `rejected` 记录，因此并发请求/预留中
  “先到先得、额满即拒”；
* TTL 过期为**惰性**处理：任何读写路径进入临界区时先把到期预留标记为
  `expired` 再计算额度，因此过期预留既不会继续占额，也不会被重复确认。

---

## 7. 数据库迁移

Schema 由 SQLite 的 `PRAGMA user_version` 管理，服务启动时自动顺序执行
待应用的迁移脚本（幂等 DDL，包在一个事务里），**无需人工干预**：

| 版本 | 内容 |
| --- | --- |
| 1 | `datasets`、`requests` 表（原始版本） |
| 2 | 新增 `reservations` 表（含状态、TTL、`expires_at` / `expired_at` / `confirmed_at` / `released_at` 等时间列与部分索引） |
| 3 | 新增 `budget_transfers` 表（全局唯一 `transfer_id`、来源/目标、金额、状态、两端 cap 转拨前后快照与占用、时间戳） |

旧版本数据库（无 `user_version`，即 v0）启动时会先确保 v1 表存在再顺序升级
到最新版本，已有数据集、申请与预留记录原样保留。

---

## 8. 运行测试

```bash
pip install -r requirements-dev.txt
PYTHONPATH=. pytest -q
```

测试覆盖：登记/重复登记、授予/拒绝、拒绝不留痕、纯 ε-DP 精确可加、
RDP 数学性质、幂等回放（含拒绝回放）、`request_id` 冲突 409、缺失
数据集 404，以及 60 路并发申请不超预算、20 路重复 `request_id` 并发仅
一条被创建。

预留相关测试另覆盖：预留锁定/拒绝、预算视图、创建幂等与三种负载冲突
409、确认转申请且不双重扣减、确认幂等/冲突/终态 409、释放幂等与终态
409、TTL 过期在查询与新建时释放额度、60 路并发预留不超预算、15 路并发
确认仅一条申请、预留/确认/释放/直接申请混合并发下始终不超 cap，以及
从 v1 schema 启动时的自动迁移。

只读释放预测测试覆盖：同刻到期分组移除与逐刻 RDP 重算（并用 `rdp.py`
独立核算交叉校验）、恰好在截止时刻到期的边界、早于任何到期的预测区间、
截止时仍有效预留继续计入、拒绝/已释放预留被忽略、非 UTC 带偏移时间归一
化、裸时间/过去时间/缺参数/坏格式返回 422、缺失数据集 404，以及预测
接口不改写预留状态（不触发惰性过期、重复调用结果一致）。

转拨测试另覆盖：两端 cap 原子增减且总量守恒、delta_cap/已入账申请不变、
已授权申请与有效预留均计入来源占用、来源转出后非正数/不足以覆盖占用/目标
超 1000 三类拒绝且两端 cap 不变（含恰好到边界的通过用例）、被拒不阻塞
后续合法转拨、来源/目标缺失 404、负载非法 422、相同负载重试返回原记录
（含被拒重试）、异负载 409、40 路并发转拨恰好不使来源变负、20 路转拨与
20 路普通申请混合并发不透支、20 路重复 `transfer_id` 并发仅一条记录，以及
从 v2 schema 启动时自动迁移出 `budget_transfers` 表。

---

## 9. 项目结构

```
.
├── app/
│   ├── __init__.py
│   ├── config.py      # 环境变量配置
│   ├── rdp.py         # Rényi DP orders、组合与 (ε,δ) 转换
│   ├── schemas.py     # Pydantic 请求/响应模型
│   ├── db.py          # SQLite 连接、schema、数据访问
│   ├── ledger.py      # 原子准入控制 + 幂等业务逻辑
│   └── main.py        # FastAPI 路由与异常处理
├── tests/             # pytest 端到端 + RDP 单元测试
├── Dockerfile
├── docker-compose.yml
├── requirements.txt
├── requirements-dev.txt
└── README.md
```
