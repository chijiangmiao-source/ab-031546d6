# 遥感档案站 · 观测记录索引迁移一致性演练场

把持续被**旧采集端**写入的观测记录迁入新索引结构的可演练系统。用户可在页面创建
含初始记录、按复制批次触发的旧端更新和可选中断点的演练，启动迁移并实时观察
**源表 / 目标表投影、快照水位、增量序号与切换结论**。

## 一致性保证（本题核心）

迁移流水线：

```
冻结复制边界 ──▶ 复制快照 ──▶ 持久化有序增量 ──▶ 恰好一次投影 ──▶ 原子切换读取指针
```

1. **先冻结复制边界**：`freeze_boundary` 在单个 SQLite 事务中写入 migration 行，
   并从源表复制一份点时间快照 `snapshot_rows`。边界与快照原子，任何旧端写入都
   不可能横跨边界。
2. **边界后的旧端写入先落盘为有序增量**：写入与幂等令牌同事务提交到
   `increments(seq, op, rid, payload)`，`seq` 在锁内单调分配，然后才允许被投影。
   旧端同时仍写活的源表（采集端不停止）。
3. **恰好一次、有序投影**：目标行变更与 `last_applied_seq` 水位在同一事务提交，
   并带 `last_applied_seq = seq-1` 的严格守卫——序号重复或有缺口会回滚事务，
   因此既不会投影两次也不会跳过。
4. **仅当目标覆盖快照与全部已记录增量才切换**：闸门用纯函数
   `expected_target = 快照 ⊕ 全部增量` 与目标表逐行比对（`content_match`），
   且 `last_applied_seq >= max(seq)`。条件不满足时读取指针保持 `source`。
5. **原子切换**：`UPDATE ... WHERE read_pointer='source'` 条件更新 +
   `rowcount==1` 检查，指针只翻转一次；切换后旧采集端的迟到写入返回
   `409 cutover_complete`，指针不可回退。

### 崩溃与重启收敛

中断点（硬中断，`os._exit(7)`，均在对应**提交已 fsync 之后**触发）：

| 中断点 | 时机 |
| --- | --- |
| `after_drill_submit` | 复制页提交后 |
| `after_freeze` | 冻结复制边界后 |
| `after_snapshot` | 快照复制后 |
| `after_increment` | 增量落盘后 |
| `after_project` | 增量投影后 |
| `after_switch_intent` | 切换意图写入后 |
| `after_switch` | 原子切换后 |

所有进度都在 SQLite（WAL + `synchronous=FULL`）。重开服务时 `recover_on_boot`
扫描持久化状态并恢复工作线程，从水位继续，最终只收敛为两种结果之一：

- **已切换（target）**：目标逐行等于快照+全部增量，源/目标一致；
- **明确未切换（source）**：覆盖闸门给出未满足项。

中断点只触发一次（记录在 `fired_crashes`），因此 Compose 的自动重启不会变成
重启循环。

### 幂等与不可变

- **重传相同 `migration_id`** 不重放任何更新（`migrations.migration_id UNIQUE`）；
  同一标识用于不同演练返回 `409`；重复冻结返回 `created=false`。
- **写入令牌**：每条旧端写入带稳定 token（`ingested_writes` 去重表），批次完成
  以每条写入的 token 为准——批次中途崩溃会补完剩余写入而不是跳过整批。
- **演练内容不可修改**：同一 `drill_id` 提交不同定义返回 `409 drill_immutable`；
  完全相同的定义重复提交幂等成功。

## 运行

需要 Docker + Compose（镜像基于 `python:3.11-slim`，应用本身为**纯标准库**，
构建无需联网安装任何包）。

```bash
# 默认宿主端口 8080；可用 HOST_PORT 覆盖
HOST_PORT=9090 docker compose up --build web
# 打开 http://localhost:9090
```

### verify 服务（代码测试 + 镜像构建 + HTTP 冒烟，退出码结束）

```bash
docker compose up --build verify
# 或分步：
docker compose build
docker compose run --rm verify
echo "exit code = $?"
```

`verify` 服务一次性执行 `verify.sh`：

1. `python -m unittest discover` —— 迁移一致性代码测试，含针对每个中断点的
   **真实子进程硬崩溃 → 重开收敛**测试（`tests/test_restart.py`）；
2. 镜像健全性检查（`app` 模块在构建出的镜像中可导入）；
3. 对健康的 `web` 容器跑 `tests/smoke_http.py` HTTP 冒烟（真实 API 完整迁移、
   幂等、不可变、拒绝切换后回退）。

成功以退出码 0 结束，任一检查失败以非零退出码结束。

### 本地无 Docker 运行

```bash
python3 -m unittest discover -s tests -p 'test_*.py'   # 全部测试
HOST=127.0.0.1 PORT=8080 DB_PATH=./data/archive.db python3 -m app
BASE_URL=http://127.0.0.1:8080 python3 tests/smoke_http.py
```

## HTTP API（页面全部通过真实 API 操作）

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET | `/healthz` | 健康状态（页面右上角轮询显示） |
| POST | `/api/drills` | 创建演练（同 id 不同内容 → 409） |
| GET | `/api/drills` | 演练列表 |
| GET | `/api/drills/{id}` | 源表/快照/目标投影、增量、覆盖闸门、事件时间线 |
| POST | `/api/drills/{id}/migration` | 冻结边界并启动（重传相同 id 不重放） |
| POST | `/api/drills/{id}/writes` | 旧采集端写入（token 幂等；切换后 → 409） |
| POST | `/api/drills/{id}/resume` | 手动驱动恢复流水线 |

创建演练请求示例：

```json
{
  "name": "晨昏轨道演练",
  "records": [
    {"rid": "RS-1001", "payload": "L1A 多光谱帧 …"}
  ],
  "updates": [
    {"batch": 1, "op": "upsert", "rid": "RS-1002", "payload": "重处理后 …"},
    {"batch": 2, "op": "delete", "rid": "RS-1003"}
  ],
  "crash_point": "after_switch_intent"
}
```

## 代码结构

```
app/
  storage.py    SQLite 持久化：快照/源/目标/增量/migration，覆盖闸门与原子切换
  migration.py  校验、幂等摄取、可恢复工作线程、崩溃点、启动恢复
  server.py     标准库 HTTP API + 静态页面
  static/       演练页面（创建、控制、实时投影、水位/序号、切换结论、健康状态）
tests/
  test_core.py     15 个一致性单测（异常模式模拟崩溃后重开）
  test_http.py     针对真实 HTTP 服务器的端到端测试
  test_restart.py  真实子进程 os._exit(7) 硬崩溃 → 重启收敛（全部中断点）
  smoke_http.py    Compose verify 使用的 HTTP 冒烟（退出码）
Dockerfile        python:3.11-slim，零运行时依赖，含 HEALTHCHECK
docker-compose.yml web（HOST_PORT 可配置）+ 一次性 verify
verify.sh         verify 服务入口
```
