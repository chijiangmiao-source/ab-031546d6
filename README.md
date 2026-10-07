# 遥感档案迁移一致性演练台 (Remote-Sensing Migration Drill)

把持续被旧采集端写入的遥感观测记录迁入新索引结构的一致性演练系统。
页面上可创建含**初始记录**、**按复制批次触发的旧端更新**和**可选中断点**的演练，
启动迁移后实时查看：源表/目标表投影、快照水位、增量序号、分页进度与最终切换结论。

## 一致性保证（核心语义）

服务严格按以下顺序推进，所有进度均持久化在 SQLite (WAL) 中：

1. **先冻结复制边界** `INTENT → FROZEN`：记录单调水位 `watermark`，并把水位下
   全部源行固化为**不可变快照镜像** (`snap_row`)，之后的旧端写入无法回改快照。
2. **边界后的旧端写入持久化为有序增量**：每批写入在同一事务内更新源表并追加
   `increment` 日志（严格单调 `seq`）+ `fired_batch` 去重表，要么整批落盘要么没有。
3. **恰好一次投影**：快照按页 (`snap_page` 断点续传) + 增量按 `seq` 顺序投影；
   每条增量的「投影 + applied 标记」同一事务提交，UPSERT 本身幂等，
   崩溃重启后未提交的投影会重做、已提交的绝不重做 —— 不丢失、不重复、不回退。
4. **覆盖性闸门 + 原子切换**：只有当目标表覆盖完整快照**且**全部已记录增量
   （无缺失行、无版本回退、无 payload 差异）时，才写入 `SWITCH_PENDING` 切换意图；
   随后在**同一事务内复检覆盖并翻转读取指针** `source → target`（`SWITCHED`）。

### 崩溃恢复（中断点 = 真实 `os._exit` 杀进程）

| 中断点 | 落盘了什么 | 重启后行为 |
|---|---|---|
| 复制页提交后 `after_submit` | 迁移意图（`migration_id` 唯一） | 从 INTENT 继续冻结→复制→应用→切换，收敛为同一完成结果 |
| 增量落盘后 `after_increment_persist` | 有序增量 + fired 标记 | 未投影的增量继续恰好一次投影，已落盘的不重放 |
| 切换意图写入后 `after_switch_intent` | SWITCH_PENDING，指针未动 | 复检覆盖后原子切指针；明确「已写意图、尚未切换」的中间态也可恢复 |

中断点由持久化 `crash_mark` 保证**只触发一次**，容器重启不会在同一点位崩溃循环。

- **重传相同 `migration_id`**：UNIQUE 约束 + 去重表 → 返回幂等成功，**绝不重放更新**；
  同一 id 用于另一个演练（改动内容）返回 **409 拒绝**。
- **演练内容不可变**：系统不提供任何修改演练内容的接口（PUT/PATCH/DELETE 返回 404/405）。

## 运行

```bash
# 可配置宿主端口（默认 8080）
HOST_PORT=9090 docker compose up -d --build
# 打开 http://localhost:9090
```

验证服务：构建应用镜像 → 执行迁移一致性代码测试（含真实子进程崩溃重启）
→ 对运行中的 app 容器做 HTTP 冒烟，全部通过后以退出码 0 结束：

```bash
docker compose build
docker compose run --rm verify      # 退出码即结论；或: docker compose up --build verify
echo $?
```

## 本地（无 Docker）

```bash
pip install 无第三方依赖   # 仅 Python 3.11 标准库
DATA_DIR=./data PORT=8080 python -m app.main
python -m unittest discover -s tests -v        # 一致性 + 崩溃恢复测试
python scripts/http_smoke.py http://127.0.0.1:8080
```

## HTTP API（真实接口，非 mock）

| 方法 | 路径 | 说明 |
|---|---|---|
| GET | `/health` | 健康状态（DB/WAL、启动时间） |
| POST | `/api/drills` | 创建演练（初始记录、页大小、批次、中断点） |
| GET | `/api/drills` | 演练列表 |
| GET | `/api/drills/{id}` | 完整状态：双表投影、水位、分页、增量序号、事件流、切换结论 |
| POST | `/api/drills/{id}/migrate` | 提交迁移意图（`migration_id` 幂等键） |
| POST | `/api/drills/{id}/batches/fire` | 手动触发一批旧端更新（重复触发幂等） |

## 布局

```
app/db.py       持久化 schema、SQLite(WAL) 连接
app/engine.py   状态机：冻结/快照/增量日志/恰好一次投影/覆盖闸门/原子切换/收敛 worker
app/api.py      WSGI HTTP API
app/main.py     入口：建库 → 从持久化状态恢复 → 后台收敛 → 对外服务
app/static/     演练页面（实时轮询、崩溃重启自动幂等重试）
tests/          一致性 + 三中断点真实子进程崩溃恢复测试
scripts/        HTTP 冒烟脚本（compose verify 使用）
```
