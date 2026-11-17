# 制品目录历史查询服务

发布值班工程师追查“误上线时制品目录实际指向哪个版本”的历史查询系统。
源 PostgreSQL 中的制品目录（制品编号 / 服务名 / 版本 / SHA-256）通过
**一致基线 + 逻辑复制**被完整捕获到本地 SQLite 仅增日志，可按任意已捕获
提交点回看整张目录，支持删除后编号重用、多行事务整体可见。

## 1. 架构与正确性设计

```
PostgreSQL 源库 (artifacts 固定四列)
   │  ① 建逻辑复制槽 test_decoding + pg_export_snapshot 导出一致性快照
   │  ② 另开连接 SET TRANSACTION SNAPSHOT 复用快照做整表基线
   │  ③ 从槽 LSN 持续消费逻辑变更流（walsender）
   ▼
捕获工作线程（FastAPI 进程内 daemon thread）
   │  每个源事务：先整体写 SQLite（行变更 + 提交点同一事务）
   │              落盘成功后才 send_feedback 推进源端槽位
   ▼
SQLite 历史库（WAL，仅增不改）  ←→  FastAPI 网页 / JSON API
```

### 关键不变量

| 需求 | 实现方式 |
| --- | --- |
| 一致基线，普通 SQL 不中断 | 建槽与 `pg_export_snapshot()` 在同一事务完成；基线用 `SET TRANSACTION SNAPSHOT` 读一致性快照，源库无需停机、普通读写照常提交 |
| 基线前开始、基线后提交的事务不漏 | 该事务对导出快照不可见（不进基线），但其提交记录的 LSN 晚于槽起始 LSN，必然出现在逻辑流中被补捕获（自测第 2 组确定性验证） |
| 多行事务整体可见 | 一个源事务的全部行变更与提交点在**同一个 SQLite 事务**落盘，不存在半条事务 |
| 删除后重用编号保留旧历史 | `artifact_history` 仅追加，每行带 `change_type` 与提交点；按编号取“最后一条 ≤ 查询点”的记录重建目录，DELETE 与后续 INSERT 各自留痕 |
| 源断连 / 强杀重启后续接 | 复制槽持久化在源库；应用重启从槽位重放，SQLite 以 (世代, 源事务 xid) 为主键幂等去重；断线自动指数/固定退避重连 |
| 不确认尚未落盘的更改 | 严格“先 SQLite commit，后 `send_feedback(flush_lsn=...)`”；keepalive 与无关消息一律不回送反馈。自测断言槽 `confirmed_flush_lsn` 永不超前已落盘提交点 |
| 初始化未完成不开放历史 | 状态机 `INITIALIZING → STREAMING / ERROR / REBUILDING`；不存在任何已完成世代时 `/api/points`、`/api/directory`、`/api/history/*` 一律 503；一旦有世代落盘，即使 ERROR/REBUILDING 旧段历史仍可回看 |
| 复制槽被删后“重建捕获” | 网页/API 确认旧槽缺失后触发：旧历史（世代 0…N-1）一行不动，新槽 + 新一致性快照 + 新基线在**新世代 N** 原子落盘，随后接续逻辑复制；段间未覆盖区间以 GAP 显式标注，旧点只按旧段回看、新点不混入断档前残留 |
| 重建中断不暴露半段 | 世代行与整批 BASELINE 在同一 SQLite 事务；建槽/快照/落盘任一环节中断，重启后要么只收尾（世代已落盘）、要么清理孤儿槽整体重做，绝不出现半空世代、绝不清除旧历史；落盘前不回送任何 feedback |
| 固定结构的增改删 | 网页/API 只接受 `insert/update/delete` 三类操作，pydantic 判别联合按 `op` 分流，仅四列，sha256 强制 64 位十六进制；无任何 DDL/任意 SQL 通道 |

### 断档重建（generation / GAP）模型

- `generations` 表每一行是一段连续捕获历史：`INITIAL`（世代 0）或
  `REBUILD`（世代 ≥1）。重建在**新世代号**下落基线，永不覆盖旧世代。
- 相邻两代之间的未覆盖区间为
  `(上一世代最后捕获点, 新基线点)`，两端都不含：区间内源变更无法追查，
  在 `/api/points` 中以 `kind=GAP` 的独立行明确标出，GAP 行不可回看，
  对区间内 LSN 调 `/api/directory` 返回 404 并提示“未覆盖区间”。
- `artifact_history` / `txns` 全部带 `generation` 列，目录回看严格按
  `(generation, lsn)` 过滤：新世代点只读到新世代数据，断档前的旧值
  不会残留进新目录；旧世代点也看不到新世代数据。
- 重建期间源库普通 SQL 全程可提交：快照点之后打开、之后提交的事务不进
  基线，但必然出现在从新槽 LSN 开始的逻辑流中，只被捕获一次
  （按 `(generation, xid)` 幂等去重）。
- 旧版本 SQLite（无 generation 列）启动时自动、幂等迁移：回填世代 0，
  不改动任何历史行。

### SQLite 表结构（`/data/history.db`）

- `capture_state`：阶段、槽名、各世代基线 LSN、最近提交点/消息 LSN、
  重建待提交世代号、错误信息。
- `generations`：世代号、类型（INITIAL/REBUILD）、槽名、基线 LSN、
  与上一世代之间的 GAP 区间、备注。
- `txns`：(世代, 源事务 xid) 联合主键、提交 LSN、提交时间。
- `artifact_history`：仅增变更链（BASELINE/INSERT/UPDATE/DELETE），带
  世代号、xid、提交 LSN 与数值化 LSN，支持按世代+提交点过滤与编号重建。

## 2. 用 Docker Compose 启动（交付方式）

```bash
docker compose up --build
# 网页：http://localhost:8000/
# 源 PostgreSQL：localhost:5432（user/pass/db = postgres/postgres/postgres）
```

- `postgres` 服务：`postgres:16-bookworm`，已开启 `wal_level=logical`，
  命名卷 `pgdata` 持久化；首次启动自动执行 `init/01_schema.sql`
  幂等建表并播种 3 行示例数据。
- `app` 服务：本仓库构建的 Python 3.12 镜像，命名卷 `historydata`
  挂载到 `/data` 持久化 SQLite；`restart: unless-stopped`，源库健康检查
  通过后才启动。
- 两侧均为命名卷，`docker compose down` 不丢数据，`down -v` 才清空。

## 3. 网页功能（单页应用，无前端构建）

1. **捕获进度**：阶段徽章（含 REBUILDING）、复制槽、当前基线/最近提交
   LSN、世代数与当前世代、已捕获事务数、历史行数、最近错误，3 秒自动刷新。
2. **重建捕获**：确认旧槽缺失后一键触发；槽仍存在 / 重建进行中会被拒绝
   （409）。触发后旧历史保留，新基线写入新世代，页面把两段历史与中间
   GAP 明确分开。
3. **成组修改源目录**：动态增删 insert/update/delete 行，整组在一个源事务
   提交；任一操作失败整组回滚。
4. **提交点回看**：按世代列出 BASELINE 与每个源事务提交点（世代、LSN、
   xid、时间、变更行数），段间插入黄色 GAP 行（不可回看）；点击普通行
   查看该时刻整张目录，目录严格按所选世代重建。
5. **单编号完整历史**：含各世代 BASELINE/INSERT/UPDATE/DELETE 全链，
   直观验证“删除后重用编号，旧历史保留”及跨世代分段。

## 4. HTTP API

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET | `/api/progress` | 捕获状态与进度（含世代信息，初始化期也可用） |
| GET | `/api/generations` | 各捕获世代及相邻世代间 GAP 区间 |
| POST | `/api/capture/rebuild` | 触发重建捕获（旧槽缺失前置；不满足返回 409） |
| GET | `/api/source/artifacts` | 源库当前目录 |
| POST | `/api/source/operations` | 成组增改删，单源事务，失败整组回滚 |
| GET | `/api/points` | 全部可查询提交点（含 GAP 标记；未就绪时 503） |
| GET | `/api/directory?point=0/16B3748` | 该提交点（含）的整目录，按其世代隔离 |
| GET | `/api/history/{artifact_no}` | 单编号完整变更链（按世代分段） |

请求示例：

```bash
curl -X POST localhost:8000/api/source/operations -H 'Content-Type: application/json' -d '{
  "operations": [
    {"op":"insert","artifact_no":1004,"service_name":"cart-svc","version":"3.1.0",
     "sha256":"abcd1234abcd1234abcd1234abcd1234abcd1234abcd1234abcd1234abcd1234"},
    {"op":"delete","artifact_no":1003}
  ]}'
curl 'localhost:8000/api/directory?point=0/16B3748'
```

## 5. 自测

### 5.1 解析器单元测试

```bash
python3 -m unittest tests.test_decoder -v
```

覆盖 BEGIN/COMMIT、带空格与转义引号的值、DELETE 仅主键、其他表/DDL 忽略。

### 5.2 端到端自测（原链路 30 项 + 重建捕获 59 项断言）

`tests/selftest.sh` 与 `tests/selftest_rebuild.sh` 在本机自动拉起便携
PostgreSQL（不依赖 Docker）、启停应用并完成全链路验证：

```bash
# 可选：指定本机 PostgreSQL 二进制位置
export PGBIN=/path/to/postgresql/bin PGCLIENT=/path/to/client/bin
bash tests/selftest.sh          # 原链路：30 passed
bash tests/selftest_rebuild.sh # 断档重建：59 passed
```

`selftest.sh` 覆盖场景：

1. 初始化窗口历史接口 503，完成后 STREAMING 与基线目录正确；
2. **跨基线事务**：通过应用内置测试钩子
   （`TEST_INFLIGHT_ARTIFACT_NO` 环境变量，默认关闭）确定性构造
   “基线快照点之后打开、基线落盘之后提交”的事务，断言不进基线、
   不被遗漏、在其提交点可见；
3. 多行事务只产生一个提交点、同事务插入/删除同时生效；
4. 删除后重用编号，旧历史链完整（BASELINE,DELETE,INSERT）；
5. 失败整组回滚、非法 sha256/非法 LSN 参数校验；
6. `kill -9` 强杀应用后重启：事务数不重复、最后更新仍在、
   槽 `confirmed_flush_lsn` 不超前已落盘 LSN、续接后新变更继续捕获；
7. 源 PostgreSQL 重启后应用自动重连恢复 STREAMING，恢复后变更不丢；
8. SQLite 与 PostgreSQL 数据目录均落盘持久化。

`selftest_rebuild.sh` 覆盖场景：

1. 槽仍存在时重建请求被拒（409）且槽不被误删；
2. 删槽制造断档：应用降级 ERROR，但旧世代历史接口仍 200、旧点正常回看；
3. 断档期普通 SQL 照常提交且不混入旧段、在旧段无历史痕迹；
4. 触发重建：同名新槽建立、世代数 +1、恢复 STREAMING；
5. 两段历史与 GAP 明确分开：GAP 起点=旧段最后捕获点、终点=新基线点，
   GAP 内 LSN 返回 404“未覆盖”，新基线=重建时刻源目录当前值，
   新点来源是新 BASELINE 而非旧残留，旧历史行数只增不减；
6. 重建后逻辑流续接，新变更归属新世代、同事务只出现一次、槽位不超前；
7. **中断重试（落盘前）**：建槽后强杀，重启清理孤儿槽整体重做，
   不存在无 BASELINE 的半空世代，旧历史完整；
8. **中断重试（落盘后收尾前）**：用 `TEST_REBUILD_SLEEP_AFTER_BASELINE_MS`
   钩子确定性构造“基线已原子落盘、未切回 STREAMING”崩溃，重启只收尾、
   不产生重复/空世代，复制槽保留，流继续工作；
9. 重建窗口内源库普通 SQL 持续可提交，期间写入进入新基线；
10. 重建进行中重复请求返回 409；全部 `(generation,xid)` 唯一无重复事务。

最近一次结果：**原链路 30 passed / 0 failed；重建 59 passed / 0 failed**。

## 6. 配置（环境变量）

| 变量 | 默认 | 说明 |
| --- | --- | --- |
| `PG_DSN` | 本地开发 DSN | 源库 libpq 连接串，Compose 中指向 `postgres` 服务 |
| `SQLITE_PATH` | `/tmp/capture/history.db` | 历史库路径（Compose 中为 `/data/history.db`） |
| `REPLICATION_SLOT` | `artifacts_history_slot` | 逻辑复制槽名 |
| `SOURCE_TABLE` | `artifacts` | 捕获的源表（结构固定） |
| `REPL_STATUS_INTERVAL_S` | `5` | 复制协议 keepalive 间隔 |
| `REPL_RECONNECT_BACKOFF_S` | `2` | 断线重连退避秒数 |
| `CAPTURE_INIT_SLEEP_MS` | `0` | 测试钩子：拉长初始化窗口 |
| `TEST_INFLIGHT_ARTIFACT_NO` | `0`(关) | 测试钩子：跨基线事务编号 |
| `TEST_INFLIGHT_SLEEP_S` | `3` | 测试钩子：在途事务持有秒数 |
| `TEST_REBUILD_SLEEP_AFTER_BASELINE_MS` | `0`(关) | 测试钩子：重建基线落盘后、切回 STREAMING 前的等待毫秒，用于验证“落盘后崩溃只收尾” |

## 7. 已知边界 / 未完成项

- 逻辑解码使用 PostgreSQL 内置 `test_decoding`（无需安装插件），解析器
  依赖本项目固定的四列结构；表结构变更不在支持范围内（需求明确禁止 DDL）。
- 历史库为单进程 SQLite，适合值班排查类读多写少场景；如需多副本水平扩展
  需更换存储层。
- 复制槽被人工删除后，应用进入 ERROR 并在 `/api/progress` 暴露，但**不会
  自动重建**：断档区间内的源变更无法追回，必须由值班工程师在网页确认旧槽
  缺失后手动点“重建捕获”，系统会显式标注 GAP 并在新世代接续。
- 重建沿用与原槽同名的逻辑复制槽；若源库上存在被其他用途占用的同名槽，
  重建前置校验（槽缺失确认）会阻止操作。
- 沙箱环境未提供 Docker，Compose 文件未经本机 `docker compose` 实跑，
  但应用本体已用同等配置在本机 PostgreSQL 15 + uvicorn 下完成全部端到端
  验证（含断档重建与中断重试，共 89 项断言）；Compose 镜像基于官方
  `python:3.12-slim` 与 `postgres:16-bookworm`，Dockerfile/Compose 无需
  为重建功能做任何改动。
