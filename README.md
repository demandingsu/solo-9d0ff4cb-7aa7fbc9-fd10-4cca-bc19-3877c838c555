# 园区能源日用电量核算 API

把逐小时电表**累计读数**换算成计量点的**日用电量**（UTC 日界），供结算使用。
FastAPI + SQLite，支持换表（含迟到换表单补录）、异常数据返回待核区间、
两个 API 实例并发导入与查询。

**数值精度**：累计读数与换表边界读数以十进制原文（`TEXT`）持久化，核算全程
`Decimal` 精确运算，响应中的数值字段以裸 JSON 数字输出（不经 float 往返、
不做定点量化）。因此 1e12 量级下每小时仅增加 `0.00001 kWh` 的合法读数也能精确
计入日总量，不同读数不会因浮点退化被当成同值重传。

## 快速开始

```bash
docker compose up --build
# api1: http://localhost:8001/docs
# api2: http://localhost:8002/docs   （两个实例共享同一 SQLite 库）
```

启动后跑一遍完整演示（跨午夜换表、迟到单据补录重算、倒退/缺整点/边界冲突、失败导入回滚）：

```bash
./scripts/demo.sh                  # 默认打 http://localhost:8001，同时验证 8002
./scripts/demo_precision.sh        # 精度修复演示：修复前 vs 修复后、旧库迁移与校正
```

本地开发（不用 Docker）：

```bash
pip install -r requirements-dev.txt
DB_PATH=./energy.db uvicorn app.main:app --port 8001
pytest tests/ -q
```

## 精度问题与修复

修复前的实现把累计读数存为 SQLite `REAL`（IEEE-754 double）并在输出时量化到
`1e-6`。在合法上限 `1e12` 附近，double 的分辨率约 `1.2e-4`，导致：

- 每小时 `+0.00001 kWh` 的增量在存储时被吞掉，日总量被低估
  （示例 24 小时应得 `0.00024`，修复前只有 `0.0002`）；
- `.99000`、`.99001`、`.99002` 等不同输入在 float 下退化为同一存储值，
  异值重传会被误判为幂等重复而静默跳过。

修复内容：

1. **导入**：CSV 中的十进制文本直接校验并规范化为十进制原文（保留输入小数位），
   不再经过 `float()`。
2. **持久化**：`readings.cum_kwh`、`meter_changes.old_final_kwh/new_initial_kwh`
   改为 `TEXT`（SQLite TEXT 亲和性，十进制原文）。
3. **冲突判断**：用 `Decimal` 精确比较；只有数值真正相等才计为幂等重复，
   不同值整批 `409` 回滚。
4. **日核算**：增量与日总量全程 `Decimal` 相减/求和，不做定点量化。
5. **响应**：新增 `DecimalJSONResponse`，把 `Decimal` 输出为裸 JSON 数值，
   任意精度且保持数值字段语义（不是字符串）。响应报文中的十进制文本是精确的；
   客户端若用 IEEE-754 double 解析 JSON 数字会自行丢精度，需要精确小数时应
   使用十进制解析（如 Python `json.loads(..., parse_float=Decimal)`）。

### 旧库迁移

启动时若检测到旧版 `REAL` 列（以 `PRAGMA user_version` + 列类型判定），在**单个
`BEGIN IMMEDIATE` 事务**内完成：建新表 → `CAST` 旧值 → 全部标记
`legacy_unverified=1` → 重建索引 → 设置 `user_version`。任一步失败整体 `ROLLBACK`，
旧库保持原样（不会留下半迁移表）。

`CAST(REAL AS TEXT)` 只能还原 SQLite 的最短往返表示，**原始 CSV 的小数已不可知**。
因此迁移后：

- 相关增量标记 `legacy_precision_unverified`，该日 `total_kwh=null`，
  只给待核区间，**不给确定日总量**；
- 旧库迁移值与常规读数的小数差异不会被误判为 `boundary_conflict`
  （差异本身不可信），但仍以待核呈现；
- 把**原始 CSV 重新导入**即可校正：导入响应给出 `legacy_corrected` 数量与
  `legacy_corrections` 明细（旧值、新值、`changed` 是否真的变化），行上的
  `legacy_unverified` 清零。换表单同理（整条单据一起恢复）。
- 校正采用**整批原子**语义：一旦同批其他行冲突仍整体回滚，不会留下部分校正；
  旧值即使恰好等于 float 换算值也照常计入校正并报告（`changed=false`），
  不会“看起来相同”就静默跳过。

### 双实例交错启动

- 迁移与初始化只使用显式事务，不用 `executescript`（它会隐式 COMMIT）；
- 拿到写锁后二次检查 `user_version`，另一实例已完成则直接复用；
- WAL 切换在旧库首次被并发打开时会遇到 `database is locked`（`journal_mode`
  切换不走 `busy_handler`），连接层显式重试；
- 读事务看到的是 WAL 的完整已提交快照，绝不会看到迁移中的半成品表。
  失败迁移不影响旧库可读性，下次启动可重试。

## 核算规则

- 结算日为 **UTC 日界**：某日用电量 = 相邻两个 UTC 午夜之间 24 个整点增量之和，
  需要当日 `00:00` 到次日 `00:00` 共 25 个整点读数。
- **换表整点 T**：换表单给出旧表末读数与新表初值（边界读数），可代替该整点的常规读数；
  `T-1→T` 为旧表末段、`T→T+1` 为新表首段，分别计算后相加。换表发生在午夜时，
  末段计入前一日、首段计入当日，天然支持跨午夜换表。
- 以下情况**不给确定日总量**（`total_kwh=null`），只返回**待核区间** `pending_intervals`
  （`known_kwh` 为可确定部分的电量，仅供参考）：

  | reason | 含义 |
  |---|---|
  | `missing_hour` | 缺整点读数 |
  | `unrecorded_meter_change` | 相邻整点表号切换但无换表单 |
  | `meter_regression` | 同表累计读数倒退 |
  | `boundary_conflict` | 常规读数与换表单边界读数（或表号）冲突 |
  | `legacy_precision_unverified` | 旧库 REAL 迁移值未按原始 CSV 校正，原始小数不可知 |

- **迟到单据**：核算为按需实时计算。补录换表单后，导入响应会给出 `affected_dates`
  （受影响结算日），重新查询这些日期即得重算结果，无需手工触发重算。

## CSV 格式

读数 CSV（`POST /api/imports/readings`，multipart 文件字段名 `file`）：

```csv
mp,meter,ts,cum_kwh
MP-001,M100,2026-01-09T00:00:00Z,1000.0
```

换表单 CSV（`POST /api/imports/meter-changes`）：

```csv
mp,change_ts,old_meter,new_meter,old_final_kwh,new_initial_kwh
MP-001,2026-01-10T00:00:00Z,M100,M200,1240.0,40.0
```

- `ts`/`change_ts` 必须是 **UTC 整点**（可带时区偏移，会归一化为 UTC，但必须落在整点）。
- 数值为十进制原文，最多 18 位小数、40 位有效数字（远高于电表实际精度）。
- 整批原子导入：任一行校验失败（422）或与已提交数据冲突（409）→ 整批回滚，已有结果不变。
- 同值重复导入幂等（计 `duplicates_skipped`），不同值视为冲突。
- 旧库迁移的未校正行（`legacy_unverified=1`）在重传原始值时计 `legacy_corrected`。

## API 一览

| 方法 | 路径 | 说明 |
|---|---|---|
| POST | `/api/imports/readings` | 导入读数 CSV |
| POST | `/api/imports/meter-changes` | 导入换表单 CSV（可迟到补录） |
| GET | `/api/settlement/daily?mp=&date=` | 单日核算（确定总量或待核区间） |
| GET | `/api/settlement/range?mp=&from=&to=` | 区间核算（同一已提交快照，≤63 天） |
| GET | `/api/metering-points` | 计量点总览 |
| GET | `/api/readings?mp=&from=&to=` | 读数查看（含 `legacy_unverified`） |
| GET | `/api/meter-changes?mp=` | 换表单查看（含 `legacy_unverified`） |
| GET | `/api/imports` | 导入批次列表 |
| GET | `/api/health` | 健康检查 |
| GET | `/api/demo/scenario` | 演示：修复前 vs 修复后 + 旧库校正时间线 |
| POST | `/api/demo/pre-fix-preview` | 演示：同批 CSV 在修复前（float）下的结果（不写库） |
| POST | `/api/demo/legacy-seed/readings` | 演示：模拟旧库迁移后未校正读数（`mp` 须 `DEMO-` 前缀，`confirm=true`） |
| POST | `/api/demo/legacy-seed/meter-changes` | 演示：模拟旧库迁移后未校正换表单 |
| POST | `/api/demo/reset?mp=&confirm=true` | 演示：清理 `DEMO-` 计量点数据 |

交互式文档见 `/docs`（Swagger UI，可直接上传 `samples/` 里的 CSV 演示）。
`/docs` 中调用 `GET /api/demo/scenario` 时选择 `confirm=true` 会同时把修复后格式的
`DEMO-PREC` 数据写入正式库，便于与 `pre_fix` 结果对比。

## 并发与一致性设计

两个 API 实例共享同一 SQLite 库文件（compose 中的 named volume）：

- **WAL 模式**：核算查询在显式只读事务（`BEGIN … COMMIT`）内执行，
  读事务看到一致的已提交快照——一次核算只读同一已提交状态，
  即使另一实例正在导入也不会读到半截批次。响应中的 `snapshot.max_import_id`
  即本次核算所读快照的批次水位。
- **原子导入**：每批导入在单个 `BEGIN IMMEDIATE` 事务内校验+写入，
  任何失败整体 `ROLLBACK`，已有核算结果不变；`busy_timeout` 吸收两实例间的写锁竞争。
- **原子迁移**：schema 初始化/旧库迁移同样在单个 `BEGIN IMMEDIATE` 事务内完成，
  以 `user_version` + 写锁后二次检查保证多实例只迁移一次；失败回滚不留半成品。
- 失败导入不留痕迹（`imports` 表也只有已提交批次）。

`tests/test_app.py` 含多线程并发导入+快照读的一致性回归测试；
`tests/test_precision_and_migration.py` 覆盖十进制精度、旧库迁移原子性、
失败迁移回滚、双实例交错首次启动（多轮压力测试）。

## 样例数据（samples/）

| 文件 | 场景 |
|---|---|
| `readings_mp001.csv` | MP-001 跨午夜换表（2026-01-10T00:00Z M100→M200），单据未到 |
| `meter_changes_mp001_late.csv` | MP-001 迟到的换表单（旧表末读数 1240.0 / 新表初值 40.0） |
| `readings_mp001_conflict.csv` | 与已提交读数冲突的文件（演示 409 + 整批回滚） |
| `readings_mp002.csv` | MP-002 同表倒退（05:00→06:00）+ 缺整点（14:00） |
| `readings_mp003.csv` + `meter_changes_mp003.csv` | MP-003 日中换表，单据新表初值与常规读数冲突 |
| `precision/readings_tiny_increment.csv` | 合法大累计读数每小时仅 +0.00001 kWh（修复前低估） |
| `precision/readings_distinct_repr.csv` | 旧版 float 下退化为同值的不同读数 |
| `precision/readings_distinct_repr_conflict.csv` | 上述读数的异值重传（修复后 409） |
| `legacy/readings_legacy_old.csv` 等 | 演示旧库迁移未校正 → 待核 → 原始 CSV 校正 |

演示脚本 `scripts/demo.sh` 的期望结果：MP-001 补录单据前 2026-01-09 为待核
（`unrecorded_meter_change`，`known_kwh=230`），补录后重算 `total_kwh=240`，
次日 `total_kwh=120`；MP-002 两段待核区间、`known_kwh=168`；
MP-003 待核区间 11:00–13:00、`known_kwh=385`。

`scripts/demo_precision.sh` 的期望结果：修复前预览 `total_kwh=0.0002`（22 个值
被 float 改变），修复后 `total_kwh=0.00024`；异值重传 `HTTP 409`；
旧库模拟先 `total_kwh=null` / `legacy_precision_unverified`，按原始 CSV 重导
（`legacy_corrected` 明细）后恢复 `total_kwh=0.00024`。

## 目录结构

```
app/
  main.py     # FastAPI 路由、导入事务、查询端点、/docs 演示端点
  calc.py     # 日核算核心（十进制精确增量、换表分段、待核区间合并）
  parsing.py  # CSV 解析与校验（Decimal 原文）
  db.py       # SQLite 连接、schema、原子迁移（WAL）
samples/      # 演示 CSV（含 precision/ 与 legacy/）
scripts/demo.sh / scripts/demo_precision.sh
tests/test_app.py / tests/test_precision_and_migration.py
Dockerfile / docker-compose.yml
```
