"""园区能源日用电量核算 API。

- POST /api/imports/readings        导入读数 CSV（计量点,表号,UTC整点,累计kWh）
- POST /api/imports/meter-changes   导入换表单 CSV（可迟到，补录后重算受影响日期）
- GET  /api/settlement/daily        单计量点单日核算（待核区间 / 确定日总量）
- GET  /api/settlement/range        日期区间核算（同一已提交快照）
- GET  /api/metering-points|readings|meter-changes|imports  数据查看
- GET  /api/health                  健康检查

精度：累计读数与换表边界读数以十进制原文入库（TEXT），核算全程 Decimal 精确运算。
响应中的 decimal 字段以裸 JSON 数值输出（不经 float 往返、不做定点量化），
因此 1e12 量级下每小时 0.00001 kWh 的增量也能完整保留。

一致性：导入单事务原子提交（失败整体回滚，已有结果不变）；核算在只读事务内
完成，WAL 快照保证一次核算只读同一已提交状态。核算按需实时计算，补录换表单
后直接重新查询受影响日期即得重算结果。
"""
from __future__ import annotations

import json
import math
import sqlite3
from contextlib import asynccontextmanager
from datetime import date as date_type
from datetime import datetime, time, timedelta, timezone
from decimal import Decimal

from fastapi import FastAPI, File, HTTPException, Query, Request, UploadFile
from fastapi.responses import JSONResponse

from . import db
from .calc import HOUR, INSTANCE, _dec, compute_day, fmt_ts
from .parsing import (ChangeRow, CsvError, ReadingRow, canonical_kwh,
                      parse_meter_changes_csv, parse_readings_csv)


def _decimal_text(d: Decimal) -> str:
    if d == 0:
        d = abs(d)
    return format(d, "f")


def _dump_json(obj, out: list[str]) -> None:
    """把 Decimal 输出为裸 JSON 数值的递归序列化器。

    Decimal 直接展开成 ``format(d, 'f')`` 的十进制文本，不经过 float，
    也不做定点量化；JSON 数字语法本身允许任意精度。
    """
    if obj is None:
        out.append("null")
    elif obj is True:
        out.append("true")
    elif obj is False:
        out.append("false")
    elif isinstance(obj, Decimal):
        out.append(_decimal_text(obj))
    elif isinstance(obj, int):
        out.append(str(obj))
    elif isinstance(obj, float):
        if not math.isfinite(obj):
            raise ValueError("Out of range float values are not JSON compliant")
        out.append(json.dumps(obj))
    elif isinstance(obj, str):
        out.append(json.dumps(obj, ensure_ascii=False))
    elif isinstance(obj, (list, tuple)):
        out.append("[")
        for i, item in enumerate(obj):
            if i:
                out.append(",")
            _dump_json(item, out)
        out.append("]")
    elif isinstance(obj, dict):
        out.append("{")
        first = True
        for key, value in obj.items():
            if not first:
                out.append(",")
            first = False
            if not isinstance(key, str):
                key = str(key)
            out.append(json.dumps(key, ensure_ascii=False))
            out.append(":")
            _dump_json(value, out)
        out.append("}")
    elif isinstance(obj, date_type):
        out.append(json.dumps(obj.isoformat()))
    else:  # pragma: no cover - 防御性
        raise TypeError(f"Object of type {type(obj).__name__} is not JSON serializable")


def dumps_json(obj) -> bytes:
    out: list[str] = []
    _dump_json(obj, out)
    return "".join(out).encode("utf-8")


class DecimalJSONResponse(JSONResponse):
    """响应体由 dumps_json 生成：Decimal 保留十进制原文、不降级为 float。"""

    media_type = "application/json"

    def render(self, content) -> bytes:
        return dumps_json(content)


def _json(payload, status_code: int = 200) -> DecimalJSONResponse:
    return DecimalJSONResponse(content=payload, status_code=status_code)


@asynccontextmanager
async def lifespan(app: FastAPI):
    db.init_db()
    yield


app = FastAPI(
    title="园区能源日用电量核算 API",
    version="1.1.0",
    description=(
        "逐小时电表累计读数 → 计量点日用电量（UTC 日界）。支持换表单（含迟到补录）、"
        "缺整点/无单据换表/同表倒退/边界读数冲突时返回待核区间。"
        "累计读数按十进制原文入库与核算，1e-5 kWh 级增量不丢失；旧库 REAL 迁移值"
        "在按原始 CSV 重新导入校正前相关增量标记 legacy_precision_unverified 待核。\n\n"
        "**精度演示（/docs 直接操作）**：上传 `samples/precision/` 下的 CSV，"
        "再查询对应日期。`readings_tiny_increment.csv` 为合法大累计读数每小时仅增 "
        "0.00001 kWh；`readings_distinct_repr.csv` 含两个在旧版 float 下退化为同一值的"
        "不同读数（新版精确保留，重传不会静默跳过）。旧库迁移演示见 "
        "`scripts/demo_precision.sh` 与 `GET /api/demo/scenario`。"
        "样例数据见仓库 samples/，完整演示见 scripts/demo.sh。"
    ),
    lifespan=lifespan,
)


@app.exception_handler(sqlite3.OperationalError)
async def sqlite_busy_handler(request: Request, exc: sqlite3.OperationalError):
    # 两个实例并发写时的兜底：busy_timeout 内仍未拿到写锁
    return JSONResponse(
        status_code=503,
        content={"detail": "数据库忙，请重试", "error": str(exc)},
    )


# ---------------------------------------------------------------- 导入

def _affected_dates(ts_list: list[str]) -> list[str]:
    """整点读数/换表影响相邻两个增量，因此影响前一日与当日两个结算日。"""
    days: set[str] = set()
    for ts in ts_list:
        t = datetime.strptime(ts, "%Y-%m-%dT%H:%M:%SZ")
        days.add(t.date().isoformat())
        days.add((t - HOUR).date().isoformat())
    return sorted(days)


def _store_readings(rows: list[ReadingRow], source_name: str) -> dict:
    conn = db.connect()
    try:
        conn.execute("BEGIN IMMEDIATE")
        import_id = conn.execute(
            "INSERT INTO imports (kind, source_name, row_count) VALUES ('readings', ?, ?)",
            (source_name, len(rows)),
        ).lastrowid
        inserted = skipped = corrected = 0
        correction_examples: list[dict] = []
        for r in rows:
            existing = conn.execute(
                "SELECT cum_kwh, legacy_unverified FROM readings WHERE mp=? AND meter=? AND ts=?",
                (r.mp, r.meter, r.ts),
            ).fetchone()
            if existing is None:
                conn.execute(
                    "INSERT INTO readings (mp, meter, ts, cum_kwh, legacy_unverified, import_id)"
                    " VALUES (?,?,?,?,0,?)",
                    (r.mp, r.meter, r.ts, canonical_kwh(r.value), import_id),
                )
                inserted += 1
            elif existing["legacy_unverified"]:
                # 旧库 REAL 迁移值：原始小数不可知。重传原始读数是校正途径，
                # 用本次导入的十进制原文覆盖并解除待核；不得因数值“看起来相同”
                # 而静默跳过（旧值可能正是丢失精度后的结果）。
                conn.execute(
                    "UPDATE readings SET cum_kwh=?, legacy_unverified=0, import_id=?"
                    " WHERE mp=? AND meter=? AND ts=?",
                    (canonical_kwh(r.value), import_id, r.mp, r.meter, r.ts),
                )
                corrected += 1
                if len(correction_examples) < 20:
                    correction_examples.append({
                        "mp": r.mp, "meter": r.meter, "ts": r.ts,
                        "legacy_value": existing["cum_kwh"],
                        "restored_value": canonical_kwh(r.value),
                        "changed": Decimal(existing["cum_kwh"]) != r.value,
                    })
            elif Decimal(existing["cum_kwh"]) == r.value:
                skipped += 1  # 幂等重传（十进制精确相等，而非 float 相等）
            else:
                raise HTTPException(
                    status_code=409,
                    detail=(f"读数冲突: {r.mp}/{r.meter}/{r.ts} 已存在 "
                            f"{existing['cum_kwh']}，新值 {canonical_kwh(r.value)}；整批已回滚"),
                )
        conn.execute("COMMIT")
    except Exception:
        try:
            conn.execute("ROLLBACK")
        except sqlite3.Error:
            pass
        raise
    finally:
        conn.close()

    by_mp: dict[str, list[str]] = {}
    for r in rows:
        by_mp.setdefault(r.mp, []).append(r.ts)
    return {
        "import_id": import_id,
        "kind": "readings",
        "status": "committed",
        "source_name": source_name,
        "inserted": inserted,
        "duplicates_skipped": skipped,
        "legacy_corrected": corrected,
        "legacy_corrections": correction_examples,
        "affected_dates": {mp: _affected_dates(ts) for mp, ts in by_mp.items()},
    }


def _store_meter_changes(rows: list[ChangeRow], source_name: str) -> dict:
    conn = db.connect()
    try:
        conn.execute("BEGIN IMMEDIATE")
        import_id = conn.execute(
            "INSERT INTO imports (kind, source_name, row_count) VALUES ('meter_changes', ?, ?)",
            (source_name, len(rows)),
        ).lastrowid
        inserted = skipped = corrected = 0
        correction_examples: list[dict] = []
        for r in rows:
            existing = conn.execute(
                "SELECT * FROM meter_changes WHERE mp=? AND change_ts=?",
                (r.mp, r.change_ts),
            ).fetchone()
            if existing is None:
                conn.execute(
                    "INSERT INTO meter_changes (mp, change_ts, old_meter, new_meter,"
                    " old_final_kwh, new_initial_kwh, legacy_unverified, import_id)"
                    " VALUES (?,?,?,?,?,?,0,?)",
                    (r.mp, r.change_ts, r.old_meter, r.new_meter,
                     canonical_kwh(r.old_final), canonical_kwh(r.new_initial), import_id),
                )
                inserted += 1
            elif existing["legacy_unverified"]:
                # 迟到的原始换表单可校正旧库迁移值；整条单据一起恢复确定核算。
                conn.execute(
                    "UPDATE meter_changes SET old_meter=?, new_meter=?,"
                    " old_final_kwh=?, new_initial_kwh=?, legacy_unverified=0, import_id=?"
                    " WHERE mp=? AND change_ts=?",
                    (r.old_meter, r.new_meter, canonical_kwh(r.old_final),
                     canonical_kwh(r.new_initial), import_id, r.mp, r.change_ts),
                )
                corrected += 1
                if len(correction_examples) < 20:
                    correction_examples.append({
                        "mp": r.mp, "change_ts": r.change_ts,
                        "legacy_old_final_kwh": existing["old_final_kwh"],
                        "legacy_new_initial_kwh": existing["new_initial_kwh"],
                        "restored_old_final_kwh": canonical_kwh(r.old_final),
                        "restored_new_initial_kwh": canonical_kwh(r.new_initial),
                        "changed": (
                            Decimal(existing["old_final_kwh"]) != r.old_final
                            or Decimal(existing["new_initial_kwh"]) != r.new_initial
                            or existing["old_meter"] != r.old_meter
                            or existing["new_meter"] != r.new_meter
                        ),
                    })
            elif (existing["old_meter"] == r.old_meter and existing["new_meter"] == r.new_meter
                  and Decimal(existing["old_final_kwh"]) == r.old_final
                  and Decimal(existing["new_initial_kwh"]) == r.new_initial):
                skipped += 1  # 幂等重传（十进制精确相等）
            else:
                raise HTTPException(
                    status_code=409,
                    detail=(f"换表单冲突: {r.mp}@{r.change_ts} 已存在不同单据；整批已回滚"),
                )
        conn.execute("COMMIT")
    except Exception:
        try:
            conn.execute("ROLLBACK")
        except sqlite3.Error:
            pass
        raise
    finally:
        conn.close()

    by_mp: dict[str, list[str]] = {}
    for r in rows:
        by_mp.setdefault(r.mp, []).append(r.change_ts)
    return {
        "import_id": import_id,
        "kind": "meter_changes",
        "status": "committed",
        "source_name": source_name,
        "inserted": inserted,
        "duplicates_skipped": skipped,
        "legacy_corrected": corrected,
        "legacy_corrections": correction_examples,
        # 补录单据后需要重算的日期
        "affected_dates": {mp: _affected_dates(ts) for mp, ts in by_mp.items()},
    }


@app.post("/api/imports/readings", summary="导入读数 CSV")
def import_readings(file: UploadFile = File(..., description="列: mp,meter,ts,cum_kwh")):
    try:
        rows = parse_readings_csv(file.file.read())
    except CsvError as e:
        raise HTTPException(status_code=422, detail={"msg": "CSV 校验失败，整批未导入", "errors": e.errors})
    if not rows:
        raise HTTPException(status_code=422, detail="CSV 没有有效数据行")
    return _json(_store_readings(rows, file.filename or ""))


@app.post("/api/imports/meter-changes", summary="导入换表单 CSV（支持迟到补录）")
def import_meter_changes(file: UploadFile = File(..., description="列: mp,change_ts,old_meter,new_meter,old_final_kwh,new_initial_kwh")):
    try:
        rows = parse_meter_changes_csv(file.file.read())
    except CsvError as e:
        raise HTTPException(status_code=422, detail={"msg": "CSV 校验失败，整批未导入", "errors": e.errors})
    if not rows:
        raise HTTPException(status_code=422, detail="CSV 没有有效数据行")
    return _json(_store_meter_changes(rows, file.filename or ""))


# ---------------------------------------------------------------- 精度演示
#
# 仅用于在 /docs 复现“修复前”的行为与迁移后的校正流程：
# - pre-fix-preview：把上传的 CSV 按修复前的 REAL(float) 存储/读回方式在内存库中
#   核算，返回同一批数据在旧版下的结果，不写正式库。
# - legacy-seed：把上传的 CSV 模拟成“旧库迁移后、尚未校正”的状态（legacy=1）
#   写入正式库；mp 必须使用 DEMO- 前缀，避免误碰真实计量点。
# 校正途径与生产一致：把原始 CSV 重新导入 /api/imports/* 即可解除待核。

DEMO_PREFIX = "DEMO-"


def _legacy_storage_value(v: Decimal) -> str:
    """修复前的持久化效果：写入 REAL，再按 Decimal(str(float)) 读回。"""
    return canonical_kwh(Decimal(str(float(v))))


def _require_demo_confirm(confirm: bool) -> None:
    if not confirm:
        raise HTTPException(status_code=422, detail="演示写入必须显式传 confirm=true")


def _require_demo_mp(mp: str) -> None:
    if not mp.startswith(DEMO_PREFIX):
        raise HTTPException(
            status_code=422,
            detail=f"演示数据只允许使用 {DEMO_PREFIX} 前缀的计量点，收到 {mp!r}",
        )


Q_PRE_FIX = Decimal("0.000001")


def _as_pre_fix_result(result: dict) -> dict:
    """把精确核算结果改写成修复前的输出方式：每个增量量化到 1e-6、float 读回。"""
    increments = []
    for inc in result["increments"]:
        item = dict(inc)
        d = item["delta_kwh"]
        if d is not None:
            item["delta_kwh"] = Decimal(str(float(d.quantize(Q_PRE_FIX))))
        increments.append(item)
    known = sum((i["delta_kwh"] for i in increments
                 if i["status"] == "ok" and i["delta_kwh"] is not None), Decimal("0"))
    total = None if result["pending_intervals"] else Decimal(str(float(known.quantize(Q_PRE_FIX))))
    return {
        "mp": result["mp"], "date": result["date"], "status": result["status"],
        "total_kwh": total, "known_kwh": Decimal(str(float(known.quantize(Q_PRE_FIX)))),
        "pending_intervals": result["pending_intervals"],
        "increments": increments,
    }


@app.post("/api/demo/pre-fix-preview", summary="演示：修复前（REAL/float）核算结果")
def pre_fix_preview(
    file: UploadFile = File(..., description="读数 CSV（同 /api/imports/readings）"),
    changes: UploadFile | None = File(None, description="可选换表单 CSV"),
):
    """在内存库中按修复前的 REAL 存储与浮点读回方式重算，不触碰正式库。

    用同一批 CSV 对比本接口与正常导入后的 ``/api/settlement/daily``，
    即可看到旧版对大累计读数 1e-5 级增量的低估/并值。
    """
    try:
        rows = parse_readings_csv(file.file.read())
        change_rows = (parse_meter_changes_csv(changes.file.read())
                       if changes is not None else [])
    except CsvError as e:
        raise HTTPException(status_code=422, detail={"msg": "CSV 校验失败", "errors": e.errors})
    if not rows:
        raise HTTPException(status_code=422, detail="CSV 没有有效数据行")

    # 逐值统计 float 往返造成的精度丢失（这是修复前的写入行为）
    lost: list[dict] = []
    for r in rows:
        stored = _legacy_storage_value(r.value)
        if Decimal(stored) != r.value:
            lost.append({"mp": r.mp, "ts": r.ts,
                         "input": canonical_kwh(r.value), "stored_before_fix": stored})
    for c in change_rows:
        for field, v in (("old_final_kwh", c.old_final), ("new_initial_kwh", c.new_initial)):
            stored = _legacy_storage_value(v)
            if Decimal(stored) != v:
                lost.append({"mp": c.mp, "ts": c.change_ts, "field": field,
                             "input": canonical_kwh(v), "stored_before_fix": stored})

    conn = sqlite3.connect(":memory:", isolation_level=None)
    conn.row_factory = sqlite3.Row
    try:
        db.ensure_schema(conn)  # 全新内存库，建当前 schema（值仍按旧版方式写入）
        conn.execute("BEGIN")
        imp = conn.execute(
            "INSERT INTO imports (kind, source_name, row_count) VALUES ('readings', 'pre-fix-preview', ?)",
            (len(rows),)).lastrowid
        for r in rows:
            conn.execute(
                "INSERT INTO readings (mp, meter, ts, cum_kwh, legacy_unverified, import_id)"
                " VALUES (?,?,?,?,0,?)",
                (r.mp, r.meter, r.ts, _legacy_storage_value(r.value), imp))
        if change_rows:
            cimp = conn.execute(
                "INSERT INTO imports (kind, source_name, row_count)"
                " VALUES ('meter_changes', 'pre-fix-preview', ?)", (len(change_rows),)).lastrowid
            for c in change_rows:
                conn.execute(
                    "INSERT INTO meter_changes (mp, change_ts, old_meter, new_meter,"
                    " old_final_kwh, new_initial_kwh, legacy_unverified, import_id)"
                    " VALUES (?,?,?,?,?,?,0,?)",
                    (c.mp, c.change_ts, c.old_meter, c.new_meter,
                     _legacy_storage_value(c.old_final),
                     _legacy_storage_value(c.new_initial), cimp))
        conn.execute("COMMIT")

        mp_tss: dict[str, list[str]] = {}
        for r in rows:
            mp_tss.setdefault(r.mp, []).append(r.ts)
        for c in change_rows:
            mp_tss.setdefault(c.mp, []).append(c.change_ts)
        pairs: set[tuple[str, str]] = set()
        for mp, tss in mp_tss.items():
            for ts in tss:
                t = datetime.strptime(ts, "%Y-%m-%dT%H:%M:%SZ")
                pairs.add((mp, t.date().isoformat()))
                pairs.add((mp, (t - HOUR).date().isoformat()))
        if len(pairs) > 63:
            raise HTTPException(status_code=422, detail="演示预览最多覆盖 63 个（计量点, 日期）组合")

        days = []
        for mp, ds in sorted(pairs):
            day = datetime.strptime(ds, "%Y-%m-%d").date()
            days.append(_as_pre_fix_result(compute_day(conn, mp, day)))
        return _json({
            "mode": "pre_fix_preview",
            "note": ("模拟修复前 REAL(float) 存储与读回；若此结果与正常导入后的核算不同，"
                     "差异即修复前会丢失的精度。本接口不写正式库。"),
            "storage_before_fix": {
                "rows": len(rows) + len(change_rows),
                "values_changed_by_float": len(lost),
                "examples": lost[:20],
            },
            "days": days,
        })
    finally:
        conn.close()


def _demo_insert(conn, kind: str, values, source: str):
    return conn.execute(
        "INSERT INTO imports (kind, source_name, row_count) VALUES (?, ?, ?)",
        (kind, source, len(values)),
    ).lastrowid


@app.post("/api/demo/legacy-seed/readings", summary="演示：模拟旧库迁移后的未校正读数")
def demo_legacy_seed_readings(
    file: UploadFile = File(..., description="读数 CSV（mp 须以 DEMO- 开头）"),
    confirm: bool = Query(False, description="演示写入需显式 confirm=true"),
):
    _require_demo_confirm(confirm)
    try:
        rows = parse_readings_csv(file.file.read())
    except CsvError as e:
        raise HTTPException(status_code=422, detail={"msg": "CSV 校验失败", "errors": e.errors})
    for r in rows:
        _require_demo_mp(r.mp)
    if not rows:
        raise HTTPException(status_code=422, detail="CSV 没有有效数据行")

    conn = db.connect()
    try:
        conn.execute("BEGIN IMMEDIATE")
        for r in rows:
            existing = conn.execute(
                "SELECT legacy_unverified FROM readings WHERE mp=? AND meter=? AND ts=?",
                (r.mp, r.meter, r.ts)).fetchone()
            if existing is not None and not existing["legacy_unverified"]:
                raise HTTPException(
                    status_code=409,
                    detail=f"{r.mp}/{r.meter}/{r.ts} 已是正式导入数据，拒绝用演示数据覆盖",
                )
        import_id = _demo_insert(conn, "readings", rows, "demo-legacy-seed")
        for r in rows:
            conn.execute(
                "INSERT OR REPLACE INTO readings"
                " (mp, meter, ts, cum_kwh, legacy_unverified, import_id)"
                " VALUES (?,?,?,?,1,?)",
                (r.mp, r.meter, r.ts, _legacy_storage_value(r.value), import_id))
        conn.execute("COMMIT")
    except Exception:
        try:
            conn.execute("ROLLBACK")
        except sqlite3.Error:
            pass
        raise
    finally:
        conn.close()
    return {
        "status": "seeded",
        "mode": "legacy_migrated_simulation",
        "inserted": len(rows),
        "affected_dates": {mp: _affected_dates(tss)
                           for mp, tss in _group_ts(rows).items()},
        "next": "查询 /api/settlement/daily 应出现 legacy_precision_unverified 待核；"
                "随后把同一原始 CSV 导入 /api/imports/readings 即可校正。",
    }


@app.post("/api/demo/legacy-seed/meter-changes", summary="演示：模拟旧库迁移后的未校正换表单")
def demo_legacy_seed_changes(
    file: UploadFile = File(..., description="换表单 CSV（mp 须以 DEMO- 开头）"),
    confirm: bool = Query(False, description="演示写入需显式 confirm=true"),
):
    _require_demo_confirm(confirm)
    try:
        rows = parse_meter_changes_csv(file.file.read())
    except CsvError as e:
        raise HTTPException(status_code=422, detail={"msg": "CSV 校验失败", "errors": e.errors})
    for r in rows:
        _require_demo_mp(r.mp)
    if not rows:
        raise HTTPException(status_code=422, detail="CSV 没有有效数据行")

    conn = db.connect()
    try:
        conn.execute("BEGIN IMMEDIATE")
        for r in rows:
            existing = conn.execute(
                "SELECT legacy_unverified FROM meter_changes WHERE mp=? AND change_ts=?",
                (r.mp, r.change_ts)).fetchone()
            if existing is not None and not existing["legacy_unverified"]:
                raise HTTPException(
                    status_code=409,
                    detail=f"{r.mp}@{r.change_ts} 已是正式导入单据，拒绝用演示数据覆盖",
                )
        import_id = _demo_insert(
            conn, "meter_changes", rows, "demo-legacy-seed")
        for r in rows:
            conn.execute(
                "INSERT OR REPLACE INTO meter_changes"
                " (mp, change_ts, old_meter, new_meter, old_final_kwh, new_initial_kwh,"
                " legacy_unverified, import_id) VALUES (?,?,?,?,?,?,1,?)",
                (r.mp, r.change_ts, r.old_meter, r.new_meter,
                 _legacy_storage_value(r.old_final),
                 _legacy_storage_value(r.new_initial), import_id))
        conn.execute("COMMIT")
    except Exception:
        try:
            conn.execute("ROLLBACK")
        except sqlite3.Error:
            pass
        raise
    finally:
        conn.close()
    return {
        "status": "seeded",
        "mode": "legacy_migrated_simulation",
        "inserted": len(rows),
        "next": "查询 /api/settlement/daily 应出现 legacy_precision_unverified 待核；"
                "随后把同一原始换表单导入 /api/imports/meter-changes 即可校正。",
    }


def _group_ts(rows) -> dict[str, list[str]]:
    out: dict[str, list[str]] = {}
    for r in rows:
        out.setdefault(r.mp, []).append(getattr(r, "ts", None) or r.change_ts)
    return out


DEMO_DAY = "2026-04-01"


@app.get("/api/demo/scenario", summary="演示：修复前触发数据 + 迁移模拟 + 校正")
def demo_scenario(
    confirm: bool = Query(False, description="写入 DEMO-PREC 数据需显式 confirm=true"),
):
    """一键复现精度问题与修复效果（供 /docs 演示）。

    返回三部分：
    1. ``pre_fix``：同一批 CSV 在修复前（REAL/float + 1e-6 量化）下的每日结果；
    2. ``fixed``：修复后同数据的精确结果（1e-5 kWh/小时不再被低估，异值不再并值）；
    3. ``legacy``：旧库迁移未校正 -> 待核 -> 按原始 CSV 重新导入 -> 恢复确定核算
       的完整时间线（仅当 ``confirm=true`` 时把 DEMO-PREC 数据写入正式库）。
    """
    base = Decimal("999999999999.99000")
    step = Decimal("0.00001")
    values = [base + Decimal(i) * step for i in range(25)]
    t0 = datetime.strptime(DEMO_DAY, "%Y-%m-%d").replace(tzinfo=timezone.utc)

    def ts_text(i: int) -> str:
        return fmt_ts(t0 + i * HOUR)

    # 1) 修复前：float 存储 + 量化输出
    lost = 0
    for v in values:
        if Decimal(_legacy_storage_value(v)) != v:
            lost += 1
    pre_fix_total = None
    conn = sqlite3.connect(":memory:", isolation_level=None)
    conn.row_factory = sqlite3.Row
    try:
        db.ensure_schema(conn)
        conn.execute("BEGIN")
        imp = conn.execute(
            "INSERT INTO imports (kind, source_name, row_count)"
            " VALUES ('readings', 'scenario-prefix', ?)", (len(values),)).lastrowid
        for i, v in enumerate(values):
            conn.execute(
                "INSERT INTO readings (mp, meter, ts, cum_kwh, legacy_unverified, import_id)"
                " VALUES (?,?,?,?,0,?)",
                ("DEMO-PREC", "M600", ts_text(i), _legacy_storage_value(v), imp))
        conn.execute("COMMIT")
        pre_fix_day = _as_pre_fix_result(compute_day(conn, "DEMO-PREC", t0.date()))
        pre_fix_total = pre_fix_day["total_kwh"]
    finally:
        conn.close()

    # 2) 修复后：直接用解析后的 Decimal 值计算（与正式导入路径一致）
    fixed_total = values[24] - values[0]
    fixed_day = {
        "mp": "DEMO-PREC", "date": DEMO_DAY, "status": "ok",
        "total_kwh": fixed_total, "known_kwh": fixed_total,
        "increment_kwh": step,
        "increments": 24,
    }

    result = {
        "mp": "DEMO-PREC",
        "day": DEMO_DAY,
        "csv_preview": (
            "mp,meter,ts,cum_kwh\n"
            "DEMO-PREC,M600,2026-04-01T00:00:00Z,999999999999.99000\n"
            "...（每小时 +0.00001）...\n"
            "DEMO-PREC,M600,2026-04-02T00:00:00Z,999999999999.99024\n"
        ),
        "pre_fix": {
            "storage": "REAL(float) + 输出量化到 1e-6",
            "values_changed_by_float": lost,
            "total_kwh": pre_fix_total,
            "note": "24 小时 × 0.00001 = 0.00024，但修复前只得到 0.0002；"
                    "且 04-01 的 .99000/.99001/.99002 等不同读数在 float 下变成同一存储值，"
                    "重传异值会被误判为幂等重复。",
        },
        "fixed": fixed_day,
        "legacy_timeline": {
            "steps": [
                "旧库 REAL 迁移 -> 相关增量 reasons=[legacy_precision_unverified]，"
                "total_kwh=null（不给确定日总量）",
                "按原始 CSV 重新导入 /api/imports/readings -> 响应 legacy_corrected=25，"
                "旧值逐条记录 legacy_corrections（changed 标明是否真的变了）",
                f"再次核算 {DEMO_DAY} -> status=ok，total_kwh=0.00024",
            ],
            "demo_samples": [
                "samples/legacy/readings_legacy_old.csv",
                "samples/legacy/readings_legacy_new.csv",
                "samples/legacy/meter_change_legacy.csv",
            ],
        },
    }

    if confirm:
        conn = db.connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            existing = conn.execute(
                "SELECT COUNT(*) AS c FROM readings WHERE mp='DEMO-PREC'"
                " AND legacy_unverified=0").fetchone()["c"]
            if existing:
                raise HTTPException(status_code=409,
                                    detail="DEMO-PREC 已有正式数据；先调用 /api/demo/reset 清理")
            imp = conn.execute(
                "INSERT INTO imports (kind, source_name, row_count)"
                " VALUES ('readings', 'demo-scenario', ?)", (len(values),)).lastrowid
            for i, v in enumerate(values):
                conn.execute(
                    "INSERT OR REPLACE INTO readings"
                    " (mp, meter, ts, cum_kwh, legacy_unverified, import_id)"
                    " VALUES (?,?,?,?,0,?)",
                    ("DEMO-PREC", "M600", ts_text(i), format(v, "f"), imp))
            conn.execute("COMMIT")
        except Exception:
            try:
                conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            raise
        finally:
            conn.close()
        result["seeded"] = "DEMO-PREC 已按修复后方式写入；查询 "
        result["seeded"] += "/api/settlement/daily?mp=DEMO-PREC&date=2026-04-01"

    return _json(result)


@app.post("/api/demo/reset", summary="演示：清理 DEMO- 计量点的数据")
def demo_reset(
    mp: str = Query(..., min_length=1, max_length=64, description="DEMO- 前缀计量点"),
    confirm: bool = Query(False, description="需显式 confirm=true"),
):
    _require_demo_confirm(confirm)
    _require_demo_mp(mp)
    conn = db.connect()
    try:
        conn.execute("BEGIN IMMEDIATE")
        r1 = conn.execute("DELETE FROM readings WHERE mp=?", (mp,)).rowcount
        r2 = conn.execute("DELETE FROM meter_changes WHERE mp=?", (mp,)).rowcount
        conn.execute("COMMIT")
    except Exception:
        try:
            conn.execute("ROLLBACK")
        except sqlite3.Error:
            pass
        raise
    finally:
        conn.close()
    return {"status": "reset", "mp": mp,
            "deleted_readings": r1, "deleted_meter_changes": r2}


# ---------------------------------------------------------------- 核算

@app.get("/api/settlement/daily", summary="单日核算")
def settlement_daily(
    mp: str = Query(..., min_length=1, max_length=64, description="计量点"),
    date: date_type = Query(..., description="结算日期 (UTC)，如 2026-01-09"),
):
    conn = db.connect()
    try:
        conn.execute("BEGIN")  # 只读事务：WAL 快照，一次核算只读同一已提交状态
        try:
            result = compute_day(conn, mp, date)
        finally:
            conn.execute("COMMIT")
        return _json(result)
    finally:
        conn.close()


@app.get("/api/settlement/range", summary="日期区间核算（同一快照）")
def settlement_range(
    mp: str = Query(..., min_length=1, max_length=64),
    from_: date_type = Query(..., alias="from"),
    to: date_type = Query(...),
):
    if to < from_:
        raise HTTPException(status_code=422, detail="to 不能早于 from")
    if (to - from_).days > 62:
        raise HTTPException(status_code=422, detail="区间最长 63 天")
    conn = db.connect()
    try:
        conn.execute("BEGIN")
        try:
            days = []
            d = from_
            while d <= to:
                days.append(compute_day(conn, mp, d))
                d += timedelta(days=1)
        finally:
            conn.execute("COMMIT")
        return _json({"mp": mp, "days": days, "instance": INSTANCE})
    finally:
        conn.close()


# ---------------------------------------------------------------- 数据查看

@app.get("/api/health")
def health():
    conn = db.connect()
    try:
        conn.execute("SELECT 1")
    finally:
        conn.close()
    return {"status": "ok", "instance": INSTANCE}


@app.get("/api/metering-points", summary="计量点列表")
def metering_points():
    conn = db.connect()
    try:
        conn.execute("BEGIN")
        rows = conn.execute(
            "SELECT mp, COUNT(*) AS reading_count, MIN(ts) AS first_ts, MAX(ts) AS last_ts"
            " FROM readings GROUP BY mp ORDER BY mp").fetchall()
        forms = conn.execute(
            "SELECT mp, COUNT(*) AS c FROM meter_changes GROUP BY mp").fetchall()
        conn.execute("COMMIT")
    finally:
        conn.close()
    fc = {f["mp"]: f["c"] for f in forms}
    mps = {r["mp"] for r in rows} | set(fc)
    return [{
        "mp": mp,
        "reading_count": next((r["reading_count"] for r in rows if r["mp"] == mp), 0),
        "first_ts": next((r["first_ts"] for r in rows if r["mp"] == mp), None),
        "last_ts": next((r["last_ts"] for r in rows if r["mp"] == mp), None),
        "meter_change_count": fc.get(mp, 0),
    } for mp in sorted(mps)]


@app.get("/api/readings", summary="读数查看")
def list_readings(
    mp: str = Query(..., min_length=1, max_length=64),
    from_: date_type = Query(None, alias="from"),
    to: date_type = Query(None),
):
    lo = fmt_ts(datetime.combine(from_, time.min)) if from_ else "0000"
    hi = fmt_ts(datetime.combine(to + timedelta(days=1), time.min)) if to else "9999"
    conn = db.connect()
    try:
        conn.execute("BEGIN")
        rows = conn.execute(
            "SELECT mp, meter, ts, cum_kwh, legacy_unverified, import_id FROM readings"
            " WHERE mp=? AND ts BETWEEN ? AND ? ORDER BY ts, meter LIMIT 5000",
            (mp, lo, hi)).fetchall()
        conn.execute("COMMIT")
    finally:
        conn.close()
    out = []
    for r in rows:
        item = dict(r)
        item["cum_kwh"] = _dec(item["cum_kwh"])
        item["legacy_unverified"] = bool(item["legacy_unverified"])
        out.append(item)
    return _json(out)


@app.get("/api/meter-changes", summary="换表单查看")
def list_meter_changes(mp: str = Query(None, min_length=1, max_length=64)):
    conn = db.connect()
    try:
        conn.execute("BEGIN")
        if mp:
            rows = conn.execute(
                "SELECT * FROM meter_changes WHERE mp=? ORDER BY change_ts", (mp,)).fetchall()
        else:
            rows = conn.execute("SELECT * FROM meter_changes ORDER BY mp, change_ts").fetchall()
        conn.execute("COMMIT")
    finally:
        conn.close()
    out = []
    for r in rows:
        item = dict(r)
        item["old_final_kwh"] = _dec(item["old_final_kwh"])
        item["new_initial_kwh"] = _dec(item["new_initial_kwh"])
        item["legacy_unverified"] = bool(item["legacy_unverified"])
        out.append(item)
    return _json(out)


@app.get("/api/imports", summary="导入批次列表")
def list_imports():
    conn = db.connect()
    try:
        conn.execute("BEGIN")
        rows = conn.execute(
            "SELECT * FROM imports ORDER BY id DESC LIMIT 100").fetchall()
        conn.execute("COMMIT")
    finally:
        conn.close()
    return [dict(r) for r in rows]


@app.get("/", include_in_schema=False)
def root():
    return {
        "service": "园区能源日用电量核算 API",
        "docs": "/docs",
        "endpoints": ["/api/imports/readings", "/api/imports/meter-changes",
                      "/api/settlement/daily", "/api/settlement/range",
                      "/api/metering-points", "/api/readings", "/api/meter-changes",
                      "/api/imports", "/api/health",
                      "/api/demo/scenario", "/api/demo/pre-fix-preview",
                      "/api/demo/legacy-seed/readings",
                      "/api/demo/legacy-seed/meter-changes", "/api/demo/reset"],
    }
