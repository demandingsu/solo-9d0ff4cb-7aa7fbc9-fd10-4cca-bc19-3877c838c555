"""日用电量核算核心逻辑（不依赖 FastAPI，可单测）。

规则：
- 结算日为 UTC 日界：某日用电量 = 相邻两个 UTC 午夜之间 24 个整点增量之和，
  需要 00:00 .. 次日 00:00 共 25 个整点读数。
- 换表整点 T：换表单给出旧表末读数与新表初值（边界读数），可代替该整点的常规读数；
  T-1→T 为旧表末段，T→T+1 为新表首段，分别计算后相加。
- 所有累计读数/边界读数按十进制原文（TEXT）读入 ``Decimal``，增量与日总量
  全程十进制精确相减/求和，不做浮点转换、不做定点量化。
- 以下情况不给出确定日总量，只返回待核区间：
  missing_hour               缺整点
  unrecorded_meter_change    无单据换表
  meter_regression           同表倒退
  boundary_conflict          边界读数冲突（常规读数与换表单边界读数不一致）
  legacy_precision_unverified 读数由旧库 REAL 迁移，原始小数不可知，
                              在按原始 CSV 重新导入校正前，相关增量一律待核
"""
from __future__ import annotations

import os
import socket
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from typing import Optional

HOUR = timedelta(hours=1)

REASON_MISSING = "missing_hour"
REASON_UNRECORDED = "unrecorded_meter_change"
REASON_REGRESSION = "meter_regression"
REASON_CONFLICT = "boundary_conflict"
REASON_LEGACY = "legacy_precision_unverified"

INSTANCE = os.environ.get("INSTANCE_NAME") or socket.gethostname()


def fmt_ts(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def _dec(value) -> Decimal:
    """已入库的十进制原文 -> Decimal。兼容迁移前的 REAL（仅旧库遗留调用）。"""
    if isinstance(value, Decimal):
        return value
    return Decimal(str(value))


def _out(d: Optional[Decimal]) -> Optional[Decimal]:
    """输出用的精确十进制（整数值补 ``.0``，保持 API 原有数值型语义）。"""
    if d is None:
        return None
    if d == 0:
        d = abs(d)  # 不输出 -0
    text = format(d, "f")
    if "." not in text:
        text += ".0"
    return Decimal(text)


def compute_day(conn, mp: str, day: date) -> dict:
    """在调用方持有的（只读）事务内核算 mp 在 day（UTC）的日用电量。"""
    t0 = datetime(day.year, day.month, day.day, tzinfo=timezone.utc)
    hours = [t0 + i * HOUR for i in range(25)]
    t1 = hours[24]

    # ts -> meter -> (Decimal 读数, 是否旧库未校正)
    csv_at: dict[str, dict[str, tuple[Decimal, bool]]] = {}
    for row in conn.execute(
        "SELECT meter, ts, cum_kwh, legacy_unverified FROM readings"
        " WHERE mp = ? AND ts BETWEEN ? AND ?",
        (mp, fmt_ts(t0), fmt_ts(t1)),
    ):
        csv_at.setdefault(row["ts"], {})[row["meter"]] = (
            _dec(row["cum_kwh"]), bool(row["legacy_unverified"]))

    forms: dict[str, dict] = {}
    for row in conn.execute(
        "SELECT * FROM meter_changes WHERE mp = ? AND change_ts BETWEEN ? AND ?",
        (mp, fmt_ts(t0), fmt_ts(t1)),
    ):
        forms[row["change_ts"]] = dict(row)

    # 每个整点的“左读数”（供 h→h+1 增量）与“右读数”（供 h-1→h 增量）。
    # 普通整点二者相同；换表整点右读数=旧表末读数、左读数=新表初值。
    Reading = tuple[str, Decimal, bool]  # (表号, 读数, 旧库未校正)
    left: list[Optional[Reading]] = [None] * 25
    right: list[Optional[Reading]] = [None] * 25
    missing = [False] * 25
    hour_problems: list[list[tuple[str, str]]] = [[] for _ in range(25)]

    for i, h in enumerate(hours):
        key = fmt_ts(h)
        form = forms.get(key)
        csvr = csv_at.get(key, {})
        if form is not None:
            om, nm = form["old_meter"], form["new_meter"]
            of, ni = _dec(form["old_final_kwh"]), _dec(form["new_initial_kwh"])
            of_legacy = bool(form["legacy_unverified"])
            ni_legacy = of_legacy  # 同一张单据的值一起迁移/校正
            right[i] = (om, of, of_legacy)
            left[i] = (nm, ni, ni_legacy)
            # 旧库迁移值未经原始单据校正时，其小数不可知：与常规读数的差异
            # 不能据此判为真实冲突（相关增量会以 legacy_precision_unverified 待核）。
            if om in csvr and csvr[om][0] != of and not (of_legacy or csvr[om][1]):
                hour_problems[i].append((
                    REASON_CONFLICT,
                    f"整点 {key} 表 {om} 常规读数 {csvr[om][0]} 与换表单旧表末读数 {of} 冲突",
                ))
            if nm in csvr and csvr[nm][0] != ni and not (ni_legacy or csvr[nm][1]):
                hour_problems[i].append((
                    REASON_CONFLICT,
                    f"整点 {key} 表 {nm} 常规读数 {csvr[nm][0]} 与换表单新表初值 {ni} 冲突",
                ))
            extra = sorted(set(csvr) - {om, nm})
            if extra:
                hour_problems[i].append((
                    REASON_CONFLICT,
                    f"整点 {key} 存在换表单之外的表计读数: {', '.join(extra)}",
                ))
        else:
            if not csvr:
                missing[i] = True
            elif len(csvr) == 1:
                m, (v, legacy) = next(iter(csvr.items()))
                left[i] = right[i] = (m, v, legacy)
            else:
                hour_problems[i].append((
                    REASON_UNRECORDED,
                    f"整点 {key} 同时存在多块表读数（{', '.join(sorted(csvr))}）且无换表单",
                ))

    increments = []
    for i in range(24):
        reasons: list[str] = []
        details: list[str] = []
        seen: set[tuple[str, str]] = set()

        def add(reason: str, detail: str) -> None:
            if (reason, detail) not in seen:
                seen.add((reason, detail))
                reasons.append(reason)
                details.append(detail)

        for j in (i, i + 1):
            for reason, detail in hour_problems[j]:
                add(reason, detail)
        if missing[i] or missing[i + 1]:
            which = [fmt_ts(hours[j]) for j in (i, i + 1) if missing[j]]
            add(REASON_MISSING, "缺整点读数: " + ", ".join(which))

        L, R = left[i], right[i + 1]
        meter = (L or R or (None, None, False))[0]
        delta: Optional[Decimal] = None
        if L is not None and R is not None:
            if L[0] != R[0]:
                if fmt_ts(hours[i]) in forms or fmt_ts(hours[i + 1]) in forms:
                    add(REASON_CONFLICT,
                        f"整点读数表计 {L[0]} → {R[0]} 与换表单登记的表号不一致")
                else:
                    add(REASON_UNRECORDED,
                        f"表计由 {L[0]} 切换为 {R[0]}，但无换表单")
            else:
                d = R[1] - L[1]
                if d < 0:
                    add(REASON_REGRESSION,
                        f"表 {L[0]} 累计读数倒退: {L[1]} → {R[1]}")
                elif L[2] or R[2]:
                    # 旧库 REAL 迁移值：原始小数不可知，差值不可作为确定电量。
                    legacy_hours = [fmt_ts(hours[j]) for j, r in ((i, L), (i + 1, R)) if r[2]]
                    add(REASON_LEGACY,
                        "旧库 REAL 迁移读数未按原始 CSV 校正，原始小数不可知: "
                        + ", ".join(legacy_hours))
                else:
                    delta = d

        increments.append({
            "start": fmt_ts(hours[i]),
            "end": fmt_ts(hours[i + 1]),
            "meter": meter,
            "delta_kwh": _out(delta),
            "status": "ok" if not reasons else "pending",
            "reasons": reasons,
            "detail": "；".join(details) if details else None,
        })

    # 相邻待核增量合并为待核区间
    pending_intervals: list[dict] = []
    for inc in increments:
        if inc["status"] != "pending":
            continue
        if pending_intervals and pending_intervals[-1]["end"] == inc["start"]:
            iv = pending_intervals[-1]
            iv["end"] = inc["end"]
            for r in inc["reasons"]:
                if r not in iv["reasons"]:
                    iv["reasons"].append(r)
            if inc["detail"]:
                for d in inc["detail"].split("；"):
                    if d not in iv["details"]:
                        iv["details"].append(d)
        else:
            pending_intervals.append({
                "start": inc["start"],
                "end": inc["end"],
                "reasons": list(inc["reasons"]),
                "details": [inc["detail"]] if inc["detail"] else [],
            })

    known = sum(
        (inc["delta_kwh"] for inc in increments
         if inc["status"] == "ok" and inc["delta_kwh"] is not None),
        Decimal("0"),
    )
    status = "pending" if pending_intervals else "ok"

    snapshot = {
        "max_import_id": conn.execute(
            "SELECT COALESCE(MAX(id), 0) AS m FROM imports").fetchone()["m"],
        "readings_in_window": sum(len(v) for v in csv_at.values()),
        "meter_changes_in_window": len(forms),
    }

    return {
        "mp": mp,
        "date": day.isoformat(),
        "status": status,
        # 有待核区间时不给确定日总量
        "total_kwh": None if pending_intervals else _out(known),
        "known_kwh": _out(known),
        "pending_intervals": pending_intervals,
        "increments": increments,
        "snapshot": snapshot,
        "instance": INSTANCE,
        "computed_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
    }
