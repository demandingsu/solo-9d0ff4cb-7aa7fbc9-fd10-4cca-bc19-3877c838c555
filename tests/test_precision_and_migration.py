"""精度回归：十进制原文入库/核算、旧库迁移原子性、双实例交错启动。

这些用例直接构造旧版（REAL 列）SQLite 库，模拟升级前的数据与并发布局。
"""
from __future__ import annotations

import json
import sqlite3
import threading
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest
from fastapi.testclient import TestClient

from app import db, main

T0 = datetime(2026, 4, 1, tzinfo=timezone.utc)
BASE = Decimal("999999999999.99000")
STEP = Decimal("0.00001")


def ts(i: int, start: datetime = T0) -> str:
    return (start + timedelta(hours=i)).strftime("%Y-%m-%dT%H:%M:%SZ")


def readings_csv(mp: str, meter: str, values, start: datetime = T0) -> str:
    lines = ["mp,meter,ts,cum_kwh"]
    lines += [f"{mp},{meter},{ts(i, start)},{format(v, 'f')}" for i, v in enumerate(values)]
    return "\n".join(lines) + "\n"


def tiny_values(n: int = 25, base: Decimal = BASE, step: Decimal = STEP):
    return [base + Decimal(i) * step for i in range(n)]


def post_csv(client, url, content: str, name="data.csv"):
    return client.post(url, files={"file": (name, content.encode(), "text/csv")})


def json_decimal(response):
    """按十进制解析响应，避免测试自身用 float 解析 JSON 数字而丢精度。"""
    return json.loads(response.text, parse_float=Decimal)


@pytest.fixture()
def client(tmp_path, monkeypatch):
    monkeypatch.setenv("DB_PATH", str(tmp_path / "test.db"))
    with TestClient(main.app) as c:
        yield c


def make_legacy_db(path: str, readings, changes=(), meter_changes=()):
    """按修复前 schema（REAL 列）建库并写入 float 化的值。"""
    conn = sqlite3.connect(path, isolation_level=None)
    conn.executescript(
        """
        CREATE TABLE imports (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            kind TEXT NOT NULL,
            source_name TEXT NOT NULL DEFAULT '',
            row_count INTEGER NOT NULL,
            created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')));
        CREATE TABLE readings (
            mp TEXT NOT NULL, meter TEXT NOT NULL, ts TEXT NOT NULL,
            cum_kwh REAL NOT NULL, import_id INTEGER NOT NULL,
            PRIMARY KEY (mp, meter, ts));
        CREATE INDEX idx_readings_mp_ts ON readings (mp, ts);
        CREATE TABLE meter_changes (
            mp TEXT NOT NULL, change_ts TEXT NOT NULL,
            old_meter TEXT NOT NULL, new_meter TEXT NOT NULL,
            old_final_kwh REAL NOT NULL, new_initial_kwh REAL NOT NULL,
            import_id INTEGER NOT NULL, PRIMARY KEY (mp, change_ts));
        CREATE INDEX idx_meter_changes_mp_ts ON meter_changes (mp, change_ts);
        """
    )
    conn.execute("INSERT INTO imports (kind, source_name, row_count) VALUES ('readings','old',?)",
                 (len(readings),))
    for mp, meter, t, v in readings:
        conn.execute("INSERT INTO readings (mp,meter,ts,cum_kwh,import_id) VALUES (?,?,?,?,1)",
                     (mp, meter, t, float(v)))
    if meter_changes:
        conn.execute("INSERT INTO imports (kind, source_name, row_count)"
                     " VALUES ('meter_changes','old',?)", (len(meter_changes),))
        for mp, t, om, nm, of, ni in meter_changes:
            conn.execute("INSERT INTO meter_changes VALUES (?,?,?,?,?,?,2)",
                         (mp, t, om, nm, float(of), float(ni)))
    conn.close()


# ---------------------------------------------------------------- 精度

def test_tiny_hourly_increment_not_underestimated(client):
    """合法大累计读数每小时 +0.00001 kWh：日总量必须精确，不能被实数吞掉。"""
    values = tiny_values()
    assert post_csv(client, "/api/imports/readings",
                    readings_csv("MP-PREC", "M600", values)).status_code == 200

    r = client.get("/api/settlement/daily", params={"mp": "MP-PREC", "date": "2026-04-01"})
    body = json_decimal(r)
    assert r.status_code == 200
    assert body["status"] == "ok"
    # 精确值 0.00024，修复前为 0.0002
    assert Decimal(str(body["total_kwh"])) == Decimal("0.00024")
    assert Decimal(str(body["known_kwh"])) == Decimal("0.00024")
    deltas = [Decimal(str(i["delta_kwh"])) for i in body["increments"]]
    assert deltas == [STEP] * 24


def test_tiny_increment_raw_json_keeps_decimal_text(client):
    values = tiny_values()
    post_csv(client, "/api/imports/readings", readings_csv("MP-PREC", "M600", values))
    raw = client.get("/api/settlement/daily",
                     params={"mp": "MP-PREC", "date": "2026-04-01"}).text
    assert '"total_kwh":0.00024' in raw
    assert "0.0002}" not in raw


def test_readings_roundtrip_preserves_decimal_text(client):
    values = tiny_values(3)
    post_csv(client, "/api/imports/readings", readings_csv("MP-PREC", "M600", values))
    rows = json_decimal(client.get(
        "/api/readings", params={"mp": "MP-PREC", "from": "2026-04-01", "to": "2026-04-01"}))
    assert [r["cum_kwh"] for r in rows] == values
    # 原文的小数位也保留（不做 float 往返）
    assert [format(r["cum_kwh"], "f") for r in rows] == [format(v, "f") for v in values]


def test_distinct_values_with_same_float_repr_are_not_duplicates(client):
    """两个不同输入值在旧版 float 下 repr 相同；重传异值必须冲突，不得静默跳过。"""
    values = [BASE + Decimal(i) * STEP for i in range(25)]
    assert post_csv(client, "/api/imports/readings",
                    readings_csv("MP-DIST", "M601", values)).status_code == 200

    # 同上但 04:00 的值再大 0.00001
    changed = list(values)
    changed[4] = changed[4] + STEP
    r = post_csv(client, "/api/imports/readings", readings_csv("MP-DIST", "M601", changed))
    assert r.status_code == 409
    assert "2026-04-01T04:00:00Z" in r.json()["detail"]

    # 原值重传仍是幂等
    r = post_csv(client, "/api/imports/readings", readings_csv("MP-DIST", "M601", values))
    assert r.status_code == 200
    assert r.json()["duplicates_skipped"] == 25


def test_same_numeric_value_different_scale_is_idempotent(client):
    content_a = "mp,meter,ts,cum_kwh\nMP-S,M1,2026-04-01T00:00:00Z,100.0\nMP-S,M1,2026-04-01T01:00:00Z,101.0\n"
    content_b = "mp,meter,ts,cum_kwh\nMP-S,M1,2026-04-01T00:00:00Z,100.000\nMP-S,M1,2026-04-01T01:00:00Z,101\n"
    assert post_csv(client, "/api/imports/readings", content_a).status_code == 200
    r = post_csv(client, "/api/imports/readings", content_b)
    assert r.status_code == 200 and r.json()["duplicates_skipped"] == 2


def test_meter_change_boundary_precision_preserved(client):
    """换表边界读数（旧表末/新表初）同样不能丢精度。"""
    old = [BASE + Decimal(i) * STEP for i in range(24)]          # 04-01 00..23
    new_start = datetime(2026, 4, 2, tzinfo=timezone.utc)
    new = [Decimal("100.00000") + Decimal(i) * STEP for i in range(25)]
    content = readings_csv("MP-X", "M1", old) + \
        "\n".join(readings_csv("MP-X", "M2", new, new_start).splitlines()[1:]) + "\n"
    assert post_csv(client, "/api/imports/readings", content).status_code == 200

    form = ("mp,change_ts,old_meter,new_meter,old_final_kwh,new_initial_kwh\n"
            f"MP-X,{ts(24)},M1,M2,{format(BASE + Decimal(24) * STEP, 'f')},100.00000\n")
    assert post_csv(client, "/api/imports/meter-changes", form).status_code == 200

    body = json_decimal(client.get("/api/settlement/daily",
                                   params={"mp": "MP-X", "date": "2026-04-01"}))
    assert body["status"] == "ok"
    assert Decimal(str(body["total_kwh"])) == Decimal("0.00024")


def test_pre_fix_preview_does_not_write(client):
    values = tiny_values()
    content = readings_csv("MP-PREV", "M600", values)
    r = client.post("/api/demo/pre-fix-preview",
                    files={"file": ("p.csv", content.encode(), "text/csv")})
    assert r.status_code == 200
    body = json_decimal(r)
    assert body["storage_before_fix"]["values_changed_by_float"] > 0
    day = next(d for d in body["days"] if d["date"] == "2026-04-01")
    assert Decimal(str(day["total_kwh"])) == Decimal("0.0002")  # 修复前的低估结果
    # 不写正式库
    assert client.get("/api/readings", params={"mp": "MP-PREV"}).json() == []


# ---------------------------------------------------------------- 旧库迁移

def test_legacy_db_migration_marks_unverified_and_pends(tmp_path, monkeypatch):
    path = tmp_path / "legacy.db"
    values = tiny_values()
    make_legacy_db(str(path), [("MP-L", "M1", ts(i), v) for i, v in enumerate(values)])
    monkeypatch.setenv("DB_PATH", str(path))

    with TestClient(main.app) as c:
        # 迁移后列类型为 TEXT，值被标记未校正
        conn = db.connect()
        try:
            cols = {r["name"]: r["type"] for r in conn.execute("PRAGMA table_info(readings)")}
            assert cols["cum_kwh"] == "TEXT"
            assert conn.execute("PRAGMA user_version").fetchone()[0] == db.SCHEMA_VERSION
            assert conn.execute(
                "SELECT COUNT(*) FROM readings WHERE legacy_unverified=1").fetchone()[0] == 25
        finally:
            conn.close()

        body = json_decimal(c.get("/api/settlement/daily",
                                  params={"mp": "MP-L", "date": "2026-04-01"}))
        assert body["status"] == "pending"
        assert body["total_kwh"] is None
        assert body["pending_intervals"][0]["reasons"] == ["legacy_precision_unverified"]

        # 按原始 CSV 重新导入 -> 全部校正，恢复确定核算
        r = post_csv(c, "/api/imports/readings", readings_csv("MP-L", "M1", values))
        assert r.status_code == 200
        assert r.json()["legacy_corrected"] == 25
        body = json_decimal(c.get("/api/settlement/daily",
                                  params={"mp": "MP-L", "date": "2026-04-01"}))
        assert body["status"] == "ok"
        assert Decimal(str(body["total_kwh"])) == Decimal("0.00024")


def test_legacy_value_same_as_float_is_still_corrected_and_reported(tmp_path, monkeypatch):
    """旧值恰好等于 float 换算值时，也必须重传校正、不能静默跳过。"""
    path = tmp_path / "legacy.db"
    # 选择 float 不损失的值
    values = [Decimal("100.0") + Decimal(i) for i in range(25)]
    make_legacy_db(str(path), [("MP-L", "M1", ts(i), v) for i, v in enumerate(values)])
    monkeypatch.setenv("DB_PATH", str(path))

    with TestClient(main.app) as c:
        r = post_csv(c, "/api/imports/readings", readings_csv("MP-L", "M1", values))
        body = r.json()
        assert r.status_code == 200
        assert body["legacy_corrected"] == 25
        assert body["duplicates_skipped"] == 0
        assert all(x["changed"] is False for x in body["legacy_corrections"])
        day = json_decimal(c.get("/api/settlement/daily",
                                 params={"mp": "MP-L", "date": "2026-04-01"}))
        assert day["status"] == "ok"


def test_legacy_meter_change_corrected_by_original_form(tmp_path, monkeypatch):
    path = tmp_path / "legacy.db"
    old = [BASE + Decimal(i) * STEP for i in range(24)]
    new_start = datetime(2026, 4, 2, tzinfo=timezone.utc)
    new = [Decimal("100.00000") + Decimal(i) * STEP for i in range(25)]
    rows = [("MP-LC", "M1", ts(i), v) for i, v in enumerate(old)]
    rows += [("MP-LC", "M2", ts(i, new_start), v) for i, v in enumerate(new)]
    make_legacy_db(
        str(path), rows,
        meter_changes=[("MP-LC", ts(24), "M1", "M2",
                        BASE + Decimal(24) * STEP, Decimal("100.00000"))],
    )
    monkeypatch.setenv("DB_PATH", str(path))

    with TestClient(main.app) as c:
        d1 = json_decimal(c.get("/api/settlement/daily",
                                params={"mp": "MP-LC", "date": "2026-04-01"}))
        assert d1["status"] == "pending"
        assert any("legacy_precision_unverified" in p["reasons"]
                   for p in d1["pending_intervals"])

        content = readings_csv("MP-LC", "M1", old) + \
            "\n".join(readings_csv("MP-LC", "M2", new, new_start).splitlines()[1:]) + "\n"
        assert post_csv(c, "/api/imports/readings", content).status_code == 200
        form = ("mp,change_ts,old_meter,new_meter,old_final_kwh,new_initial_kwh\n"
                f"MP-LC,{ts(24)},M1,M2,{format(BASE + Decimal(24) * STEP, 'f')},100.00000\n")
        r = post_csv(c, "/api/imports/meter-changes", form)
        assert r.status_code == 200 and r.json()["legacy_corrected"] == 1

        d1 = json_decimal(c.get("/api/settlement/daily",
                                params={"mp": "MP-LC", "date": "2026-04-01"}))
        assert d1["status"] == "ok"
        assert Decimal(str(d1["total_kwh"])) == Decimal("0.00024")


def test_failed_migration_keeps_old_database_intact(tmp_path, monkeypatch):
    """迁移遇到未知结构时整体回滚：旧库仍可被旧版本读取，不留半成品。"""
    path = tmp_path / "legacy.db"
    values = tiny_values(3)
    make_legacy_db(str(path), [("MP-L", "M1", ts(i), v) for i, v in enumerate(values)])
    # 构造一个无法识别的 meter_changes 结构（既非旧 REAL 也非当前 TEXT）
    conn = sqlite3.connect(path, isolation_level=None)
    conn.execute("DROP TABLE meter_changes")
    conn.execute("CREATE TABLE meter_changes (mp TEXT, change_ts TEXT, weird BLOB)")
    conn.close()

    monkeypatch.setenv("DB_PATH", str(path))
    with pytest.raises(RuntimeError):
        db.init_db()

    conn = sqlite3.connect(path, isolation_level=None)
    try:
        assert conn.execute("PRAGMA user_version").fetchone()[0] == 0
        cols = {r[1]: r[2] for r in conn.execute("PRAGMA table_info(readings)")}
        assert cols["cum_kwh"] == "REAL"          # 原表未被替换
        assert conn.execute("SELECT COUNT(*) FROM readings").fetchone()[0] == 3
        assert conn.execute(
            "SELECT COUNT(*) FROM sqlite_master WHERE name LIKE '%migrating%'").fetchone()[0] == 0
    finally:
        conn.close()


def test_legacy_real_not_trusted_when_user_version_preset(tmp_path, monkeypatch):
    """外部工具误设 user_version 时，仍按列类型识别旧 REAL 并迁移标记待核。"""
    path = tmp_path / "legacy.db"
    values = tiny_values(3)
    make_legacy_db(str(path), [("MP-L", "M1", ts(i), v) for i, v in enumerate(values)])
    conn = sqlite3.connect(path, isolation_level=None)
    conn.execute(f"PRAGMA user_version = {db.SCHEMA_VERSION}")
    conn.close()
    monkeypatch.setenv("DB_PATH", str(path))

    conn = db.connect()
    try:
        cols = {r["name"]: r["type"] for r in conn.execute("PRAGMA table_info(readings)")}
        assert cols["cum_kwh"] == "TEXT"
        assert conn.execute(
            "SELECT COUNT(*) FROM readings WHERE legacy_unverified=1").fetchone()[0] == 3
    finally:
        conn.close()


def test_migration_failure_midway_rolls_back_everything(tmp_path, monkeypatch):
    """迁移在换表单阶段失败：readings 的替换也必须回滚，旧库完全保持原状。"""
    path = tmp_path / "legacy.db"
    values = tiny_values(3)
    make_legacy_db(
        str(path),
        [("MP-L", "M1", ts(i), v) for i, v in enumerate(values)],
        meter_changes=[("MP-L", ts(2), "M1", "M2", values[2], Decimal("0"))],
    )
    monkeypatch.setenv("DB_PATH", str(path))

    def boom(conn):
        raise RuntimeError("injected migration failure")

    monkeypatch.setattr(db, "_migrate_meter_changes", boom)
    with pytest.raises(RuntimeError):
        db.init_db()

    conn = sqlite3.connect(path, isolation_level=None)
    try:
        assert conn.execute("PRAGMA user_version").fetchone()[0] == 0
        rcols = {r[1]: r[2] for r in conn.execute("PRAGMA table_info(readings)")}
        mcols = {r[1]: r[2] for r in conn.execute("PRAGMA table_info(meter_changes)")}
        assert rcols["cum_kwh"] == "REAL" and "legacy_unverified" not in rcols
        assert mcols["old_final_kwh"] == "REAL" and "legacy_unverified" not in mcols
        assert conn.execute("SELECT COUNT(*) FROM readings").fetchone()[0] == 3
        assert conn.execute("SELECT COUNT(*) FROM meter_changes").fetchone()[0] == 1
        assert conn.execute(
            "SELECT COUNT(*) FROM sqlite_master WHERE name LIKE '%migrating%'"
        ).fetchone()[0] == 0
    finally:
        conn.close()


def test_migration_retried_after_previous_failure(tmp_path, monkeypatch):
    path = tmp_path / "legacy.db"
    values = tiny_values(2)
    make_legacy_db(str(path), [("MP-L", "M1", ts(i), v) for i, v in enumerate(values)])
    monkeypatch.setenv("DB_PATH", str(path))
    db.init_db()  # 正常迁移成功
    # 二次调用幂等
    db.init_db()
    conn = db.connect()
    try:
        assert conn.execute("PRAGMA user_version").fetchone()[0] == db.SCHEMA_VERSION
        assert conn.execute("SELECT COUNT(*) FROM readings").fetchone()[0] == 2
    finally:
        conn.close()


def test_legacy_correction_batch_is_atomic(tmp_path, monkeypatch):
    """校正批次中若另一行冲突：整批回滚，legacy 标记保持，不出现部分校正。"""
    path = tmp_path / "legacy.db"
    values = tiny_values(3)
    make_legacy_db(str(path), [("MP-L", "M1", ts(i), v) for i, v in enumerate(values)])
    monkeypatch.setenv("DB_PATH", str(path))

    with TestClient(main.app) as c:
        # 先建一个正式导入的冲突行（新 mp）
        base = "mp,meter,ts,cum_kwh\nMP-N,M2,2026-04-01T00:00:00Z,1.0\n"
        assert post_csv(c, "/api/imports/readings", base).status_code == 200

        content = readings_csv("MP-L", "M1", values) + \
            "MP-N,M2,2026-04-01T00:00:00Z,2.0\n"
        r = post_csv(c, "/api/imports/readings", content)
        assert r.status_code == 409

        conn = db.connect()
        try:
            assert conn.execute(
                "SELECT COUNT(*) FROM readings WHERE mp='MP-L' AND legacy_unverified=1"
            ).fetchone()[0] == 3
            assert conn.execute(
                "SELECT cum_kwh FROM readings WHERE mp='MP-N'").fetchone()[0] == "1.0"
        finally:
            conn.close()


def test_demo_scenario_reports_pre_fix_and_fixed(client):
    r = client.get("/api/demo/scenario")
    assert r.status_code == 200
    body = json_decimal(r)
    assert body["pre_fix"]["total_kwh"] == Decimal("0.0002")
    assert body["fixed"]["total_kwh"] == Decimal("0.00024")
    assert body["pre_fix"]["values_changed_by_float"] > 0
    # 不写正式库
    assert client.get("/api/readings", params={"mp": "DEMO-PREC"}).json() == []


def test_demo_legacy_seed_requires_prefix_and_confirm(client):
    content = readings_csv("REAL-MP", "M1", tiny_values(3))
    r = post_csv(client, "/api/demo/legacy-seed/readings?confirm=true", content)
    assert r.status_code == 422
    demo = readings_csv("DEMO-X", "M1", tiny_values(3))
    assert post_csv(client, "/api/demo/legacy-seed/readings", demo).status_code == 422
    assert post_csv(client, "/api/demo/legacy-seed/readings?confirm=true",
                    demo).status_code == 200
    body = json_decimal(client.get("/api/settlement/daily",
                                   params={"mp": "DEMO-X", "date": "2026-04-01"}))
    assert body["status"] == "pending"
    assert "legacy_precision_unverified" in body["pending_intervals"][0]["reasons"]
    # 演示重置
    assert client.post("/api/demo/reset?mp=DEMO-X&confirm=true").status_code == 200
    assert client.get("/api/readings", params={"mp": "DEMO-X"}).json() == []


def test_interleaved_two_instances_never_expose_partial_state(tmp_path, monkeypatch):
    """两个独立连接反复交错：迁移中并发查询/导入，每次查询都是完整状态。"""
    path = tmp_path / "legacy.db"
    values = tiny_values()
    make_legacy_db(str(path), [("MP-L", "M1", ts(i), v) for i, v in enumerate(values)])
    monkeypatch.setenv("DB_PATH", str(path))

    errors: list[str] = []
    stop = threading.Event()

    def observer(tag: str):
        try:
            while not stop.is_set():
                conn = db.connect()  # 内部可能触发/等待迁移
                try:
                    conn.execute("BEGIN")
                    cols = {r["name"]: r["type"]
                            for r in conn.execute("PRAGMA table_info(readings)")}
                    n = conn.execute("SELECT COUNT(*) FROM readings").fetchone()[0]
                    flags = conn.execute(
                        "SELECT COUNT(*) FROM readings WHERE legacy_unverified=1"
                    ).fetchone()[0]
                    uv = conn.execute("PRAGMA user_version").fetchone()[0]
                    conn.execute("COMMIT")
                    if cols.get("cum_kwh") != "TEXT" or "legacy_unverified" not in cols:
                        errors.append(f"{tag}: partial columns {cols}")
                    if uv != db.SCHEMA_VERSION:
                        errors.append(f"{tag}: user_version {uv}")
                    if n != 25 or flags != 25:
                        errors.append(f"{tag}: inconsistent rows n={n} flags={flags}")
                finally:
                    conn.close()
        except Exception as e:  # noqa: BLE001
            errors.append(f"{tag}: {e}")

    def migrator():
        try:
            for _ in range(5):
                db.init_db()
        except Exception as e:  # noqa: BLE001
            errors.append(f"migrator: {e}")
        finally:
            stop.set()

    threads = [threading.Thread(target=observer, args=(f"obs{i}",)) for i in range(3)]
    threads.append(threading.Thread(target=migrator))
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=30)
    assert errors == []



def test_concurrent_legacy_correction_is_idempotent(tmp_path, monkeypatch):
    """两实例同时用原始 CSV 校正同一批旧值：串行化后一个校正、一个幂等跳过。"""
    path = tmp_path / "legacy.db"
    values = tiny_values()
    make_legacy_db(str(path), [("MP-L", "M1", ts(i), v) for i, v in enumerate(values)])
    monkeypatch.setenv("DB_PATH", str(path))

    errors: list[str] = []
    results: list[dict] = []
    ready = threading.Barrier(2)

    def correct():
        try:
            ready.wait(timeout=5)
            rows = main.parse_readings_csv(readings_csv("MP-L", "M1", values).encode())
            results.append(main._store_readings(rows, "correct"))
        except Exception as e:  # noqa: BLE001
            errors.append(f"correct: {e}")

    threads = [threading.Thread(target=correct) for _ in range(2)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert errors == []
    assert sorted(r["legacy_corrected"] for r in results) == [0, 25]
    assert sorted(r["duplicates_skipped"] for r in results) == [0, 25]
    conn = db.connect()
    try:
        assert conn.execute(
            "SELECT COUNT(*) FROM readings WHERE legacy_unverified=1").fetchone()[0] == 0
    finally:
        conn.close()
    day = json_decimal(TestClient(main.app).get(
        "/api/settlement/daily", params={"mp": "MP-L", "date": "2026-04-01"}))
    assert day["total_kwh"] == Decimal("0.00024")


# ---------------------------------------------------------------- 双实例交错启动

def test_concurrent_first_start_migrates_once(tmp_path, monkeypatch):
    """两个实例同时首次启动：迁移只发生一次，双方都看到完整且一致的 schema。"""
    path = tmp_path / "legacy.db"
    values = tiny_values()
    make_legacy_db(str(path), [("MP-L", "M1", ts(i), v) for i, v in enumerate(values)])
    monkeypatch.setenv("DB_PATH", str(path))

    errors: list[str] = []
    ready = threading.Barrier(2)

    def starter():
        try:
            ready.wait(timeout=5)
            conn = db.connect()
            try:
                # 每个实例都必须看到完整迁移结果，绝不可见 REAL 半成品
                cols = {r["name"]: r["type"] for r in conn.execute("PRAGMA table_info(readings)")}
                if cols.get("cum_kwh") != "TEXT":
                    errors.append(f"half-migrated schema visible: {cols}")
                if "legacy_unverified" not in cols:
                    errors.append(f"legacy flag missing: {cols}")
                if conn.execute("PRAGMA user_version").fetchone()[0] != db.SCHEMA_VERSION:
                    errors.append("schema version not current")
                if conn.execute("SELECT COUNT(*) FROM readings").fetchone()[0] != 25:
                    errors.append("readings count wrong")
            finally:
                conn.close()
        except Exception as e:  # noqa: BLE001
            errors.append(f"starter: {e}")

    threads = [threading.Thread(target=starter) for _ in range(2)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert errors == []


def test_concurrent_start_and_import_no_partial_state(tmp_path, monkeypatch):
    """一实例迁移、另一实例同时导入/查询：读到的要么迁移前完整旧库要么完整新库。"""
    path = tmp_path / "legacy.db"
    values = tiny_values()
    make_legacy_db(str(path), [("MP-L", "M1", ts(i), v) for i, v in enumerate(values)])
    monkeypatch.setenv("DB_PATH", str(path))

    errors: list[str] = []
    ready = threading.Barrier(2)

    def migrator():
        try:
            ready.wait(timeout=5)
            db.init_db()
        except Exception as e:  # noqa: BLE001
            errors.append(f"migrator: {e}")

    def reader():
        try:
            ready.wait(timeout=5)
            for _ in range(60):
                conn = db.connect()  # connect() 自身保证 schema 就绪
                try:
                    conn.execute("BEGIN")
                    cols = {r["name"]: r["type"]
                            for r in conn.execute("PRAGMA table_info(readings)")}
                    count = conn.execute("SELECT COUNT(*) FROM readings").fetchone()[0]
                    conn.execute("COMMIT")
                    if cols.get("cum_kwh") != "TEXT" or "legacy_unverified" not in cols:
                        errors.append(f"partial schema visible: {cols}")
                    if count != 25:
                        errors.append(f"partial data visible: {count}")
                finally:
                    conn.close()
        except Exception as e:  # noqa: BLE001
            errors.append(f"reader: {e}")

    threads = [threading.Thread(target=migrator), threading.Thread(target=reader)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert errors == []
