"""SQLite 数据访问层。

精度与一致性设计：
- 累计读数/换表边界读数以**十进制文本**（``TEXT`` 亲和性）持久化，
  避免 REAL/float 在 ~1e12 量级上吃掉 1e-5 甚至更小的差异。
- 旧库（REAL 列）启动时在**单个 BEGIN IMMEDIATE 事务**内迁移：
  建新表、CAST 旧值为文本、打上 ``legacy_unverified=1``、重建索引、
  设置 ``PRAGMA user_version``。任何一步失败整体 ROLLBACK，原库保持可读。
- 迁移以 ``user_version`` + 列类型判定，并在拿到写锁后二次确认，多个 API 实例
  交错启动时只有一个执行迁移，另一个等待后直接使用已提交结果。
- WAL 模式：读事务看到一致的已提交快照，读写互不阻塞。
- 导入用 BEGIN IMMEDIATE 单事务提交，任何一行失败整体 ROLLBACK。
"""
from __future__ import annotations

import os
import sqlite3
import time

SCHEMA_VERSION = 1

# 注意：所有建表语句逐条 execute，不能用 executescript（它会先 COMMIT，
# 破坏迁移/初始化的原子性）。
CREATE_IMPORTS = """
CREATE TABLE IF NOT EXISTS imports (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    kind        TEXT NOT NULL CHECK (kind IN ('readings', 'meter_changes')),
    source_name TEXT NOT NULL DEFAULT '',
    row_count   INTEGER NOT NULL,
    created_at  TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
)
"""

CREATE_READINGS = """
CREATE TABLE IF NOT EXISTS readings (
    mp                TEXT NOT NULL,       -- 计量点
    meter             TEXT NOT NULL,       -- 表号
    ts                TEXT NOT NULL,       -- UTC 整点，格式 YYYY-MM-DDTHH:00:00Z
    cum_kwh           TEXT NOT NULL,       -- 累计 kWh，十进制原文（如 999999999999.99001）
    legacy_unverified INTEGER NOT NULL DEFAULT 0,  -- 1=由旧库 REAL 迁移、尚未按原始 CSV 校正
    import_id         INTEGER NOT NULL REFERENCES imports(id),
    PRIMARY KEY (mp, meter, ts)
)
"""

CREATE_READINGS_INDEX = "CREATE INDEX IF NOT EXISTS idx_readings_mp_ts ON readings (mp, ts)"

CREATE_METER_CHANGES = """
CREATE TABLE IF NOT EXISTS meter_changes (
    mp                TEXT NOT NULL,       -- 计量点
    change_ts         TEXT NOT NULL,       -- 换表整点 UTC
    old_meter         TEXT NOT NULL,       -- 旧表号
    new_meter         TEXT NOT NULL,       -- 新表号
    old_final_kwh     TEXT NOT NULL,       -- 旧表末读数（边界读数），十进制原文
    new_initial_kwh   TEXT NOT NULL,       -- 新表初值（边界读数），十进制原文
    legacy_unverified INTEGER NOT NULL DEFAULT 0,  -- 1=由旧库 REAL 迁移、尚未按原始单据校正
    import_id         INTEGER NOT NULL REFERENCES imports(id),
    PRIMARY KEY (mp, change_ts)
)
"""

CREATE_METER_CHANGES_INDEX = (
    "CREATE INDEX IF NOT EXISTS idx_meter_changes_mp_ts ON meter_changes (mp, change_ts)"
)

# 旧版（REAL 存储）表结构，仅用于迁移时重建。
# 迁移时旧表整表 DROP，因此这里只用于读取列/复制数据。
_MIGRATED_READINGS = "readings__migrating"
_MIGRATED_METER_CHANGES = "meter_changes__migrating"


def db_path() -> str:
    return os.environ.get("DB_PATH", "/data/energy.db")


def _raw_connect() -> sqlite3.Connection:
    conn = sqlite3.connect(db_path(), isolation_level=None, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout=30000")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA foreign_keys=ON")
    _enable_wal(conn)
    return conn


def _enable_wal(conn: sqlite3.Connection) -> None:
    """把库切到 WAL。旧库首次被两个实例同时打开时，journal_mode 切换需要
    排他锁且不一定走 busy handler，因此这里显式重试；已是 WAL 则跳过。"""
    for attempt in range(200):
        try:
            mode = conn.execute("PRAGMA journal_mode").fetchone()[0]
            if str(mode).lower() == "wal":
                return
            conn.execute("PRAGMA journal_mode=WAL")
            return
        except sqlite3.OperationalError as exc:
            if "locked" not in str(exc).lower() and "busy" not in str(exc).lower():
                raise
            time.sleep(0.01 + min(attempt, 50) * 0.002)
    raise sqlite3.OperationalError("无法将数据库切换到 WAL 模式：database is locked")


def _table_info(conn: sqlite3.Connection, table: str) -> dict[str, str] | None:
    rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    if not rows:
        return None
    return {r["name"]: (r["type"] or "").upper() for r in rows}


def _is_current(columns: dict[str, str] | None, value_cols: tuple[str, ...]) -> bool:
    """当前 schema 的判定：有 legacy 标记列，且所有运算列为 TEXT 亲和性。"""
    if not columns:
        return False
    if "legacy_unverified" not in columns:
        return False
    return all(columns.get(c) == "TEXT" for c in value_cols)


def _migrate_readings(conn: sqlite3.Connection) -> None:
    conn.execute(f"DROP TABLE IF EXISTS {_MIGRATED_READINGS}")
    conn.execute(
        f"""CREATE TABLE {_MIGRATED_READINGS} (
    mp                TEXT NOT NULL,
    meter             TEXT NOT NULL,
    ts                TEXT NOT NULL,
    cum_kwh           TEXT NOT NULL,
    legacy_unverified INTEGER NOT NULL DEFAULT 0,
    import_id         INTEGER NOT NULL REFERENCES imports(id),
    PRIMARY KEY (mp, meter, ts)
)"""
    )
    # CAST(REAL AS TEXT) 只能还原 SQLite 最短往返表示；原始 CSV 的小数
    # 已不可知，因此整行标记 legacy_unverified=1，相关增量必须待核。
    conn.execute(
        f"""INSERT INTO {_MIGRATED_READINGS}
              (mp, meter, ts, cum_kwh, legacy_unverified, import_id)
           SELECT mp, meter, ts, CAST(cum_kwh AS TEXT), 1, import_id
           FROM readings"""
    )
    conn.execute("DROP TABLE readings")
    conn.execute(f"ALTER TABLE {_MIGRATED_READINGS} RENAME TO readings")


def _migrate_meter_changes(conn: sqlite3.Connection) -> None:
    conn.execute(f"DROP TABLE IF EXISTS {_MIGRATED_METER_CHANGES}")
    conn.execute(
        f"""CREATE TABLE {_MIGRATED_METER_CHANGES} (
    mp                TEXT NOT NULL,
    change_ts         TEXT NOT NULL,
    old_meter         TEXT NOT NULL,
    new_meter         TEXT NOT NULL,
    old_final_kwh     TEXT NOT NULL,
    new_initial_kwh   TEXT NOT NULL,
    legacy_unverified INTEGER NOT NULL DEFAULT 0,
    import_id         INTEGER NOT NULL REFERENCES imports(id),
    PRIMARY KEY (mp, change_ts)
)"""
    )
    conn.execute(
        f"""INSERT INTO {_MIGRATED_METER_CHANGES}
              (mp, change_ts, old_meter, new_meter,
               old_final_kwh, new_initial_kwh, legacy_unverified, import_id)
           SELECT mp, change_ts, old_meter, new_meter,
                  CAST(old_final_kwh AS TEXT), CAST(new_initial_kwh AS TEXT), 1, import_id
           FROM meter_changes"""
    )
    conn.execute("DROP TABLE meter_changes")
    conn.execute(f"ALTER TABLE {_MIGRATED_METER_CHANGES} RENAME TO meter_changes")


def _apply_schema(conn: sqlite3.Connection) -> None:
    """在当前写事务内建/迁移全部表。调用方负责 BEGIN/COMMIT/ROLLBACK。"""
    conn.execute(CREATE_IMPORTS)

    readings_cols = _table_info(conn, "readings")
    if readings_cols is None:
        conn.execute(CREATE_READINGS)
    elif not _is_current(readings_cols, ("cum_kwh",)):
        # 只接受已知的旧 REAL 结构；未知结构拒绝迁移并回滚，避免误删数据。
        if readings_cols.get("cum_kwh") != "REAL":
            raise RuntimeError(f"无法识别的 readings 表结构: {readings_cols}")
        _migrate_readings(conn)
    conn.execute(CREATE_READINGS_INDEX)

    changes_cols = _table_info(conn, "meter_changes")
    if changes_cols is None:
        conn.execute(CREATE_METER_CHANGES)
    elif not _is_current(changes_cols, ("old_final_kwh", "new_initial_kwh")):
        if changes_cols.get("old_final_kwh") != "REAL":
            raise RuntimeError(f"无法识别的 meter_changes 表结构: {changes_cols}")
        _migrate_meter_changes(conn)
    conn.execute(CREATE_METER_CHANGES_INDEX)

    conn.execute(f"PRAGMA user_version = {SCHEMA_VERSION}")


def _schema_is_current(conn: sqlite3.Connection) -> bool:
    """版本号只是快速判据；必须同时确认列类型，避免外部工具误设 user_version
    时旧 REAL 表被当成已迁移（那会再次静默丢精度）。"""
    if conn.execute("PRAGMA user_version").fetchone()[0] < SCHEMA_VERSION:
        return False
    readings = _table_info(conn, "readings")
    changes = _table_info(conn, "meter_changes")
    return (_is_current(readings, ("cum_kwh",))
            and _is_current(changes, ("old_final_kwh", "new_initial_kwh")))


def ensure_schema(conn: sqlite3.Connection) -> None:
    """确保库已处于当前 schema；旧库迁移是原子且幂等的。"""
    if _schema_is_current(conn):
        return

    # BEGIN IMMEDIATE 串行化多实例启动；等待期间另一实例可能已完成迁移。
    conn.execute("BEGIN IMMEDIATE")
    try:
        if _schema_is_current(conn):
            conn.execute("COMMIT")
            return
        _apply_schema(conn)
        conn.execute("COMMIT")
    except BaseException:
        try:
            conn.execute("ROLLBACK")
        except sqlite3.Error:
            pass
        raise


def connect() -> sqlite3.Connection:
    """新建连接（autocommit 模式）+ 确保 schema 就绪，事务由调用方显式控制。"""
    conn = _raw_connect()
    try:
        ensure_schema(conn)
    except BaseException:
        conn.close()
        raise
    return conn


def init_db() -> None:
    path = db_path()
    parent = os.path.dirname(path)
    if parent:
        os.makedirs(parent, exist_ok=True)
    conn = _raw_connect()
    try:
        ensure_schema(conn)
    finally:
        conn.close()
