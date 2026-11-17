"""SQLite 历史库：只增不改的变更日志 + 捕获状态（支持断档后重建捕获）。

关键约束：
- 所有写入在一把进程内锁内串行化，SQLite 开启 WAL；
- 一个源事务的全部行变更与该事务的提交点在同一个 SQLite 事务里落盘，
  保证多行事务整体可见、不会出现半条事务；
- 复制槽的推进位置（last_commit_lsn）与对应数据在同一事务更新，
  崩溃重启后靠 (generation, xid) 去重，绝不重复、也不丢数据；
- 复制槽被人工删除后可“重建捕获”：旧历史全部保留，新基线写入新的
  generation（世代），两个世代之间的未覆盖区间在 points / directory /
  history 各接口显式标注，旧点只按旧段回看，新点绝不混入断档前残留值。
"""
from __future__ import annotations

import sqlite3
import threading
from datetime import datetime, timezone
from typing import Any, Iterable

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS capture_state (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS txns (
    xid            INTEGER PRIMARY KEY,
    commit_lsn     TEXT NOT NULL,
    commit_lsn_num INTEGER NOT NULL,
    commit_ts      TEXT
);

CREATE TABLE IF NOT EXISTS artifact_history (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    artifact_no  INTEGER NOT NULL,
    change_type  TEXT NOT NULL,              -- BASELINE / INSERT / UPDATE / DELETE
    service_name TEXT,
    version      TEXT,
    sha256       TEXT,
    xid          INTEGER,
    commit_lsn   TEXT,
    commit_lsn_num INTEGER,
    captured_at  TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_hist_artifact ON artifact_history(artifact_no, id);
CREATE INDEX IF NOT EXISTS idx_hist_lsn ON artifact_history(commit_lsn_num);

-- 重建捕获引入的世代表：一行 = 一段连续捕获历史
CREATE TABLE IF NOT EXISTS generations (
    generation       INTEGER PRIMARY KEY,
    kind             TEXT NOT NULL,          -- INITIAL / REBUILD
    slot_name        TEXT NOT NULL,
    baseline_lsn     TEXT NOT NULL,
    baseline_lsn_num INTEGER NOT NULL,
    baseline_captured_at TEXT NOT NULL,
    gap_from_lsn     TEXT,                   -- 与上一世代之间未覆盖区间起点(不含)
    gap_to_lsn       TEXT,                   -- 未覆盖区间终点(不含，即新基线点)
    note             TEXT
);
"""

PHASE_INITIALIZING = "INITIALIZING"
PHASE_STREAMING = "STREAMING"
PHASE_ERROR = "ERROR"
PHASE_REBUILDING = "REBUILDING"

KIND_INITIAL = "INITIAL"
KIND_REBUILD = "REBUILD"


def lsn_to_num(lsn: str) -> int:
    """'0/16B3748' -> 整数，便于比较先后。"""
    high, low = lsn.split("/", 1)
    return (int(high, 16) << 32) | int(low, 16)


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f") + "Z"


class HistoryStore:
    def __init__(self, path: str) -> None:
        import os

        directory = os.path.dirname(path) or "."
        os.makedirs(directory, exist_ok=True)
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(
            path, check_same_thread=False, isolation_level=None
        )
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL;")
        self._conn.execute("PRAGMA synchronous=FULL;")
        self._conn.execute("PRAGMA foreign_keys=ON;")
        self._conn.executescript(SCHEMA_SQL)
        self._migrate_legacy()

    # ------------------------------------------------------------- migration
    def _migrate_legacy(self) -> None:
        """旧库（无 generations 表 / 无 generation 列）平滑迁移。

        迁移只追加世代元数据并回填 generation=0，绝不改动任何历史行；
        迁移本身在一个 SQLite 事务内完成，中断后重跑结果一致。
        """
        with self._lock:
            cols = {
                r["name"]
                for r in self._conn.execute("PRAGMA table_info(txns)").fetchall()
            }
            if "generation" not in cols:
                self._conn.execute("BEGIN IMMEDIATE")
                try:
                    self._conn.execute(
                        "ALTER TABLE txns ADD COLUMN generation INTEGER NOT NULL DEFAULT 0"
                    )
                    self._conn.execute(
                        "ALTER TABLE artifact_history ADD COLUMN"
                        " generation INTEGER NOT NULL DEFAULT 0"
                    )
                    self._conn.execute(
                        "CREATE UNIQUE INDEX IF NOT EXISTS ux_txns_gen_xid"
                        " ON txns(generation, xid)"
                    )
                    self._conn.execute(
                        "CREATE INDEX IF NOT EXISTS idx_hist_gen_lsn"
                        " ON artifact_history(generation, commit_lsn_num, id)"
                    )
                    self._conn.execute(
                        "CREATE INDEX IF NOT EXISTS idx_hist_gen_art"
                        " ON artifact_history(generation, artifact_no, id)"
                    )
                    self._conn.execute("COMMIT")
                except Exception:
                    self._conn.execute("ROLLBACK")
                    raise
            # 旧库已有基线但没有世代行：补一条 INITIAL 世代（幂等）
            state = self.get_state()
            baseline = state.get("baseline_lsn")
            if baseline:
                exists = self._conn.execute(
                    "SELECT 1 FROM generations WHERE generation=0"
                ).fetchone()
                if not exists:
                    self._conn.execute(
                        "INSERT INTO generations(generation, kind, slot_name,"
                        " baseline_lsn, baseline_lsn_num, baseline_captured_at,"
                        " gap_from_lsn, gap_to_lsn, note)"
                        " VALUES (0, ?, ?, ?, ?, ?, NULL, NULL, NULL)",
                        (
                            KIND_INITIAL,
                            state.get("slot_name", ""),
                            baseline,
                            lsn_to_num(baseline),
                            state.get("baseline_captured_at") or utc_now_iso(),
                        ),
                    )

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    # ------------------------------------------------------------------ state
    def _get_state(self, key: str) -> str | None:
        row = self._conn.execute(
            "SELECT value FROM capture_state WHERE key=?", (key,)
        ).fetchone()
        return row["value"] if row else None

    def get_state(self) -> dict[str, str]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT key, value FROM capture_state"
            ).fetchall()
            return {r["key"]: r["value"] for r in rows}

    def get_phase(self) -> str:
        return self._get_state("phase") or PHASE_INITIALIZING

    def is_ready(self) -> bool:
        # 只要存在任一已完成世代，历史就始终开放：即使复制槽缺失进入
        # ERROR、或正在 REBUILDING，旧段提交点仍可正常回看，绝不因捕获
        # 侧故障而封存已落盘历史。仅首次基线（无任何世代）时返回 503。
        return self.has_any_generation()

    def _set_many(self, items: dict[str, Any]) -> None:
        self._conn.executemany(
            """
            INSERT INTO capture_state(key, value) VALUES(?, ?)
            ON CONFLICT(key) DO UPDATE SET value=excluded.value
            """,
            [(k, str(v)) for k, v in items.items()],
        )

    def mark_initializing(self, slot_name: str) -> None:
        with self._lock:
            self._set_many(
                {
                    "phase": PHASE_INITIALIZING,
                    "slot_name": slot_name,
                    "initialized_at": utc_now_iso(),
                    "last_error": "",
                }
            )

    def mark_error(self, message: str) -> None:
        with self._lock:
            self._set_many({"phase": PHASE_ERROR, "last_error": message})

    def clear_error(self) -> None:
        with self._lock:
            self._set_many({"last_error": ""})

    def update_last_message_lsn(self, lsn: str) -> None:
        with self._lock:
            self._set_many({"last_message_lsn": lsn})

    # ------------------------------------------------------------- generations
    def get_generations(self) -> list[dict[str, Any]]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM generations ORDER BY generation"
            ).fetchall()
            return [dict(r) for r in rows]

    def current_generation(self) -> int:
        with self._lock:
            row = self._conn.execute(
                "SELECT COALESCE(MAX(generation), 0) AS g FROM generations"
            ).fetchone()
            return int(row["g"])

    def has_any_generation(self) -> bool:
        with self._lock:
            return (
                self._conn.execute(
                    "SELECT 1 FROM generations LIMIT 1"
                ).fetchone()
                is not None
            )

    def get_generation_row(self, generation: int) -> dict[str, Any] | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM generations WHERE generation=?", (generation,)
            ).fetchone()
            return dict(row) if row else None

    def last_captured_lsn(self, generation: int) -> str | None:
        """该世代最后一个已落盘提交点 LSN（无事务时回退到基线点）。"""
        with self._lock:
            row = self._conn.execute(
                "SELECT commit_lsn FROM txns WHERE generation=?"
                " ORDER BY commit_lsn_num DESC LIMIT 1",
                (generation,),
            ).fetchone()
            if row is not None:
                return row["commit_lsn"]
            grow = self.get_generation_row(generation)
            return grow["baseline_lsn"] if grow else None

    def begin_rebuild(self, slot_name: str) -> int:
        """进入重建阶段。仅写入状态机，不碰任何旧历史；返回新世代号。

        重建中断后重跑：若上次停在 REBUILDING 且尚未落任何新世代数据，
        直接复用同一世代号（generations 行只在基线落盘时才创建），
        因此不会产生空世代，也不会暴露半段历史。
        """
        with self._lock:
            phase = self.get_phase()
            if phase == PHASE_REBUILDING:
                # 上次重建未完成（崩溃/中断后续做）：沿用待提交的世代号
                pending = self._get_state("rebuild_pending_generation")
                if pending is not None:
                    self._set_many({"last_error": ""})
                    return int(pending)
            next_gen = self.current_generation() + 1
            self._set_many(
                {
                    "phase": PHASE_REBUILDING,
                    "rebuild_requested_at": utc_now_iso(),
                    "rebuild_pending_generation": next_gen,
                    "last_error": "",
                }
            )
            return next_gen

    def save_baseline(
        self,
        rows: Iterable[sqlite3.Row | tuple[int, str, str, str]],
        lsn: str,
        generation: int = 0,
        kind: str = KIND_INITIAL,
        gap: tuple[str | None, str | None] | None = None,
        slot_name: str | None = None,
        note: str | None = None,
    ) -> None:
        """整批基线与世代/基线提交点在一个 SQLite 事务里落盘。

        世代行与首批 BASELINE 历史同事务写入：要么整段新世代可见，
        要么完全不存在（崩溃后重试不会留下半段）。
        """
        now = utc_now_iso()
        lsn_num = lsn_to_num(lsn)
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                if generation == 0 and kind == KIND_INITIAL:
                    self._set_many(
                        {
                            "phase": PHASE_INITIALIZING,
                            "baseline_lsn": lsn,
                            "baseline_lsn_num": lsn_num,
                            "baseline_captured_at": now,
                        }
                    )
                gap_from = gap[0] if gap else None
                gap_to = gap[1] if gap else None
                self._conn.execute(
                    "INSERT INTO generations(generation, kind, slot_name,"
                    " baseline_lsn, baseline_lsn_num, baseline_captured_at,"
                    " gap_from_lsn, gap_to_lsn, note)"
                    " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        generation,
                        kind,
                        slot_name or self._get_state("slot_name") or "",
                        lsn,
                        lsn_num,
                        now,
                        gap_from,
                        gap_to,
                        note,
                    ),
                )
                payload = [
                    (
                        r[0],
                        "BASELINE",
                        r[1],
                        r[2],
                        r[3],
                        None,
                        lsn,
                        lsn_num,
                        now,
                        generation,
                    )
                    for r in rows
                ]
                self._conn.executemany(
                    """
                    INSERT INTO artifact_history(
                        artifact_no, change_type, service_name, version, sha256,
                        xid, commit_lsn, commit_lsn_num, captured_at, generation)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    payload,
                )
                self._conn.execute("COMMIT")
            except Exception:
                self._conn.execute("ROLLBACK")
                raise

    def mark_streaming(self, generation: int | None = None) -> None:
        with self._lock:
            items: dict[str, Any] = {
                "phase": PHASE_STREAMING,
                "streaming_at": utc_now_iso(),
            }
            if generation is not None:
                items["current_generation"] = generation
                items["rebuild_pending_generation"] = ""
                row = self.get_generation_row(generation)
                if row is not None:
                    items["baseline_lsn"] = row["baseline_lsn"]
                    items["baseline_lsn_num"] = row["baseline_lsn_num"]
                    items["baseline_captured_at"] = row["baseline_captured_at"]
            self._set_many(items)

    def persist_transaction(
        self,
        xid: int,
        commit_lsn: str,
        commit_ts: str | None,
        changes: list[dict[str, Any]],
        generation: int = 0,
    ) -> bool:
        """落盘一个已提交源事务。返回 True=新写入，False=重复事务（已跳过）。"""
        lsn_num = lsn_to_num(commit_lsn)
        now = utc_now_iso()
        with self._lock:
            existed = self._conn.execute(
                "SELECT 1 FROM txns WHERE generation=? AND xid=?",
                (generation, xid),
            ).fetchone()
            if existed:
                return False
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                self._conn.execute(
                    "INSERT INTO txns(generation, xid, commit_lsn,"
                    " commit_lsn_num, commit_ts) VALUES (?, ?, ?, ?, ?)",
                    (generation, xid, commit_lsn, lsn_num, commit_ts),
                )
                self._conn.executemany(
                    """
                    INSERT INTO artifact_history(
                        artifact_no, change_type, service_name, version, sha256,
                        xid, commit_lsn, commit_lsn_num, captured_at, generation)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    [
                        (
                            c["artifact_no"],
                            c["change_type"],
                            c.get("service_name"),
                            c.get("version"),
                            c.get("sha256"),
                            xid,
                            commit_lsn,
                            lsn_num,
                            now,
                            generation,
                        )
                        for c in changes
                    ],
                )
                # 槽位推进位置与数据同事务落盘（按世代记录）
                self._set_many(
                    {
                        f"last_commit_lsn_gen{generation}": commit_lsn,
                        f"last_commit_lsn_num_gen{generation}": lsn_num,
                        f"last_xid_gen{generation}": xid,
                        "last_commit_lsn": commit_lsn,
                        "last_commit_lsn_num": lsn_num,
                        "last_xid": xid,
                    }
                )
                self._conn.execute("COMMIT")
                return True
            except Exception:
                self._conn.execute("ROLLBACK")
                raise

    # ------------------------------------------------------------------ reads
    def get_points(self) -> list[dict[str, Any]]:
        """全部可查询提交点，按世代分段并在段间插入 GAP 标记。"""
        with self._lock:
            gens = self._conn.execute(
                "SELECT * FROM generations ORDER BY generation"
            ).fetchall()
            points: list[dict[str, Any]] = []
            for g in gens:
                gen = g["generation"]
                if gen > 0:
                    prev = self._conn.execute(
                        "SELECT * FROM generations WHERE generation=?",
                        (gen - 1,),
                    ).fetchone()
                    # 两段之间的未覆盖区间：上一世代最后捕获点(不含)
                    # 到新基线点(不含)。旧点仍只按旧段回看，新点不混入旧值。
                    points.append(
                        {
                            "kind": "GAP",
                            "generation": gen,
                            "lsn": g["gap_to_lsn"],
                            "lsn_num": lsn_to_num(g["gap_to_lsn"])
                            if g["gap_to_lsn"]
                            else None,
                            "gap_from_lsn": g["gap_from_lsn"],
                            "gap_to_lsn": g["gap_to_lsn"],
                            "xid": None,
                            "commit_ts": None,
                            "change_count": 0,
                        }
                    )
                count = self._conn.execute(
                    "SELECT COUNT(*) AS c FROM artifact_history"
                    " WHERE change_type='BASELINE' AND generation=?",
                    (gen,),
                ).fetchone()["c"]
                points.append(
                    {
                        "kind": "BASELINE",
                        "generation": gen,
                        "lsn": g["baseline_lsn"],
                        "lsn_num": g["baseline_lsn_num"],
                        "xid": None,
                        "commit_ts": g["baseline_captured_at"],
                        "change_count": count,
                    }
                )
                for row in self._conn.execute(
                    """
                    SELECT t.xid, t.commit_lsn, t.commit_lsn_num, t.commit_ts,
                           (SELECT COUNT(*) FROM artifact_history h
                             WHERE h.generation = t.generation AND h.xid = t.xid
                             AND h.change_type != 'BASELINE') AS change_count
                    FROM txns t WHERE t.generation=?
                    ORDER BY t.commit_lsn_num, t.xid
                    """,
                    (gen,),
                ).fetchall():
                    points.append(
                        {
                            "kind": "TXN",
                            "generation": gen,
                            "lsn": row["commit_lsn"],
                            "lsn_num": row["commit_lsn_num"],
                            "xid": row["xid"],
                            "commit_ts": row["commit_ts"],
                            "change_count": row["change_count"],
                        }
                    )
            return points

    def get_directory_at(self, lsn_num: int, generation: int) -> list[dict[str, Any]]:
        """查看某世代内某个提交点（含）为止的整目录。

        严格限定 generation：新世代的查询绝不会混入断档前旧世代的残留值，
        旧世代查询也看不到新世代的数据。
        """
        with self._lock:
            rows = self._conn.execute(
                """
                SELECT artifact_no, service_name, version, sha256, change_type
                FROM artifact_history
                WHERE generation=? AND id IN (
                    SELECT MAX(id) FROM artifact_history
                    WHERE generation=? AND commit_lsn_num <= ?
                    GROUP BY artifact_no
                )
                AND change_type != 'DELETE'
                ORDER BY artifact_no
                """,
                (generation, generation, lsn_num),
            ).fetchall()
            return [dict(r) for r in rows]

    def get_history_of(self, artifact_no: int) -> list[dict[str, Any]]:
        with self._lock:
            rows = self._conn.execute(
                """
                SELECT artifact_no, change_type, service_name, version, sha256,
                       xid, commit_lsn, commit_lsn_num, captured_at, generation
                FROM artifact_history WHERE artifact_no=?
                ORDER BY generation, id
                """,
                (artifact_no,),
            ).fetchall()
            return [dict(r) for r in rows]

    def get_progress(self) -> dict[str, Any]:
        with self._lock:
            state = self.get_state()
            txn_count = self._conn.execute(
                "SELECT COUNT(*) AS c FROM txns"
            ).fetchone()["c"]
            history_rows = self._conn.execute(
                "SELECT COUNT(*) AS c FROM artifact_history"
            ).fetchone()["c"]
            gen_count = self._conn.execute(
                "SELECT COUNT(*) AS c FROM generations"
            ).fetchone()["c"]
            current_gen = self.current_generation() if gen_count else None
            return {
                "phase": state.get("phase", PHASE_INITIALIZING),
                "slot_name": state.get("slot_name"),
                "baseline_lsn": state.get("baseline_lsn"),
                "last_commit_lsn": state.get("last_commit_lsn"),
                "last_message_lsn": state.get("last_message_lsn"),
                "last_xid": int(state["last_xid"]) if state.get("last_xid") else None,
                "captured_transactions": txn_count,
                "history_rows": history_rows,
                "last_error": state.get("last_error") or None,
                "initialized_at": state.get("initialized_at"),
                "generation_count": gen_count,
                "current_generation": current_gen,
                "rebuild_pending_generation": int(
                    state["rebuild_pending_generation"]
                )
                if state.get("rebuild_pending_generation")
                else None,
                "generations": self.get_generations(),
            }
