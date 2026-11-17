"""SQLite 历史库：只增不改的变更日志 + 捕获状态。

关键约束：
- 所有写入在一把进程内锁内串行化，SQLite 开启 WAL；
- 一个源事务的全部行变更与该事务的提交点在同一个 SQLite 事务里落盘，
  保证多行事务整体可见、不会出现半条事务；
- 复制槽的推进位置（last_commit_lsn）与对应数据在同一事务更新，
  崩溃重启后靠 xid 去重，绝不重复、也不丢数据。
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
"""

PHASE_INITIALIZING = "INITIALIZING"
PHASE_STREAMING = "STREAMING"
PHASE_ERROR = "ERROR"


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
        return self.get_phase() == PHASE_STREAMING

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

    # ----------------------------------------------------------------- writes
    def save_baseline(
        self, rows: Iterable[sqlite3.Row | tuple[int, str, str, str]], lsn: str
    ) -> None:
        """整批基线与基线提交点在一个 SQLite 事务里落盘。"""
        now = utc_now_iso()
        lsn_num = lsn_to_num(lsn)
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                self._set_many(
                    {
                        "phase": PHASE_INITIALIZING,
                        "baseline_lsn": lsn,
                        "baseline_lsn_num": lsn_num,
                        "baseline_captured_at": now,
                    }
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
                    )
                    for r in rows
                ]
                self._conn.executemany(
                    """
                    INSERT INTO artifact_history(
                        artifact_no, change_type, service_name, version, sha256,
                        xid, commit_lsn, commit_lsn_num, captured_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    payload,
                )
                self._conn.execute("COMMIT")
            except Exception:
                self._conn.execute("ROLLBACK")
                raise

    def mark_streaming(self) -> None:
        with self._lock:
            self._set_many({"phase": PHASE_STREAMING, "streaming_at": utc_now_iso()})

    def persist_transaction(
        self,
        xid: int,
        commit_lsn: str,
        commit_ts: str | None,
        changes: list[dict[str, Any]],
    ) -> bool:
        """落盘一个已提交源事务。返回 True=新写入，False=重复事务（已跳过）。"""
        lsn_num = lsn_to_num(commit_lsn)
        now = utc_now_iso()
        with self._lock:
            existed = self._conn.execute(
                "SELECT 1 FROM txns WHERE xid=?", (xid,)
            ).fetchone()
            if existed:
                return False
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                self._conn.execute(
                    "INSERT INTO txns(xid, commit_lsn, commit_lsn_num, commit_ts)"
                    " VALUES (?, ?, ?, ?)",
                    (xid, commit_lsn, lsn_num, commit_ts),
                )
                self._conn.executemany(
                    """
                    INSERT INTO artifact_history(
                        artifact_no, change_type, service_name, version, sha256,
                        xid, commit_lsn, commit_lsn_num, captured_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                        )
                        for c in changes
                    ],
                )
                # 槽位推进位置与数据同事务落盘
                self._set_many(
                    {
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
        with self._lock:
            points: list[dict[str, Any]] = []
            baseline_lsn = self._get_state("baseline_lsn")
            if baseline_lsn:
                count = self._conn.execute(
                    "SELECT COUNT(*) AS c FROM artifact_history"
                    " WHERE change_type='BASELINE'"
                ).fetchone()["c"]
                points.append(
                    {
                        "kind": "BASELINE",
                        "lsn": baseline_lsn,
                        "lsn_num": lsn_to_num(baseline_lsn),
                        "xid": None,
                        "commit_ts": self._get_state("baseline_captured_at"),
                        "change_count": count,
                    }
                )
            for row in self._conn.execute(
                """
                SELECT t.xid, t.commit_lsn, t.commit_lsn_num, t.commit_ts,
                       (SELECT COUNT(*) FROM artifact_history h
                         WHERE h.xid = t.xid) AS change_count
                FROM txns t ORDER BY t.commit_lsn_num, t.xid
                """
            ).fetchall():
                points.append(
                    {
                        "kind": "TXN",
                        "lsn": row["commit_lsn"],
                        "lsn_num": row["commit_lsn_num"],
                        "xid": row["xid"],
                        "commit_ts": row["commit_ts"],
                        "change_count": row["change_count"],
                    }
                )
            return points

    def get_directory_at(self, lsn_num: int) -> list[dict[str, Any]]:
        """查看某个提交点（含）为止的整目录：每个编号取最后一条变更。"""
        with self._lock:
            rows = self._conn.execute(
                """
                SELECT artifact_no, service_name, version, sha256, change_type
                FROM artifact_history
                WHERE id IN (
                    SELECT MAX(id) FROM artifact_history
                    WHERE commit_lsn_num <= ?
                    GROUP BY artifact_no
                )
                AND change_type != 'DELETE'
                ORDER BY artifact_no
                """,
                (lsn_num,),
            ).fetchall()
            return [dict(r) for r in rows]

    def get_history_of(self, artifact_no: int) -> list[dict[str, Any]]:
        with self._lock:
            rows = self._conn.execute(
                """
                SELECT artifact_no, change_type, service_name, version, sha256,
                       xid, commit_lsn, commit_lsn_num, captured_at
                FROM artifact_history WHERE artifact_no=?
                ORDER BY id
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
            }
