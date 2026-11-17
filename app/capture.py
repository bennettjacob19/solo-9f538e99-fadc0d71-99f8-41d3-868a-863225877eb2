"""捕获工作线程：一致基线 + test_decoding 逻辑复制 + 崩溃续接。

一致性与不丢事务的原理
----------------------
1. 在一个普通事务里调用 pg_create_logical_replication_slot 建槽并
   pg_export_snapshot 导出快照，槽的起始 LSN 对应一个精确一致性点；
2. 另开一条普通连接 SET TRANSACTION SNAPSHOT 复用该快照做整表基线；
3. 从槽的 LSN 开始读逻辑变更流。快照时刻"正在进行中"的事务不会进基线，
   而它们提交后必然出现在复制流里——基线前开始、基线后提交的事务不会漏；
4. 每个源事务先整体写 SQLite（数据 + 提交点同事务），成功后才通过
   复制协议 send_feedback 推进源端槽位。任何时刻强杀：未落盘的事务槽位
   不会前进，重启后重放并按 xid 幂等去重，已保存历史与续接位置永远一致。
"""
from __future__ import annotations

import logging
import threading
from typing import Any

import psycopg2
import psycopg2.extras

from .config import Settings
from .decoder import parse_message
from .source import SourceDB
from .storage import PHASE_STREAMING, HistoryStore

log = logging.getLogger("capture")


class CaptureWorker:
    def __init__(self, store: HistoryStore, source: SourceDB,
                 settings: Settings) -> None:
        self._store = store
        self._source = source
        self._settings = settings
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._current_xid: int | None = None
        self._current_changes: list[dict[str, Any]] = []

    # ----------------------------------------------------------------- life
    def start(self) -> None:
        self._thread = threading.Thread(
            target=self._safe_loop, name="capture-worker", daemon=True
        )
        self._thread.start()

    def stop(self, timeout: float = 10.0) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout)

    def _safe_loop(self) -> None:
        try:
            self._run()
        except Exception as exc:  # 线程兜底：错误暴露到 /api/progress
            log.exception("capture worker crashed: %s", exc)
            try:
                self._store.mark_error(f"capture worker crashed: {exc}")
            except Exception:
                pass

    # ----------------------------------------------------------------- main
    def _run(self) -> None:
        self._source.ensure_schema()
        state = self._store.get_state()
        if state.get("baseline_lsn"):
            log.info("resuming capture from slot %s", self._settings.slot_name)
        else:
            self._initialize()
        self._stream_forever()

    def _start_inflight_hook(self):
        """测试钩子：开一个源事务并保持打开，用于构造跨基线事务。

        返回该事务的独立连接；调用方在基线落盘后 commit。默认禁用。
        """
        import psycopg2

        no = self._settings.inflight_hook_no
        if not no:
            return None
        conn = psycopg2.connect(self._settings.pg_dsn)
        conn.autocommit = False
        with conn.cursor() as cur:
            cur.execute("BEGIN")
            cur.execute(
                "INSERT INTO artifacts(artifact_no, service_name, version,"
                " sha256) VALUES (%s, %s, %s, %s)",
                (no, "inflight-hook-svc", "0.0.9",
                 "a" * 64),
            )
        log.info(
            "TEST hook: inflight transaction opened on artifact_no=%s,"
            " holding for %.1fs before baseline",
            no, self._settings.inflight_hook_sleep_s,
        )
        return conn

    def _initialize(self) -> None:
        """首次基线。初始化未完成前 phase 始终为 INITIALIZING，网页不开放历史。"""
        self._store.mark_initializing(self._settings.slot_name)

        if self._settings.init_sleep_ms:  # 测试钩子：拉长初始化窗口
            self._stop.wait(self._settings.init_sleep_ms / 1000.0)

        slot = self._settings.slot_name
        inflight_conn = None
        try:
            if self._source.slot_exists(slot):
                # 历史库为空但槽已存在（例如初始化中途被杀）：废弃旧槽重来，
                # 因为基线数据从未落盘，旧槽没有任何需要保留的已确认位置。
                log.warning(
                    "orphan slot %s found, dropping for clean baseline", slot
                )
                with self._source.connect() as conn, conn.cursor() as cur:
                    cur.execute("SELECT pg_drop_replication_slot(%s)", (slot,))

            # 1) 先建槽并导出一致性快照（拿到精确的基线 LSN）
            consistent_lsn, slot_conn, snapshot_id = (
                self._source.create_slot_with_snapshot(slot)
            )
            try:
                # 2) 测试钩子：快照点之后、基线读取之前打开并持有源事务。
                #    它的 xid 对该快照不可见（不进基线），提交后又必然
                #    出现在从槽 LSN 开始的逻辑流里（不遗漏）。
                inflight_conn = self._start_inflight_hook()
                if inflight_conn is not None:
                    self._stop.wait(self._settings.inflight_hook_sleep_s)

                # 3) 复用快照读基线整目录
                rows = self._source.baseline_rows_using_snapshot(snapshot_id)
                # 整批基线在一个 SQLite 事务里落盘；落盘前不向源端确认任何东西
                self._store.save_baseline(rows, consistent_lsn)
                log.info(
                    "baseline saved: %d rows at %s (snapshot %s)",
                    len(rows), consistent_lsn, snapshot_id,
                )
            finally:
                slot_conn.close()
            self._store.mark_streaming()
        finally:
            # 基线已落盘：此刻才提交在途事务。它开始于快照点之后，
            # 不会出现在基线里，必须由逻辑复制流补捕获。
            if inflight_conn is not None:
                try:
                    inflight_conn.commit()
                    log.info(
                        "TEST hook: inflight transaction committed after baseline"
                    )
                finally:
                    inflight_conn.close()

    # ------------------------------------------------------------- streaming
    def _stream_forever(self) -> None:
        slot = self._settings.slot_name
        backoff = max(1, self._settings.reconnect_backoff_s)
        transient_streak = 0  # 连续瞬断次数；未超阈值不降级为 ERROR
        while not self._stop.is_set():
            repl_conn = None
            try:
                if not self._source.slot_exists(slot):
                    raise RuntimeError(
                        f"replication slot '{slot}' missing on source"
                    )
                repl_conn = psycopg2.connect(
                    self._settings.pg_dsn,
                    connection_factory=psycopg2.extras.LogicalReplicationConnection,
                )
                # ReplicationConnection 固定 autocommit，属性只读
                cur = repl_conn.cursor()
                cur.start_replication(
                    slot_name=slot,
                    options={"include-xids": "1", "include-timestamp": "1"},
                )
                log.info("replication stream started on slot %s", slot)
                transient_streak = 0
                # 重连成功即恢复正常服务状态，并清空历史断线提示
                if self._store.get_phase() != PHASE_STREAMING:
                    self._store.mark_streaming()
                else:
                    self._store.clear_error()
                cur.consume_stream(
                    lambda msg: self._handle_message(msg),
                    keepalive_interval=self._settings.status_interval_s,
                )
            except Exception as exc:
                if self._stop.is_set():
                    break
                transient_streak += 1
                log.warning(
                    "replication stream broken (streak=%d): %r; reconnecting",
                    transient_streak, exc, exc_info=transient_streak >= 3,
                )
                # 应用刚重启时旧 walsender 释放槽位有短暂延迟，属可恢复瞬断：
                # 保留 STREAMING，历史查询继续可用；连续失败才降级为 ERROR。
                if transient_streak >= 3:
                    self._store.mark_error(
                        f"replication stream broken: {exc}"
                    )
                self._stop.wait(backoff)
            finally:
                if repl_conn is not None:
                    try:
                        repl_conn.close()
                    except Exception:
                        pass

    def _handle_message(self, msg: Any) -> None:
        if self._stop.is_set():
            raise psycopg2.OperationalError("shutting down")

        if msg.data_start == 0:  # Primary keepalive 消息无载荷
            # 不主动确认：只随已落盘事务推进，绝不确认尚未落盘的更改
            return

        data = msg.payload.decode("utf-8")
        parsed = parse_message(data, self._settings.table_name)
        if parsed is None:
            # 无关消息（DDL/其他表/序列…）：不回送任何 feedback。
            # 槽位只随"已落盘事务"通过 send_feedback 推进，
            # 保证任何尚未落盘的更改都不会向源库确认。
            return

        kind = parsed["kind"]
        if kind == "BEGIN":
            self._current_xid = parsed["xid"]
            self._current_changes: list[dict[str, Any]] = []
        elif kind == "CHANGE":
            self._current_changes.append(parsed)
        elif kind == "COMMIT":
            # test_decoding: COMMIT 行本身不带 LSN，消息 wal_end 即该事务
            # 提交记录结束位置，用作该提交点的 LSN。
            commit_lsn = msg.wal_end
            self._finish_transaction(parsed["xid"], commit_lsn, data, msg)
        self._store.update_last_message_lsn(str(msg.wal_end))

    def _finish_transaction(self, xid: int, commit_lsn: int,
                            raw_commit_msg: str, msg: Any) -> None:
        changes = getattr(self, "_current_changes", [])
        # include-timestamp=1 时形如 "COMMIT 12345 (at 2026-11-16 10:00:00.0+00)"
        commit_ts = None
        marker = "(at "
        if marker in raw_commit_msg:
            commit_ts = raw_commit_msg.split(marker, 1)[1].rstrip(")")

        lsn_text = f"{commit_lsn >> 32:X}/{commit_lsn & 0xFFFFFFFF:08X}"
        written = self._store.persist_transaction(
            xid, lsn_text, commit_ts, changes
        )
        if written:
            log.info(
                "txn %s persisted at %s with %d change(s)",
                xid, lsn_text, len(changes),
            )
        else:
            log.info("txn %s replayed after restart, skipped (idempotent)", xid)

        # 数据已落盘（或此前已落盘）——现在才通过复制协议向源端确认，
        # 推进该槽的 confirmed_flush_lsn。绝不在落盘前发送 feedback。
        msg.cursor.send_feedback(flush_lsn=commit_lsn)
        self._current_changes = []
        self._current_xid = None
