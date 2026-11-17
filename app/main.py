"""FastAPI 历史查询网页：源目录成组修改、提交点查询、捕获进度、重建捕获。

首次基线未完成（不存在任何已完成世代）时，所有历史查询接口返回 503，
不开放尚未完整的历史；一旦有世代落盘，即使复制槽缺失（ERROR）或正在
重建（REBUILDING），旧段历史仍可正常回看。源目录修改走源库自身事务，
不受捕获状态限制。
"""
from __future__ import annotations

import logging
import os
from contextlib import asynccontextmanager
from typing import Annotated, Literal, Union

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse, JSONResponse
from pydantic import BaseModel, Field, field_validator

from .capture import CaptureWorker, RebuildError
from .config import settings
from .source import SourceDB, SourceError
from .storage import HistoryStore, lsn_to_num

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
log = logging.getLogger("web")

SHA256_RE = r"^[0-9a-fA-F]{64}$"


def _check_sha(v: str) -> str:
    import re

    if not re.match(SHA256_RE, v):
        raise ValueError("sha256 必须是 64 位十六进制")
    return v


class _Columns(BaseModel):
    """增/改共用的四列固定字段，严格拒绝额外列。"""

    artifact_no: int = Field(gt=0)
    service_name: str = Field(min_length=1, max_length=200)
    version: str = Field(min_length=1, max_length=100)
    sha256: str

    @field_validator("sha256")
    @classmethod
    def _sha(cls, v: str) -> str:
        return _check_sha(v)


class InsertOp(_Columns):
    op: Literal["insert"]


class UpdateOp(_Columns):
    op: Literal["update"]


class DeleteOp(BaseModel):
    op: Literal["delete"]
    artifact_no: int = Field(gt=0)


AnyOp = Annotated[
    Union[InsertOp, UpdateOp, DeleteOp], Field(discriminator="op")
]


class BatchRequest(BaseModel):
    operations: list[AnyOp] = Field(min_length=1)


store = HistoryStore(settings.sqlite_path)
source = SourceDB(settings)
worker = CaptureWorker(store, source, settings)


@asynccontextmanager
async def lifespan(app: FastAPI):
    worker.start()
    try:
        yield
    finally:
        worker.stop()
        store.close()


app = FastAPI(title="制品目录历史查询", lifespan=lifespan)


def _require_ready() -> None:
    if not store.is_ready():
        progress = store.get_progress()
        raise HTTPException(
            status_code=503,
            detail={
                "message": "捕获初始化尚未完成，历史暂不开放",
                "phase": progress.get("phase"),
                "last_error": progress.get("last_error"),
            },
        )


# --------------------------------------------------------------------- 页面
@app.get("/", include_in_schema=False)
def index() -> FileResponse:
    return FileResponse(
        os.path.join(os.path.dirname(__file__), "static", "index.html")
    )


# --------------------------------------------------------------------- 进度
@app.get("/api/progress")
def api_progress() -> dict:
    return store.get_progress()


# --------------------------------------------------------------- 源目录读写
@app.get("/api/source/artifacts")
def api_source_list() -> dict:
    return {"artifacts": source.list_artifacts()}


@app.post("/api/source/operations")
def api_batch_write(req: BatchRequest) -> dict:
    """成组修改源目录；整组在一个源事务内提交，失败整体回滚。"""
    payload = [op.model_dump() for op in req.operations]
    try:
        result = source.apply_operations(payload)
    except SourceError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"源库错误: {exc}")
    result["operations"] = len(payload)
    return result


# -------------------------------------------------------------- 重建捕获
@app.post("/api/capture/rebuild")
def api_rebuild_capture() -> dict:
    """复制槽被人工删除后的“重建捕获”。

    保留全部旧提交点与目录历史：确认旧槽缺失后，在新世代对当前源目录
    建立新的一致基线并接续逻辑复制；两段历史之间的未覆盖区间在提交点
    列表中以 GAP 显式分开。重建期间普通 SQL 可继续提交。
    """
    try:
        generation = worker.request_rebuild()
    except RebuildError as exc:
        # 槽仍存在 / 重建进行中 / 尚无基线：属于前置条件不满足
        raise HTTPException(status_code=409, detail=str(exc))
    return {
        "message": "重建捕获已开始：保留旧历史，新基线写入新世代",
        "pending_generation": generation,
    }


# ---------------------------------------------------------------- 历史查询
@app.get("/api/points")
def api_points() -> dict:
    _require_ready()
    return {"points": store.get_points()}


@app.get("/api/generations")
def api_generations() -> dict:
    """各捕获世代及相邻世代之间的未覆盖区间（断档）。"""
    _require_ready()
    return {"generations": store.get_generations()}


@app.get("/api/directory")
def api_directory(point: str) -> JSONResponse:
    """按提交点（LSN，如 0/16B3748）查看该时刻的整张目录。

    提交点按世代隔离：旧点只按旧段回看，新世代的点不会混入断档前的
    残留值。GAP（未覆盖区间）不是可回看的提交点。
    """
    _require_ready()
    try:
        lsn_num = lsn_to_num(point)
    except (ValueError, IndexError):
        raise HTTPException(status_code=400, detail="point 必须是 LSN，形如 0/16B3748")
    points = store.get_points()
    match = next(
        (p for p in points if p["kind"] != "GAP" and p["lsn_num"] == lsn_num),
        None,
    )
    if match is None:
        gap = next((p for p in points if p["kind"] == "GAP"), None)
        if gap is not None:
            raise HTTPException(
                status_code=404,
                detail=(
                    "该 LSN 落在复制槽删除造成的未覆盖区间(GAP)内，"
                    "无法回看；请选择 GAP 之前的旧世代提交点或之后的新基线点"
                ),
            )
        raise HTTPException(
            status_code=404,
            detail="该 LSN 不是已捕获的提交点，请先在 /api/points 中选择",
        )
    return JSONResponse(
        {
            "point": point,
            "point_lsn_num": lsn_num,
            "generation": match["generation"],
            "artifacts": store.get_directory_at(lsn_num, match["generation"]),
        }
    )


@app.get("/api/history/{artifact_no}")
def api_artifact_history(artifact_no: int) -> dict:
    """单个制品编号的完整变更链；删除后重用编号，旧历史也全部保留。"""
    _require_ready()
    return {
        "artifact_no": artifact_no,
        "history": store.get_history_of(artifact_no),
    }
