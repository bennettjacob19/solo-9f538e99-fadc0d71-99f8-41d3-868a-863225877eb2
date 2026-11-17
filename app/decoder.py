"""test_decoding 文本消息解析。

源表结构固定（artifact_no / service_name / version / sha256），因此可以按
列名前缀稳妥切分，避免对空格做通用 split 导致带空格的字符串值被拆坏。

典型消息：
    BEGIN 12345
    COMMIT 12345
    table public.artifacts: INSERT: artifact_no[integer]:1004 service_name[text]:'svc' version[text]:'1.0' sha256[text]:'aa'
    table public.artifacts: UPDATE: artifact_no[integer]:1004 ...
    table public.artifacts: DELETE: artifact_no[integer]:1004
"""
from __future__ import annotations

import re
from typing import Any

COLUMNS = ("artifact_no", "service_name", "version", "sha256")
_OPS = ("INSERT", "UPDATE", "DELETE")
_TABLE_PREFIX_RE = re.compile(r"^table (\S+): (INSERT|UPDATE|DELETE): ")
_COL_RE_TEMPLATE = r"{name}\[[^\]]+\]:(?:""null""|(?:'((?:[^']|'')*)'|-?\d+(?:\.\d+)?))"


def _unescape_sql_literal(value: str) -> str:
    # test_decoding 输出的字符串字面量按 SQL 规则转义单引号
    return value.replace("''", "'")


def _parse_columns(payload: str, table_name: str) -> dict[str, Any] | None:
    if not payload.startswith("table "):
        return None
    head = _TABLE_PREFIX_RE.match(payload)
    if not head:
        return None
    qualified_table, op = head.group(1), head.group(2)
    if not qualified_table.endswith("." + table_name):
        return None  # 只捕获目标表，其他表/DDL 一律忽略

    body = payload[head.end():]
    result: dict[str, Any] = {"change_type": op}

    # 逐列定位，用下一个列名前缀做边界，保证值里的空格/冒号不会被误切
    starts: list[tuple[int, str]] = []
    for name in COLUMNS:
        marker = name + "["
        idx = body.find(marker)
        if idx >= 0:
            starts.append((idx, name))
    starts.sort()

    for pos, (start, name) in enumerate(starts):
        end = starts[pos + 1][0] if pos + 1 < len(starts) else len(body)
        token = body[start:end].strip()
        # token 形如  name[type]:value
        _, _, raw = token.partition("]:")
        raw = raw.strip()
        if raw == "null":
            result[name] = None
        elif raw.startswith("'") and raw.endswith("'"):
            result[name] = _unescape_sql_literal(raw[1:-1])
        else:
            result[name] = int(raw) if name == "artifact_no" else raw

    if "artifact_no" not in result:
        return None
    return result


def parse_message(data: str, table_name: str) -> dict[str, Any] | None:
    """返回结构化消息：
    {"kind": "BEGIN", "xid": int}
    {"kind": "COMMIT", "xid": int}
    {"kind": "CHANGE", **change}
    None = 与本任务无关（其他表、DDL、序列等）。
    """
    if data.startswith("BEGIN "):
        return {"kind": "BEGIN", "xid": int(data.split()[1])}
    if data.startswith("COMMIT "):
        return {"kind": "COMMIT", "xid": int(data.split()[1])}
    if data.startswith("table "):
        change = _parse_columns(data, table_name)
        if change is not None:
            change["kind"] = "CHANGE"
            return change
    return None
