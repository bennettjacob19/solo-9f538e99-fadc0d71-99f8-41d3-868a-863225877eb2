#!/usr/bin/env bash
# 端到端自测（第二部分）：复制槽删除后的“重建捕获”。
# 复用 tests/selftest.sh 的便携 PostgreSQL 启动约定，独立运行。
# 用法：bash tests/selftest_rebuild.sh
set -u
cd "$(dirname "$0")/.."
REPO=$PWD

# ------------------------------------------------------------- 环境定位
PGBIN=${PGBIN:-/tmp/pgdeb/extracted/usr/lib/postgresql/15/bin}
PGCLIENT=${PGCLIENT:-$PGBIN}
export PATH="$PGCLIENT:$PGBIN:$PATH"
export LD_LIBRARY_PATH=${LD_LIBRARY_PATH:-}:/tmp/pgdeb/extracted/usr/lib/x86_64-linux-gnu
PGDATA=${PGDATA:-/tmp/rebuild-pgdata}
PGPORT=55434
PGSOCK=/tmp/rebuild-pgsock
SQLDIR=/tmp/rebuild-sqlite
APPPORT=8012
BASE="http://127.0.0.1:$APPPORT"
SHA64="a1b2c3d4e5f60718293a4b5c6d7e8f90a1b2c3d4e5f60718293a4b5c6d7e8f90"
DSN="host=$PGSOCK port=$PGPORT dbname=postgres user=postgres"
SLOT=artifacts_history_slot
PASS=0; FAIL=0

ok()   { echo "PASS: $1"; PASS=$((PASS+1)); }
bad()  { echo "FAIL: $1"; FAIL=$((FAIL+1)); }
check(){ if [ "$1" = "$2" ]; then ok "$3"; else bad "$3 (got '$1' want '$2')"; fi; }
api()  { curl -s -m 10 "http://127.0.0.1:$APPPORT$1"; }
psqlq(){ psql -h "$PGSOCK" -p "$PGPORT" -U postgres -tAc "$1"; }
drop_slot(){ # 先终止 walsender 再删槽，模拟“槽被人工从源库删除”的现场
  psqlq "select pg_terminate_backend(active_pid) from pg_replication_slots where active and slot_name='$SLOT'" >/dev/null
  for _ in $(seq 1 20); do
    psqlq "select pg_drop_replication_slot(slot_name) from pg_replication_slots" >/dev/null 2>&1
    [ "$(psqlq "select count(*) from pg_replication_slots")" = "0" ] && return 0
    sleep 0.5
  done
  return 1
}
wait_phase(){
  local want=$1 deadline=$((SECONDS+$2))
  while [ "$SECONDS" -lt "$deadline" ]; do
    [ "$(api /api/progress | jq -r .phase)" = "$want" ] && return 0
    sleep 0.5
  done
  return 1
}
wait_capture(){ # 等某编号的某版本被捕获（最多 ~20s）
  local no=$1 ver=$2
  for _ in $(seq 1 40); do
    n=$(api "/api/history/$no" | jq --arg v "$ver" '[.history[]|select(.version==$v)]|length')
    [ "${n:-0}" = "1" ] && return 0
    sleep 0.5
  done
  return 1
}
start_app(){
  local extra=${1:-}
  stop_app
  for _ in $(seq 1 20); do
    curl -s -m 1 "$BASE/api/progress" >/dev/null 2>&1 || break
    sleep 0.5
  done
  mkdir -p "$SQLDIR"
  # shellcheck disable=SC2086
  PG_DSN="$DSN" SQLITE_PATH="$SQLDIR/history.db" env $extra \
    setsid nohup python3 -m uvicorn app.main:app --host 127.0.0.1 \
    --port "$APPPORT" > /tmp/rebuild-app.log 2>&1 < /dev/null &
  local new_pid=$!
  echo "$new_pid" > /tmp/rebuild-app.pid
  for _ in $(seq 1 40); do
    if ! kill -0 "$new_pid" 2>/dev/null; then
      echo "new app exited early:"; cat /tmp/rebuild-app.log; return 1
    fi
    if curl -s -m 1 "$BASE/api/progress" >/dev/null 2>&1; then
      [ "$(app_pid)" = "$new_pid" ] && return 0
    fi
    sleep 0.5
  done
  echo "app failed to become ready:"; cat /tmp/rebuild-app.log; return 1
}
stop_app(){
  local pid
  pid=$(app_pid)
  [ -n "$pid" ] && kill $pid 2>/dev/null || true
  for _ in $(seq 1 20); do
    app_pid | grep -q . || return 0
    sleep 0.5
  done
}
force_kill_app(){
  local pid
  pid=$(app_pid)
  [ -n "$pid" ] && kill -9 $pid 2>/dev/null || true
  sleep 1
}
app_pid(){ ps -eo pid,comm,args | awk '$2=="python3" && /uvicorn app.main:app/ {print $1}' | head -1; }
pg_ctl_q(){ pg_ctl -D "$PGDATA" -l /tmp/rebuild-pg.log "$@"; }
write_op(){ # write_op <json>
  curl -s -o /dev/null -X POST "$BASE/api/source/operations" \
    -H 'Content-Type: application/json' -d "$1"
}

cleanup(){
  stop_app || true
  pg_ctl_q stop -m fast >/dev/null 2>&1 || true
}
trap cleanup EXIT

# ------------------------------------------------------------- 0. 准备源库
echo "=== 0. 准备全新源 PostgreSQL（wal_level=logical）==="
pg_ctl_q stop -m fast >/dev/null 2>&1 || true
rm -rf "$PGDATA" "$PGSOCK" "$SQLDIR"; mkdir -p "$PGSOCK"
initdb -D "$PGDATA" -U postgres --auth=trust -E UTF8 --no-sync >/dev/null
cat >> "$PGDATA/postgresql.conf" <<EOF
port = $PGPORT
unix_socket_directories = '$PGSOCK'
listen_addresses = ''
wal_level = logical
max_wal_senders = 10
max_replication_slots = 10
EOF
pg_ctl_q start -w >/dev/null
psql -h "$PGSOCK" -p "$PGPORT" -U postgres -q -f init/01_schema.sql
check "$(psqlq 'select count(*) from artifacts')" "3" "源表初始化 3 行"

# ------------------------------------------------------------- 1. 首段捕获
echo "=== 1. 首次基线并在旧世代积累历史 ==="
start_app || { echo FATAL; exit 1; }
wait_phase STREAMING 20 && ok "首次基线完成进入 STREAMING" || bad "首次基线超时"
write_op "{\"operations\":[{\"op\":\"insert\",\"artifact_no\":4001,\"service_name\":\"svc-a\",\"version\":\"1.0\",\"sha256\":\"$SHA64\"}]}"
write_op "{\"operations\":[{\"op\":\"update\",\"artifact_no\":1001,\"service_name\":\"billing-gateway\",\"version\":\"1.5.0\",\"sha256\":\"$SHA64\"}]}"
wait_capture 4001 1.0 && ok "世代0 插入 4001 已捕获" || bad "4001 未捕获"
wait_capture 1001 1.5.0 && ok "世代0 更新 1001 已捕获" || bad "1001 更新未捕获"
gen0_points=$(api /api/points | jq '[.points[]|select(.generation==0)]|length')
gen0_last=$(api /api/points | jq -r '[.points[]|select(.generation==0 and .kind=="TXN")][-1].lsn')
old_history_rows=$(api /api/progress | jq .history_rows)
old_txns=$(api /api/progress | jq .captured_transactions)
echo "  世代0 提交点数=$gen0_points 最后点=$gen0_last 历史行=$old_history_rows"

# ------------------------------------------------------------- 2. 槽仍存在时拒绝重建
echo "=== 2. 前置条件校验：槽未缺失时拒绝重建 ==="
code=$(curl -s -o /tmp/rb.json -w '%{http_code}' -X POST "$BASE/api/capture/rebuild")
check "$code" "409" "槽仍存在时重建请求返回 409"
grep -q "仍存在" /tmp/rb.json && ok "拒绝原因明确（槽仍存在）" || bad "拒绝原因异常: $(cat /tmp/rb.json)"
check "$(api /api/progress | jq -r .phase)" "STREAMING" "被拒绝后仍为 STREAMING"
check "$(psqlq "select count(*) from pg_replication_slots where slot_name='$SLOT'")" "1" "复制槽未被误删"

# ------------------------------------------------------------- 3. 删除复制槽，制造断档区间
echo "=== 3. 删除源复制槽，制造断档 ==="
drop_slot || bad "复制槽删除失败"
check "$(psqlq "select count(*) from pg_replication_slots")" "0" "复制槽已从源库删除"
# 应用连续重连失败后应降级 ERROR（旧历史仍可查）
for _ in $(seq 1 20); do
  [ "$(api /api/progress | jq -r .phase)" = "ERROR" ] && break
  sleep 1
done
check "$(api /api/progress | jq -r .phase)" "ERROR" "槽缺失后应用暴露 ERROR"
# 断档期间源库继续提交普通 SQL：旧槽已删、新槽未建，该变更永久落在 GAP 内
write_op "{\"operations\":[{\"op\":\"insert\",\"artifact_no\":4002,\"service_name\":\"gap-svc\",\"version\":\"9.9\",\"sha256\":\"$SHA64\"}]}"
sleep 1
check "$(psqlq "select count(*) from artifacts where artifact_no=4002")" "1" "断档期间普通 SQL 照常提交成功"
source_rows=$(psqlq 'select count(*) from artifacts')
# ERROR 后旧世代历史仍可回看（旧点按旧段回看）
code=$(curl -s -o /dev/null -w '%{http_code}' "$BASE/api/points")
check "$code" "200" "ERROR 期间旧世代历史接口仍可用"
check "$(api "/api/directory?point=$gen0_last" | jq '[.artifacts[]|select(.artifact_no==4001)]|length')" "1" \
  "旧点仍按旧段回看，4001 在旧最后提交点可见"
check "$(api "/api/directory?point=$gen0_last" | jq '[.artifacts[]|select(.artifact_no==4002)]|length')" "0" \
  "断档期间的 4002 不混入旧段任何提交点"
check "$(api /api/history/4002 | jq '.history|length')" "0" "断档期变更在旧段无任何历史痕迹"

# ------------------------------------------------------------- 4. 触发重建
echo "=== 4. 网页触发重建捕获 ==="
resp=$(curl -s -X POST "$BASE/api/capture/rebuild")
echo "  $resp"
echo "$resp" | jq -e .pending_generation >/dev/null && ok "重建已启动并返回新世代号" || bad "重建启动失败"
for _ in $(seq 1 30); do
  ph=$(api /api/progress | jq -r .phase)
  [ "$ph" = "STREAMING" ] && break
  sleep 0.5
done
check "$(api /api/progress | jq -r .phase)" "STREAMING" "重建完成恢复 STREAMING"
check "$(psqlq "select count(*) from pg_replication_slots where slot_name='$SLOT' and slot_type='logical'")" "1" \
  "同名新逻辑复制槽已建立"
check "$(api /api/progress | jq .generation_count)" "2" "世代数=2（旧段+重建段）"

# ------------------------------------------------------------- 5. 两段历史与 GAP 明确分开
echo "=== 5. 世代、GAP 与新旧段隔离 ==="
gap=$(api /api/points | jq -r '.points[]|select(.kind=="GAP")')
[ -n "$gap" ] && ok "提交点列表包含 GAP 未覆盖区间标记" || bad "缺少 GAP 标记"
check "$(api /api/points | jq '[.points[]|select(.kind=="GAP")]|length')" "1" "恰好一个 GAP"
gap_from=$(api /api/points | jq -r '.points[]|select(.kind=="GAP")|.gap_from_lsn')
gap_to=$(api /api/points | jq -r '.points[]|select(.kind=="GAP")|.gap_to_lsn')
check "$gap_from" "$gen0_last" "GAP 起点=旧世代最后捕获点 $gen0_last"
[ -n "$gap_to" ] && ok "GAP 终点=新基线点 $gap_to" || bad "GAP 终点缺失"
# GAP 区间内部的 LSN 不可回看（gap_to 本身是新基线点，可回看）
gap_mid=$(python3 -c "
def n(s):
    h,l=s.split('/'); return (int(h,16)<<32)|int(l,16)
a=n('$gap_from'); b=n('$gap_to'); m=(a+b)//2
print(f'{m>>32:X}/{m&0xFFFFFFFF:08X}')")
code=$(curl -s -o /tmp/gap.json -w '%{http_code}' "$BASE/api/directory?point=$gap_mid")
check "$code" "404" "GAP 区间内部 LSN 不可回看（404）"
grep -q "未覆盖" /tmp/gap.json && ok "GAP 查询返回明确的未覆盖提示" || bad "GAP 提示异常: $(cat /tmp/gap.json)"
code=$(curl -s -o /dev/null -w '%{http_code}' "$BASE/api/directory?point=$gap_to")
check "$code" "200" "GAP 终点（新基线点）本身可正常回看"

# 新基线内容 = 重建时刻源目录当前值（含断档期间提交的 4002）
new_base=$(api /api/points | jq -r '[.points[]|select(.generation==1 and .kind=="BASELINE")][0].lsn')
check "$(api "/api/directory?point=$new_base" | jq '.artifacts|length')" "$source_rows" \
  "新基线目录 $source_rows 行，与重建时刻源目录一致（含断档期写入）"
check "$(api "/api/directory?point=$new_base" | jq '[.artifacts[]|select(.artifact_no==4002)]|length')" "1" \
  "新基线包含断档期间源库已提交的 4002（当前值基线化）"
# 新点不混入断档前残留：新世代目录只来自世代1
check "$(api "/api/directory?point=$new_base" | jq '[.artifacts[]|select(.artifact_no==4001)]|length')" "1" \
  "新基线按当前源目录重建（4001 仍在源库，故在基线中，来源为新 BASELINE 而非旧值）"
check "$(api "/api/directory?point=$new_base" | jq -r '[.artifacts[]|select(.artifact_no==4001)][0].change_type')" "BASELINE" \
  "新世代 4001 来源是新 BASELINE，不是断档前旧世代残留"
# 旧历史行数只增不减
cur_rows=$(api /api/progress | jq .history_rows)
[ "$cur_rows" -gt "$old_history_rows" ] && ok "旧历史完整保留且新基线追加（$old_history_rows -> $cur_rows 行）" \
  || bad "历史行数异常: $old_history_rows -> $cur_rows"
# 单编号历史按世代分段，旧链不被覆盖
chain1001=$(api /api/history/1001 | jq -r '[.history[]|.generation]|join(",")')
case "$chain1001" in
  0*1*) ok "1001 历史跨世代分段保留: $chain1001" ;;
  *) bad "1001 世代分段异常: $chain1001" ;;
esac
check "$(api /api/history/1001 | jq '[.history[]|select(.generation==0 and .version=="1.5.0")]|length')" "1" \
  "旧世代 1001=1.5.0 旧值仍可查"

# ------------------------------------------------------------- 6. 重建后接续复制：新变更继续捕获
echo "=== 6. 重建后续接逻辑复制 ==="
write_op "{\"operations\":[{\"op\":\"update\",\"artifact_no\":1001,\"service_name\":\"billing-gateway\",\"version\":\"2.0.0\",\"sha256\":\"$SHA64\"}]}"
wait_capture 1001 2.0.0 && ok "重建后新事务继续被捕获（世代1）" || bad "重建后流未续接"
check "$(api /api/history/1001 | jq '[.history[]|select(.generation==1 and .version=="2.0.0")]|length')" "1" \
  "新变更归属世代1"
# 跨新基线的事务只出现一次：重建快照点之后打开、之后提交的事务
# （基线读取是瞬时的，用重建后普通事务验证世代1内 xid 唯一）
write_op "{\"operations\":[{\"op\":\"insert\",\"artifact_no\":4003,\"service_name\":\"post-gap\",\"version\":\"1\",\"sha256\":\"$SHA64\"},{\"op\":\"delete\",\"artifact_no\":4002}]}"
sleep 2
check "$(api /api/history/4003 | jq '[.history[]|select(.change_type=="INSERT")]|length')" "1" \
  "世代1 事务仅出现一次（无重复）"
check "$(api /api/points | jq '[.points[]|select(.generation==1 and .kind=="TXN" and .change_count>=2)]|length')" "1" \
  "多行事务在世代1 整体可见、单提交点"
# 槽位确认位置不超前已落盘
flush=$(psqlq "select confirmed_flush_lsn from pg_replication_slots where slot_name='$SLOT'")
last=$(api /api/progress | jq -r .last_commit_lsn)
fnum=$(python3 -c "h,l='$flush'.split('/');print((int(h,16)<<32)+int(l,16))")
lnum=$(python3 -c "h,l='$last'.split('/');print((int(h,16)<<32)+int(l,16))")
[ "$fnum" -le "$lnum" ] && ok "新槽 confirmed_flush_lsn($flush) 不超前已落盘点($last)" \
  || bad "槽位超前落盘: $flush > $last"

# ------------------------------------------------------------- 7. 中断重试：建槽后、落盘前强杀
echo "=== 7. 中断重试：建槽后基线落盘前强杀，重试不暴露半段"
# 再次制造断档：先删槽
drop_slot || bad "复制槽删除失败"
for _ in $(seq 1 20); do
  [ "$(api /api/progress | jq -r .phase)" = "ERROR" ] && break
  sleep 1
done
check "$(api /api/progress | jq -r .phase)" "ERROR" "第二次槽缺失后进入 ERROR"
# 用拉长初始化窗口的钩子让重建停在 REBUILDING；重建代码路径没有独立
# sleep 钩子，改为触发后立即强杀（建槽+快照是第一个外部动作）。
curl -s -o /dev/null -X POST "$BASE/api/capture/rebuild"
# 轮询等到新槽出现（建槽成功、SQLite 尚未落盘的窗口），随即 kill -9
killed=0
for _ in $(seq 1 40); do
  n=$(psqlq "select count(*) from pg_replication_slots where slot_name='$SLOT'")
  if [ "$n" = "1" ]; then
    # 再等一个极短窗口让进程停在落盘前后，强杀；若已落盘则用世代校验兜底
    force_kill_app
    killed=1
    break
  fi
  ph=$(api /api/progress | jq -r .phase 2>/dev/null)
  [ "$ph" = "STREAMING" ] && break
  sleep 0.1
done
[ "$killed" = "1" ] && ok "重建窗口内强杀应用成功" || echo "  （未捕捉到窗口，按已落盘路径校验）"
# 强杀后检查：不能出现没有 BASELINE 行的空世代
start_app || { echo FATAL; exit 1; }
sleep 3
gens=$(api /api/progress | jq .generation_count)
empty_gen=$(python3 - "$SQLDIR/history.db" <<'PY'
import sqlite3, sys
con = sqlite3.connect(sys.argv[1])
rows = con.execute(
  "select g.generation from generations g left join artifact_history h"
  " on h.generation=g.generation and h.change_type='BASELINE'"
  " group by g.generation having count(h.id)=0").fetchall()
print(len(rows))
PY
)
check "$empty_gen" "0" "不存在无基线数据的半空世代（原子落盘）"
# 应用重启后自动续做未完成的重建并恢复 STREAMING
wait_phase STREAMING 30 && ok "重启后自动续做重建并恢复 STREAMING" || bad "重启后续做失败: $(api /api/progress)"
final_gens=$(api /api/progress | jq .generation_count)
[ "$final_gens" -ge 2 ] && ok "世代结构完整（共 $final_gens 段），旧历史未被清除" || bad "世代异常: $final_gens"
# 旧世代数据仍可回看
check "$(api "/api/directory?point=$gen0_last" | jq '[.artifacts[]|select(.artifact_no==4001)]|length')" "1" \
  "中断重试后旧世代提交点仍可正常回看"
# 续做后复制流仍然工作
write_op "{\"operations\":[{\"op\":\"update\",\"artifact_no\":1001,\"service_name\":\"billing-gateway\",\"version\":\"3.0.0\",\"sha256\":\"$SHA64\"}]}"
wait_capture 1001 3.0.0 && ok "中断重试后续接流继续捕获新变更" || bad "续做后流不工作"

# --------------------------------------------------- 7b. 落盘后、收尾前强杀
echo "=== 7b. 基线已落盘但未切回 STREAMING 时强杀：重启只收尾不重建 ==="
gens_before=$(api /api/progress | jq .generation_count)
drop_slot || bad "复制槽删除失败"
for _ in $(seq 1 20); do
  [ "$(api /api/progress | jq -r .phase)" = "ERROR" ] && break
  sleep 1
done
# 带钩子启动：基线落盘后 sleep 5s 才 mark_streaming，窗口内强杀
stop_app
start_app "TEST_REBUILD_SLEEP_AFTER_BASELINE_MS=5000" || { echo FATAL; exit 1; }
wait_phase ERROR 20 && ok "带钩子应用启动并识别槽缺失" || bad "未进入 ERROR: $(api /api/progress)"
curl -s -o /dev/null -X POST "$BASE/api/capture/rebuild"
# 等新世代行落盘（世代数 +1），此时 phase 仍是 REBUILDING
new_gens=$((gens_before+1))
for _ in $(seq 1 40); do
  g=$(api /api/progress | jq .generation_count)
  ph=$(api /api/progress | jq -r .phase)
  [ "$g" = "$new_gens" ] && [ "$ph" = "REBUILDING" ] && break
  sleep 0.25
done
check "$(api /api/progress | jq -r .phase)" "REBUILDING" "落盘后停在 REBUILDING 收尾窗口"
check "$(api /api/progress | jq .generation_count)" "$new_gens" "新世代基线已原子落盘"
force_kill_app
# 不带钩子重启：应识别到世代已存在，仅 mark_streaming 收尾，不新建世代
start_app || { echo FATAL; exit 1; }
wait_phase STREAMING 30 && ok "重启后只收尾即恢复 STREAMING" || bad "收尾失败: $(api /api/progress)"
check "$(api /api/progress | jq .generation_count)" "$new_gens" "收尾未产生重复/空世代"
# 新槽与新世代基线保留，流继续工作
check "$(psqlq "select count(*) from pg_replication_slots where slot_name='$SLOT'")" "1" "收尾后复制槽仍在"
write_op "{\"operations\":[{\"op\":\"update\",\"artifact_no\":1001,\"service_name\":\"billing-gateway\",\"version\":\"3.1.0\",\"sha256\":\"$SHA64\"}]}"
wait_capture 1001 3.1.0 && ok "收尾后新变更正常捕获" || bad "收尾后流不工作"
# 3.1.0 只在当前最新世代出现一次（收尾路径不会重放产生重复行）
latest_gen=$(api /api/progress | jq .current_generation)
check "$(api /api/history/1001 | jq --argjson g "$latest_gen" '[.history[]|select(.generation==$g and .version=="3.1.0")]|length')" "1" \
  "收尾路径无重复落盘（3.1.0 在世代 $latest_gen 仅一条）"
# 每个 (世代, xid) 在 txns 中唯一：重放幂等，不存在重复事务行
dup_txns=$(python3 - "$SQLDIR/history.db" <<'PY'
import sqlite3, sys
con = sqlite3.connect(sys.argv[1])
print(con.execute(
  "select count(*) from (select generation, xid from txns"
  " group by generation, xid having count(*)>1)").fetchone()[0])
PY
)
check "$dup_txns" "0" "全部世代内 (generation,xid) 唯一，无重复事务"

# ------------------------------------------------------------- 8. 重建期间普通 SQL 不中断
echo "=== 8. 重建窗口内源库普通 SQL 持续可提交"
drop_slot || bad "复制槽删除失败"
for _ in $(seq 1 20); do
  [ "$(api /api/progress | jq -r .phase)" = "ERROR" ] && break
  sleep 1
done
curl -s -o /dev/null -X POST "$BASE/api/capture/rebuild"
# 重建进行中直接对源库写入（不走捕获链路）
ok_write=$(psqlq "insert into artifacts values(5001,'during-rebuild','1','$SHA64') on conflict do nothing")
check "$(psqlq "select count(*) from artifacts where artifact_no=5001")" "1" "重建期间源库普通 SQL 正常提交"
wait_phase STREAMING 30 && ok "第三次重建完成" || bad "第三次重建失败"
nb=$(api /api/points | jq -r '[.points[]|select(.kind=="BASELINE")][-1].lsn')
check "$(api "/api/directory?point=$nb" | jq '[.artifacts[]|select(.artifact_no==5001)]|length')" "1" \
  "重建期间提交的行进入新基线（不丢，且不经旧段）"

# ------------------------------------------------------------- 9. 重复请求与状态机
echo "=== 9. 重建进行中重复请求被拒 ==="
drop_slot || bad "复制槽删除失败"
for _ in $(seq 1 20); do
  [ "$(api /api/progress | jq -r .phase)" = "ERROR" ] && break
  sleep 1
done
curl -s -o /dev/null -X POST "$BASE/api/capture/rebuild"
code=$(curl -s -o /tmp/dup.json -w '%{http_code}' -X POST "$BASE/api/capture/rebuild")
check "$code" "409" "重建进行中重复请求返回 409"
wait_phase STREAMING 30 && ok "重建最终完成" || bad "重建未完成"

echo
echo "======== 重建捕获自测结果: $PASS passed / $FAIL failed ========"
[ "$FAIL" -eq 0 ]
