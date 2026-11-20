#!/usr/bin/env bash
# T04 Paimon 冒烟测试：验证 Flink 与 Spark 能读写同一张 Paimon 主键表，且重复主键只保留一行。
#
#   1. Flink 建主键表 paimon.ops.smoke_pk，分两次写入，第二次包含重复主键
#   2. Spark 读取，期望 4 行、v 之和为 6；然后 Spark 再写入（1 行更新 + 1 行新增）
#   3. Flink 读取，期望 5 行、v 之和为 9
#
# 前提：`make up` 已启动服务。可重复运行（每次先删表重建）。
set -euo pipefail
cd "$(dirname "$0")/.."

COMPOSE=(docker compose --env-file infra/versions.env --env-file .env)
FLINK_SQL=("${COMPOSE[@]}" exec -T --user flink flink-jobmanager
           /opt/flink/bin/sql-client.sh -i /opt/sql/00_catalog.sql -f)
SPARK_SQL=("${COMPOSE[@]}" --profile batch run --rm --no-deps -T spark spark-sql -S -e)

pass=0; fail=0
check() {  # check <说明> <期望> <实际>
  if [[ "$2" == "$3" ]]; then echo "  PASS  $1"; pass=$((pass + 1))
  else echo "  FAIL  $1：期望 [$2]，实际 [$3]"; fail=$((fail + 1)); fi
}

echo "== 1/3 Flink：建表并写入（含重复主键）"
out=$("${FLINK_SQL[@]}" /opt/sql/smoke/t04_flink_write.sql 2>&1)
if grep -q '\[ERROR\]' <<<"${out}"; then echo "${out}" | grep -A5 '\[ERROR\]'; exit 1; fi
echo "${out}" | grep -E '^\| +[0-9]' || true

echo "== 2/3 Spark：读取 Flink 写入的数据，再写入"
rows=$("${SPARK_SQL[@]}" "SELECT id, v FROM paimon.ops.smoke_pk ORDER BY id" 2>/dev/null | tr '\t' ':' | paste -sd' ' -)
check "Spark 读到 Flink 的数据且重复主键已合并" "1:1 2:2 3:2 4:1" "${rows}"
"${SPARK_SQL[@]}" "INSERT INTO paimon.ops.smoke_pk VALUES (1, 'a', 3), (5, 'e', 1)" >/dev/null 2>&1
rows=$("${SPARK_SQL[@]}" "SELECT COUNT(*), SUM(v) FROM paimon.ops.smoke_pk" 2>/dev/null | tr '\t' ' ')
check "Spark 写入后：5 行，v 之和 9" "5 9" "${rows}"

echo "== 3/3 Flink：读取 Spark 写入后的结果"
out=$("${FLINK_SQL[@]}" /opt/sql/smoke/t04_flink_read.sql 2>&1)
rows=$(grep -o 'FLINK_RESULT [0-9]* [0-9]*' <<<"${out}" | tail -1 | cut -d' ' -f2-)
check "Flink 读到 Spark 写入的数据" "5 9" "${rows}"

owner=$("${COMPOSE[@]}" exec -T flink-jobmanager stat -c '%u' /data/paimon/ops.db/smoke_pk)
check "表目录归 uid 9999 所有（两个引擎都能写）" "9999" "${owner}"

echo
echo "结果：${pass} 项通过，${fail} 项失败"
[[ "${fail}" -eq 0 ]]
