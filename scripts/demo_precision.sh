#!/usr/bin/env bash
# 精度修复演示：修复前触发数据、修复后精确结果、旧库迁移待核与校正。
# 用法: ./scripts/demo_precision.sh [BASE_URL]   默认 http://localhost:8001
set -euo pipefail

BASE="${1:-${BASE_URL:-http://localhost:8001}}"
SAMPLES="$(cd "$(dirname "$0")/.." && pwd)/samples"

if command -v jq >/dev/null 2>&1; then PP="jq ."; else PP="python3 -m json.tool"; fi
step() { printf '\n\033[1;36m== %s ==\033[0m\n' "$*"; }
post_csv() { curl -sS -X POST "$1" -H "accept: application/json" -F "file=@$2;type=text/csv"; }
get() { curl -sS "$1"; }

step "0. /docs 一键场景：修复前 vs 修复后（不写正式库）"
get "$BASE/api/demo/scenario" | $PP

step "1. 修复前触发数据：合法大累计读数每小时仅 +0.00001 kWh"
echo "-- 修复前（REAL/float 模拟，不写库）：日总量被低估为 0.0002"
# GitHub Actions 等环境没有本地文件也无妨，scenario 接口已经内联该数据
post_csv "$BASE/api/demo/pre-fix-preview" "$SAMPLES/precision/readings_tiny_increment.csv" | $PP

step "2. 修复后：同一 CSV 正常导入 -> 日总量精确 = 0.00024（24 × 0.00001）"
post_csv "$BASE/api/imports/readings" "$SAMPLES/precision/readings_tiny_increment.csv" | $PP
get "$BASE/api/settlement/daily?mp=MP-PREC&date=2026-04-01" | $PP

step "3. 不同读数在旧版 float 下退化为同值；修复后异值重传必须 409（不得静默跳过）"
post_csv "$BASE/api/imports/readings" "$SAMPLES/precision/readings_distinct_repr.csv" | $PP
echo "-- 下一批把 04:00 的值再 +0.00001："
curl -sS -o /tmp/prec_conflict.json -w "HTTP %{http_code}\n" \
  -X POST "$BASE/api/imports/readings" \
  -F "file=@$SAMPLES/precision/readings_distinct_repr_conflict.csv;type=text/csv"
cat /tmp/prec_conflict.json | $PP

step "4. 旧库迁移模拟：未按原始 CSV 校正的旧值 -> 相关增量待核，不给确定日总量"
post_csv "$BASE/api/demo/legacy-seed/readings?confirm=true" "$SAMPLES/legacy/readings_legacy_old.csv" | $PP
post_csv "$BASE/api/demo/legacy-seed/readings?confirm=true" "$SAMPLES/legacy/readings_legacy_new.csv" | $PP
post_csv "$BASE/api/demo/legacy-seed/meter-changes?confirm=true" "$SAMPLES/legacy/meter_change_legacy.csv" | $PP
get "$BASE/api/settlement/daily?mp=DEMO-LEGACY&date=2026-04-01" | $PP

step "5. 按原始 CSV 重新导入校正 -> legacy_corrected，逐条报告 changed；恢复确定核算"
post_csv "$BASE/api/imports/readings" "$SAMPLES/legacy/readings_legacy_old.csv" | $PP
post_csv "$BASE/api/imports/meter-changes" "$SAMPLES/legacy/meter_change_legacy.csv" | $PP
post_csv "$BASE/api/imports/readings" "$SAMPLES/legacy/readings_legacy_new.csv" | $PP
get "$BASE/api/settlement/daily?mp=DEMO-LEGACY&date=2026-04-01" | $PP
get "$BASE/api/settlement/daily?mp=DEMO-LEGACY&date=2026-04-02" | $PP

step "6. 清理演示数据"
get "$BASE/api/demo/reset?mp=DEMO-LEGACY&confirm=true" | $PP

printf '\n\033[1;32m精度演示完成。也可在 %s/docs 直接调用 /api/demo/scenario 查看。\033[0m\n' "$BASE"
