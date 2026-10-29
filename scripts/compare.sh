#!/usr/bin/env bash
# Runs both scenarios and prints a comparison table
set -euo pipefail

cd "$(dirname "$0")/.."
docker-compose build consumer

./scripts/run-benchmark.sh baseline
./scripts/run-benchmark.sh optimized
docker-compose --profile tools down -v --remove-orphans > /dev/null 2>&1

value() { grep "^$2=" "results/$1.env" | cut -d= -f2; }

printf '\n| %-14s | %12s | %12s |\n' "Metric" "baseline" "optimized"
printf '|%s|%s|%s|\n' "----------------" "--------------" "--------------"
for metric in status partitions messages duration_ms throughput db_commits rows_updated mismatches; do
  printf '| %-14s | %12s | %12s |\n' "$metric" "$(value baseline "$metric")" "$(value optimized "$metric")"
done

BASE=$(value baseline throughput)
OPT=$(value optimized throughput)
if (( BASE > 0 )); then
  echo
  echo "Speed-up: $(LC_NUMERIC=C awk "BEGIN { printf \"%.1f\", $OPT / $BASE }")x"
fi
