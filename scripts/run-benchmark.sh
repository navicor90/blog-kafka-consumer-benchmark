#!/usr/bin/env bash
# Runs one scenario from a clean environment and writes its metrics to results/<scenario>.env
set -euo pipefail

SCENARIO="${1:?usage: run-benchmark.sh <baseline|optimized|optimized-3p>}"
case "$SCENARIO" in
  baseline) PROFILE=baseline; PARTITIONS=3 ;;
  optimized) PROFILE=optimized; PARTITIONS=6 ;;
  # All recommendations except scaling partitions, to isolate their gain
  optimized-3p) PROFILE=optimized; PARTITIONS=3 ;;
  *) echo "Unknown scenario: $SCENARIO" >&2; exit 1 ;;
esac

export PROFILE PARTITIONS
export SENSORS="${SENSORS:-600000}"
export MESSAGES="${MESSAGES:-2000000}"
TIMEOUT_SECONDS="${TIMEOUT_SECONDS:-900}"

cd "$(dirname "$0")/.."
COMPOSE="docker-compose"
mkdir -p results

psql_value() {
  $COMPOSE exec -T postgres psql -U sensors -d sensors -tAc "$1" | tr -d '[:space:]'
}

echo "==> [$SCENARIO] Resetting environment"
$COMPOSE --profile tools down -v --remove-orphans > /dev/null 2>&1
$COMPOSE up -d --wait kafka postgres

echo "==> [$SCENARIO] Creating topic with $PARTITIONS partitions"
$COMPOSE exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
  --create --topic sensor-readings --partitions "$PARTITIONS" --replication-factor 1 > /dev/null

echo "==> [$SCENARIO] Producing $MESSAGES messages for $SENSORS sensors"
$COMPOSE run --rm producer

psql_value "SELECT pg_stat_reset()" > /dev/null
COMMITS_BEFORE=$(psql_value "SELECT xact_commit FROM pg_stat_database WHERE datname = 'sensors'")

echo "==> [$SCENARIO] Starting consumer (timeout ${TIMEOUT_SECONDS}s)"
$COMPOSE up -d consumer
DEADLINE=$((SECONDS + TIMEOUT_SECONDS))
STATUS="completed"
until $COMPOSE logs consumer 2>/dev/null | grep "RESULT " > /dev/null; do
  if (( SECONDS > DEADLINE )); then STATUS="timeout"; break; fi
  if [[ -z "$($COMPOSE ps -q --status running consumer)" ]]; then
    echo "Consumer stopped unexpectedly" >&2; $COMPOSE logs --tail 50 consumer >&2; exit 1
  fi
  LAST=$($COMPOSE logs consumer 2>/dev/null | grep "PROGRESS " | tail -1 | sed 's/.*PROGRESS //' || true)
  if [[ -n "$LAST" ]]; then echo "    $LAST"; fi
  sleep 10
done

if [[ "$STATUS" == "completed" ]]; then
  LINE=$($COMPOSE logs consumer | grep "RESULT " | tail -1 | sed 's/.*RESULT //')
else
  LINE=$($COMPOSE logs consumer | grep "PROGRESS " | tail -1 | sed 's/.*PROGRESS //')
fi
$COMPOSE stop consumer > /dev/null 2>&1

# pg_stat counters are flushed asynchronously
sleep 2
COMMITS_AFTER=$(psql_value "SELECT xact_commit FROM pg_stat_database WHERE datname = 'sensors'")
ROWS_UPDATED=$(psql_value "SELECT n_tup_upd FROM pg_stat_user_tables WHERE relname = 'sensor'")
MISMATCHES=$(psql_value "SELECT count(*) FROM sensor s JOIN expected_state e USING (id) WHERE NOT s.decommissioned AND s.temperature IS DISTINCT FROM e.temperature")

eval "$(echo "$LINE" | tr ' ' '\n' | sed 's/^/RESULT_/')"
{
  echo "scenario=$SCENARIO"
  echo "status=$STATUS"
  echo "partitions=$PARTITIONS"
  echo "messages=$RESULT_messages"
  echo "duration_ms=$RESULT_durationMs"
  echo "throughput=$RESULT_throughput"
  echo "db_commits=$((COMMITS_AFTER - COMMITS_BEFORE))"
  echo "rows_updated=$ROWS_UPDATED"
  echo "mismatches=$MISMATCHES"
} | tee "results/$SCENARIO.env"

if [[ "$STATUS" == "timeout" ]]; then
  echo "NOTE: timed out, mismatches are expected because the topic was not fully consumed" >&2
fi
