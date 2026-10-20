#!/bin/sh

STAT_CPU=/sys/fs/cgroup/cpu.stat
CPU_MAX=/sys/fs/cgroup/cpu.max

STATE_DIR=/tmp/.healthcheck
PREV_CPU=$STATE_DIR/cpu_usage
PREV_TS=$STATE_DIR/ts
CNT_FILE=$STATE_DIR/cnt

MAX_CNT=2
THRESHOLD=0.9

mkdir -p "$STATE_DIR"

CUR_CPU=$(awk '/usage_usec/ {print $2}' "$STAT_CPU")
CUR_TS=$(date +%s%6N)

read QUOTA PERIOD < "$CPU_MAX"
if [ "$QUOTA" = "max" ]; then
  exit 1
fi

CPU_LIMIT=$(awk "BEGIN { printf \"%.6f\", $QUOTA / $PERIOD }")

if [ -f "$PREV_CPU" ] && [ -f "$PREV_TS" ]; then
  PREV_CPU_VAL=$(cat "$PREV_CPU")
  PREV_TS_VAL=$(cat "$PREV_TS")

  DELTA_CPU=$((CUR_CPU - PREV_CPU_VAL))
  DELTA_TS=$((CUR_TS - PREV_TS_VAL))

  if [ "$DELTA_TS" -gt 0 ]; then
    USAGE=$(awk "BEGIN { printf \"%.3f\", $DELTA_CPU / ($DELTA_TS * $CPU_LIMIT) }")
    OVER=$(awk "BEGIN { if ($USAGE >= $THRESHOLD) print 1; else print 0 }")

    CNT=$(cat "$CNT_FILE" 2>/dev/null || echo 0)
    if [ "$OVER" = "1" ]; then
      CNT=$((CNT + 1))
    else
      CNT=0
    fi
    echo "$CNT" > "$CNT_FILE"

    if [ "$CNT" -ge "$MAX_CNT" ]; then
      kill -TERM 1
    fi
  fi
fi

echo "$CUR_CPU" > "$PREV_CPU"
echo "$CUR_TS" > "$PREV_TS"
exit 0
