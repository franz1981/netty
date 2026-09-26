#!/bin/bash
# perf record of the server: recycling 64 B before/after/biased, fixed 4 KiB before/after. 10 s each.
set -u
S=$(cd "$(dirname "$0")" && pwd)
setfreq() {
  sudo -n sh -c 'for c in /sys/devices/system/cpu/cpu[0-9]*/cpufreq/scaling_max_freq; do echo '"$1"' > $c; done'
  ok=$(cat /sys/devices/system/cpu/cpu[0-9]*/cpufreq/scaling_max_freq | grep -c "^$1\$")
  echo "== scaling_max_freq readback: $ok/32 CPUs at $1"; [ "$ok" = 32 ] || { echo "FREQ MISMATCH" >&2; return 1; }
}
setfreq 2300000 || exit 1
p=20200
for cell in "recycling 64 before" "recycling 64 after" "recycling 64 biased" "fixed 4096 before" "fixed 4096 after"; do
  set -- $cell; type=$1; slot=$2; v=$3; p=$((p+1))
  JAR=${JARS:?dir with mb-before.jar mb-after.jar mb-biased.jar}/mb-$v.jar EXTRA_JOPTS="-Dsink.batch=16384" CONNS=${CONNS:-8} "$S/run-sink-perf.sh" "$type" "$slot" $p "prof-$v" 5 10 > /dev/null 2>&1
  OUT=${OUTDIR:-/tmp}/sinkthp-prof-$v-$type-$slot
  echo "######## $type $slot $v  (samples: $(perf report -i $OUT.perfdata --stdio 2>/dev/null | grep -m1 'Event count' | cut -c1-60))"
  perf report -i $OUT.perfdata --stdio --sort sym --percent-limit 0.7 2>/dev/null | grep -vE "^#|^$" | head -45
done
setfreq 4300000
