#!/bin/bash
# handover alone (after) vs handover + owner-biased refcount (biased); recycling only, ring full.
set -u
S=$(cd "$(dirname "$0")" && pwd)
setfreq() {
  sudo -n sh -c 'for c in /sys/devices/system/cpu/cpu[0-9]*/cpufreq/scaling_max_freq; do echo '"$1"' > $c; done'
  ok=$(cat /sys/devices/system/cpu/cpu[0-9]*/cpufreq/scaling_max_freq | grep -c "^$1\$")
  echo "== scaling_max_freq readback: $ok/32 CPUs at $1"; [ "$ok" = 32 ] || { echo "FREQ MISMATCH" >&2; return 1; }
}
setfreq 2300000 || exit 1
p=20100
for rep in 1 2 3; do
  for slot in 64 4096; do
    for inc in false true; do
      for v in after biased; do
        p=$((p+1))
        JAR=${JARS:?dir with mb-before.jar mb-after.jar mb-biased.jar}/mb-$v.jar EXTRA_JOPTS="-Dsink.batch=16384 -Dsink.incremental=$inc" CONNS=${CONNS:-8} "$S/run-sink-thp.sh" recycling "$slot" $p "bi-$v-inc$inc-r$rep" 5 20
        grep -E "^(ALLOCS|USED_DIRECT)" ${OUTDIR:-/tmp}/sinkthp-bi-$v-inc$inc-r$rep-recycling-$slot.server; echo "-----"; sleep 2
      done
    done
  done
done
setfreq 4300000
