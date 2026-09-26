#!/bin/bash
# fixed vs recycling with the ring FULL from the start (batch = ring = 16384), region = ring + ring/4.
set -u
S=$(cd "$(dirname "$0")" && pwd)
setfreq() {
  sudo -n sh -c 'for c in /sys/devices/system/cpu/cpu[0-9]*/cpufreq/scaling_max_freq; do echo '"$1"' > $c; done'
  ok=$(cat /sys/devices/system/cpu/cpu[0-9]*/cpufreq/scaling_max_freq | grep -c "^$1\$")
  echo "== scaling_max_freq readback: $ok/32 CPUs at $1"; [ "$ok" = 32 ] || { echo "FREQ MISMATCH" >&2; return 1; }
}
setfreq 2300000 || exit 1
p=19800
for rep in 1 2 3; do
  for slot in 64 4096; do
    for type in recycling; do
      p=$((p+1))
      EXTRA_JOPTS="-Dsink.batch=16384 -Dsink.inflight=1" CONNS=${CONNS:-8} "$S/run-sink-thp.sh" "$type" "$slot" $p "inflight1-r$rep" 5 20
      grep -E "^(ALLOCS|USED_DIRECT)" ${OUTDIR:-/tmp}/sinkthp-inflight1-r$rep-$type-$slot.server
      echo "-----"; sleep 2
    done
  done
done
setfreq 4300000
