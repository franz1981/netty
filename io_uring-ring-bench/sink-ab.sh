#!/bin/bash
set -u
S=$(cd "$(dirname "$0")" && pwd)
setfreq() {
  sudo -n sh -c 'for c in /sys/devices/system/cpu/cpu[0-9]*/cpufreq/scaling_max_freq; do echo '"$1"' > $c; done'
  ok=$(cat /sys/devices/system/cpu/cpu[0-9]*/cpufreq/scaling_max_freq | grep -c "^$1\$")
  echo "== scaling_max_freq readback: $ok/32 CPUs at $1"; [ "$ok" = 32 ] || { echo "FREQ MISMATCH" >&2; return 1; }
}
setfreq 2300000 || exit 1
p=19100
for rep in 1 2 3; do
  for slot in 64 256 4096; do
    for type in fixed recycling; do
      p=$((p+1))
  CONNS=${CONNS:-8} "$S/run-sink.sh" "$type" "$slot" $p "r$rep" 5 20; echo "-----"; sleep 2
    done
  done
done
setfreq 4300000
