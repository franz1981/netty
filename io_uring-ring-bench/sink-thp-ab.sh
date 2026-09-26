#!/bin/bash
# THP A/B on the sink cell, 4096 B slots (region = 16384 * 4096 = 64 MiB, so 2 MiB pages can apply;
# at 64 B slots the region is 1 MiB and cannot hold one). fixed vs recycling, 2 reps, interleaved.
set -u
S=$(cd "$(dirname "$0")" && pwd)
setfreq() {
  sudo -n sh -c 'for c in /sys/devices/system/cpu/cpu[0-9]*/cpufreq/scaling_max_freq; do echo '"$1"' > $c; done'
  ok=$(cat /sys/devices/system/cpu/cpu[0-9]*/cpufreq/scaling_max_freq | grep -c "^$1\$")
  echo "== scaling_max_freq readback: $ok/32 CPUs at $1"; [ "$ok" = 32 ] || { echo "FREQ MISMATCH" >&2; return 1; }
}
setthp() { sudo -n sh -c "echo $1 > /sys/kernel/mm/transparent_hugepage/enabled"; echo "== thp now: $(cat /sys/kernel/mm/transparent_hugepage/enabled)"; }
setfreq 2300000 || exit 1
p=19300
for rep in 1 2; do
  for thp in madvise always; do
    setthp $thp
    for type in fixed recycling; do
      p=$((p+1)); CONNS=8 "$S/run-sink-thp.sh" "$type" 4096 $p "$thp-$rep" 5 20; echo "-----"; sleep 2
    done
  done
done
setthp madvise
setfreq 4300000
