#!/bin/bash
# Is the huge-page gain from the region or from the Java heap? recycling, 4096 B slots, 2 reps:
#   madvise            : nothing huge-paged
#   madvise + -XX:+UseTransparentHugePages : JVM madvises its heap only
#   always             : everything
set -u
S=$(cd "$(dirname "$0")" && pwd)
setfreq() {
  sudo -n sh -c 'for c in /sys/devices/system/cpu/cpu[0-9]*/cpufreq/scaling_max_freq; do echo '"$1"' > $c; done'
  ok=$(cat /sys/devices/system/cpu/cpu[0-9]*/cpufreq/scaling_max_freq | grep -c "^$1\$")
  echo "== scaling_max_freq readback: $ok/32 CPUs at $1"; [ "$ok" = 32 ] || { echo "FREQ MISMATCH" >&2; return 1; }
}
setthp() { sudo -n sh -c "echo $1 > /sys/kernel/mm/transparent_hugepage/enabled"; echo "== thp now: $(cat /sys/kernel/mm/transparent_hugepage/enabled)"; }
setfreq 2300000 || exit 1
p=19500
for rep in 1 2; do
  setthp madvise
  p=$((p+1)); CONNS=${CONNS:-8} "$S/run-sink-thp.sh" recycling 4096 $p "iso-none-$rep" 5 20; echo "-----"; sleep 2
  p=$((p+1)); EXTRA_JOPTS=-XX:+UseTransparentHugePages CONNS=${CONNS:-8} "$S/run-sink-thp.sh" recycling 4096 $p "iso-heaponly-$rep" 5 20; echo "-----"; sleep 2
  setthp always
  p=$((p+1)); CONNS=${CONNS:-8} "$S/run-sink-thp.sh" recycling 4096 $p "iso-all-$rep" 5 20; echo "-----"; sleep 2
done
setthp madvise
setfreq 4300000
