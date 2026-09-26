#!/bin/bash
# One sink cell. args: <fixed|recycling> <slotSize> <port> <tag> [warm] [measure]
set -u
W=${W:?set W to the netty checkout with this PR built}
CP="$W/io_uring-ring-bench/classes:$W/microbench/target/microbenchmarks.jar"
JAVA=${JAVA:-java}
TYPE=$1; SLOT=$2; PORT=$3; TAG=$4; WARM=${5:-5}; MEASURE=${6:-20}
LOOPS=1; RING=${RING:-16384}; CONNS=${CONNS:-8}; CORE=${CORE:-3}
OUT=${OUTDIR:-/tmp}/sinkthp-$TAG-$TYPE-$SLOT
JOPTS="-Xms1g -Xmx1g -XX:MaxDirectMemorySize=2g -Dio.netty.leakDetection.level=disabled"

taskset -c $CORE ${SERVER_NUMA:-numactl --membind=0} $JAVA $JOPTS -cp "$CP" RingSinkServer "$TYPE" $SLOT $LOOPS $PORT $RING > "$OUT.server" 2>&1 &
SPID=$!
for i in $(seq 1 100); do grep -q READY "$OUT.server" && break; sleep 0.2; done
if ! grep -q READY "$OUT.server"; then echo "SERVER FAILED"; cat "$OUT.server"; kill -9 $SPID 2>/dev/null; exit 1; fi

${CLIENT_NUMA:-numactl --cpunodebind=1 --membind=1} $JAVA $JOPTS -cp "$CP" \
    SinkClient 127.0.0.1 $PORT $CONNS $WARM $MEASURE 8 > "$OUT.client" 2>&1 &
CPID=$!
for i in $(seq 1 300); do grep -q CONNECTED "$OUT.client" && break; sleep 0.2; done
sleep $WARM
( for i in 1 2 3 4 5 6; do cut -d' ' -f4 /proc/loadavg; sleep 3; done ) > "$OUT.runnable" &
( sleep 8; grep -E "^(Rss|AnonHugePages)" /proc/$SPID/smaps_rollup ) > "$OUT.smaps" &
S0=$(grep STAT "$OUT.server" | tail -1)
perf stat -e task-clock,cycles,instructions,ls_l1_d_tlb_miss.all,ls_l1_d_tlb_miss.all_l2_miss -p $SPID -- sleep $MEASURE 2> "$OUT.perf"
S1=$(grep STAT "$OUT.server" | tail -1)
wait $CPID
kill -TERM $SPID 2>/dev/null; wait $SPID 2>/dev/null

R0=$(echo $S0 | cut -d' ' -f3); B0=$(echo $S0 | cut -d' ' -f4); T0=$(echo $S0 | cut -d' ' -f2)
R1=$(echo $S1 | cut -d' ' -f3); B1=$(echo $S1 | cut -d' ' -f4); T1=$(echo $S1 | cut -d' ' -f2)
TC=$(grep task-clock "$OUT.perf" | awk '{gsub(",","",$1); print $1}')
CYC=$(grep -E " cycles" "$OUT.perf" | awk '{gsub(",","",$1); print $1}')
INS=$(grep instructions "$OUT.perf" | awk '{gsub(",","",$1); print $1}')
echo "cell=$TYPE slot=$SLOT tag=$TAG"
grep -E "^CONFIG" "$OUT.server" | cut -c1-160
grep -E "^RESULT" "$OUT.client"
grep -E "^FALLBACKS|^EXHAUSTED" "$OUT.server" | tr '\n' ' '; echo
echo "window: reads=$((R1-R0)) bytes=$((B1-B0)) secs=$(echo "scale=3; ($T1-$T0)/1000000000" | bc) taskclock_ms=$TC"
echo "per read: ns_cpu=$(echo "scale=1; $TC*1000000/($R1-$R0)" | bc) cycles=$(echo "scale=0; $CYC/($R1-$R0)" | bc) insns=$(echo "scale=0; $INS/($R1-$R0)" | bc) bytes=$(echo "scale=1; ($B1-$B0)/($R1-$R0)" | bc)"
echo "rates: reads/s=$(echo "scale=0; ($R1-$R0)*1000000000/($T1-$T0)" | bc) MB/s=$(echo "scale=1; ($B1-$B0)*1000000000/($T1-$T0)/1048576" | bc) cpu_share=$(echo "scale=3; $TC/1000/(($T1-$T0)/1000000000)" | bc)"
TLB=$(grep "ls_l1_d_tlb_miss.all_l2_miss" "$OUT.perf" | awk '{gsub(",","",$1); print $1}')
echo "walks_per_read=$(echo "scale=2; $TLB/($R1-$R0)" | bc) thp=$(cat /sys/kernel/mm/transparent_hugepage/enabled) $(tr '\n' ' ' < "$OUT.smaps")"
echo "runnable=$(tr '\n' ',' < "$OUT.runnable")"
