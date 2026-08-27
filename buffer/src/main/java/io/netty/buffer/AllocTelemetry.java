/*
 * TEMPORARY EXPERIMENT SCAFFOLDING - remove before merge.
 *
 * Counters that explain where chunk demand is satisfied from, and why chunks die.
 * Enabled with -Dio.netty.allocTelemetry=true; when off, ON is a constant false and
 * every call site folds away.
 */
package io.netty.buffer;

import io.netty.util.internal.SystemPropertyUtil;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

final class AllocTelemetry {
    static final boolean ON = SystemPropertyUtil.getBoolean("io.netty.allocTelemetry", false);
    /** Ablation A: pretend the chunk recycler does not exist. */
    static final boolean NO_RECYCLER = SystemPropertyUtil.getBoolean("io.netty.expt.noRecycler", false);
    /** Ablation B: cap the size-class cache exactly the way upstream 4.2 does. */
    static final boolean CACHE_CAP = SystemPropertyUtil.getBoolean("io.netty.expt.cacheCap", false);

    static final LongAdder cacheOfferRejectCap = new LongAdder();
    static final LongAdder transitionCalls = new LongAdder();
    static final LongAdder transitionFull = new LongAdder();
    // Understanding the gap: how big is the cache really, and where do segments come from?
    static final LongAdder cacheSizeSum = new LongAdder();
    static final LongAdder cacheSizeSamples = new LongAdder();
    static final LongAdder offerCalls = new LongAdder();
    static final LongAdder segFromLocal = new LongAdder();
    static final LongAdder segFromExternal = new LongAdder();
    // Why does a magazine give up its current chunk?
    static final LongAdder magKept = new LongAdder();
    static final LongAdder magTransferNoRoom = new LongAdder();
    static final LongAdder magDry = new LongAdder();
    // How full is a chunk at the moment the magazine takes it out of the cache?
    static final LongAdder pollFreeSegsSum = new LongAdder();
    static final LongAdder pollFreeSegsN = new LongAdder();

    static void sample(LongAdder sum, LongAdder n, int v) {
        if (ON) {
            sum.add(v);
            n.increment();
        }
    }

    // Where did a magazine's chunk demand get satisfied?
    static final LongAdder cachePollHit = new LongAdder();
    static final LongAdder cachePollMiss = new LongAdder();
    static final LongAdder recyclerPollHit = new LongAdder();
    static final LongAdder recyclerPollMiss = new LongAdder();
    static final LongAdder freshAlloc = new LongAdder();

    // What happened to a chunk the cache gave up on?
    static final LongAdder recyclerOfferAccept = new LongAdder();
    static final LongAdder recyclerOfferReject = new LongAdder();
    // Third theory: does the parking lot ever give memory back?
    static final LongAdder recyclerFreeAllReleased = new LongAdder();
    // Where do real frees actually happen?
    static final LongAdder deallocWithDelegate = new LongAdder();
    static final LongAdder deallocWithoutDelegate = new LongAdder();
    static final LongAdder rejectReachedDealloc = new LongAdder();

    // WHEN do rejects / evictions happen? nanoTime of first and last occurrence.
    static final AtomicLong firstRejectNanos = new AtomicLong();
    static final AtomicLong lastRejectNanos = new AtomicLong();
    static final AtomicLong firstEvictNanos = new AtomicLong();
    static final AtomicLong lastEvictNanos = new AtomicLong();
    static final AtomicLong firstFreeNanos = new AtomicLong();
    static final AtomicLong lastFreeNanos = new AtomicLong();

    static void stamp(AtomicLong first, AtomicLong last) {
        if (ON) {
            long now = System.nanoTime();
            first.compareAndSet(0L, now);
            last.lazySet(now);
        }
    }

    // Why did the cache give it up?
    static final LongAdder evictCalls = new LongAdder();
    static final LongAdder evictNotFull = new LongAdder();
    static final LongAdder evictAtFloor = new LongAdder();
    static final LongAdder evictDone = new LongAdder();
    static final LongAdder tickPurgeEvicted = new LongAdder();

    static void inc(LongAdder a) {
        if (ON) {
            a.increment();
        }
    }

    static {
        if (ON) {
            Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
                @Override
                public void run() {
                    StringBuilder sb = new StringBuilder(512);
                    sb.append("\n=== ALLOC TELEMETRY (ours) ===\n");
                    row(sb, "cache.poll.hit", cachePollHit);
                    row(sb, "cache.poll.miss", cachePollMiss);
                    row(sb, "recycler.poll.hit", recyclerPollHit);
                    row(sb, "recycler.poll.miss", recyclerPollMiss);
                    row(sb, "FRESH.byte[].alloc", freshAlloc);
                    sb.append("  --\n");
                    row(sb, "recycler.offer.accept", recyclerOfferAccept);
                    row(sb, "recycler.offer.REJECT", recyclerOfferReject);
                    row(sb, "recycler.freeAll.released", recyclerFreeAllReleased);
                    sb.append(String.format("  %-30s %,15d%n", "recycler.PARKED(acc-hit-freed)",
                            recyclerOfferAccept.sum() - recyclerPollHit.sum()
                                    - recyclerFreeAllReleased.sum()));
                    sb.append("  --\n");
                    long n = cacheSizeSamples.sum();
                    sb.append(String.format("  %-30s %,15.2f  (n=%,d)%n", "cache.size.AVG at poll",
                            n == 0 ? 0.0 : (double) cacheSizeSum.sum() / n, n));
                    long pn = pollFreeSegsN.sum();
                    sb.append(String.format("  %-30s %,15.2f  (n=%,d)%n", "polled chunk: FREE SEGMENTS",
                            pn == 0 ? 0.0 : (double) pollFreeSegsSum.sum() / pn, pn));
                    row(sb, "mag.kept.current", magKept);
                    row(sb, "mag.gaveUp.noRoomForSize", magTransferNoRoom);
                    row(sb, "mag.gaveUp.DRY(remaining==0)", magDry);
                    row(sb, "cache.offer.calls", offerCalls);
                    row(sb, "seg.from.localFreeList", segFromLocal);
                    row(sb, "seg.from.externalFreeList", segFromExternal);
                    row(sb, "transition.calls(cached release)", transitionCalls);
                    row(sb, "transition.chunkWasFull", transitionFull);
                    row(sb, "evict.calls", evictCalls);
                    row(sb, "evict.skip.notFullCapacity", evictNotFull);
                    row(sb, "evict.skip.atFloor", evictAtFloor);
                    row(sb, "evict.done", evictDone);
                    row(sb, "tickPurge.evicted", tickPurgeEvicted);
                    row(sb, "cache.offer.REJECT_AT_CAP", cacheOfferRejectCap);
                    sb.append(String.format("  ablation noRecycler=%s cacheCap=%s%n",
                            NO_RECYCLER, CACHE_CAP));
                    sb.append("  --\n");
                    row(sb, "dealloc.WITH.delegate(realfree)", deallocWithDelegate);
                    row(sb, "dealloc.without.delegate", deallocWithoutDelegate);
                    row(sb, "reject.reached.dealloc", rejectReachedDealloc);
                    long end = System.nanoTime();
                    sb.append("  -- timing, ms BEFORE this shutdown hook --\n");
                    span(sb, "recycler REJECT", firstRejectNanos, lastRejectNanos, end);
                    span(sb, "evict.done", firstEvictNanos, lastEvictNanos, end);
                    span(sb, "real free", firstFreeNanos, lastFreeNanos, end);
                    String path = SystemPropertyUtil.get("io.netty.allocTelemetry.file", null);
                    if (path != null) {
                        try (java.io.PrintWriter w = new java.io.PrintWriter(path, "UTF-8")) {
                            w.println(sb);
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                    System.err.println(sb);
                }
            }, "alloc-telemetry-dump"));
        }
    }

    private static void span(StringBuilder sb, String name, AtomicLong f, AtomicLong l, long end) {
        long fv = f.get();
        long lv = l.get();
        if (fv == 0L) {
            sb.append(String.format("  %-30s never happened%n", name));
            return;
        }
        sb.append(String.format("  %-30s first %,10.1f ms ago   last %,10.1f ms ago%n",
                name, (end - fv) / 1e6, (end - lv) / 1e6));
    }

    private static void row(StringBuilder sb, String name, LongAdder v) {
        sb.append(String.format("  %-30s %,15d%n", name, v.sum()));
    }

    private AllocTelemetry() {
    }
}
