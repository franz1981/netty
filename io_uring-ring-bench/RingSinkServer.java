// Uncommitted measurement harness: receive sink on io_uring with a provided buffer ring (liburing proxy-style:
// small slots, high buffer turnover). Counts reads and bytes, releases every buffer, prints STAT lines every 200 ms.
// args: <fixed|recycling> <slotSize> <loops> <port> <bufferRingSize>
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelOption;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.uring.IoUring;
import io.netty.channel.uring.IoUringBufferRingConfig;
import io.netty.channel.uring.IoUringBufferRingAllocator;
import io.netty.channel.uring.IoUringChannelOption;
import io.netty.channel.uring.IoUringFixedBufferRingAllocator;
import io.netty.channel.uring.IoUringIoHandler;
import io.netty.channel.uring.IoUringIoHandlerConfig;
import io.netty.channel.uring.IoUringRecyclingBufferRingAllocator;
import io.netty.channel.uring.IoUringServerSocketChannel;
import java.util.concurrent.atomic.AtomicLong;

public final class RingSinkServer {
    private static final AtomicLong EXHAUSTED = new AtomicLong();
    // Written by the single event loop, read by the stat thread: plain volatile counters (no atomics on the hot path).
    private static volatile long READS;
    private static volatile long BYTES;
    // Optional consumer-side work: sum the payload, so the mapping is read by user space too and not
    // only written by the kernel's recv copy. -Dsink.touch=true
    private static final boolean TOUCH = Boolean.getBoolean("sink.touch");
    private static volatile long CHECKSUM;

    public static void main(String[] args) throws Exception {
        String type = args[0];
        int slotSize = Integer.parseInt(args[1]);
        int loops = Integer.parseInt(args[2]);
        int port = Integer.parseInt(args[3]);
        short ringSize = Short.parseShort(args[4]);
        if (loops != 1) {
            throw new IllegalArgumentException("sink counters assume one loop");
        }
        IoUring.ensureAvailability();
        final IoUringBufferRingAllocator allocator;
        if ("fixed".equals(type)) {
            allocator = new IoUringFixedBufferRingAllocator(ByteBufAllocator.DEFAULT, slotSize);
        } else if ("recycling".equals(type)) {
            allocator = new IoUringRecyclingBufferRingAllocator(ByteBufAllocator.DEFAULT, ringSize, slotSize);
        } else {
            throw new IllegalArgumentException(type);
        }
        short bgId = 1;
        IoUringBufferRingConfig ringConfig = IoUringBufferRingConfig.builder()
                .bufferGroupId(bgId)
                .bufferRingSize(ringSize)
                .batchSize(ringSize / 4)
                .incremental(false)
                .batchAllocation(false)
                .allocator(allocator)
                .build();
        IoUringIoHandlerConfig handlerConfig = new IoUringIoHandlerConfig();
        handlerConfig.setBufferRingConfig(ringConfig);
        System.out.println("CONFIG type=" + type + " slotSize=" + slotSize + " loops=" + loops
                + " bufferRingSize=" + ringConfig.bufferRingSize() + " batchSize=" + ringConfig.batchSize()
                + " incremental=" + ringConfig.isIncremental()
                + " allocator=" + allocator.getClass().getSimpleName()
                + " channelAllocator=" + ByteBufAllocator.DEFAULT.getClass().getSimpleName()
                + " recvMultishot=" + IoUring.isRecvMultishotEnabled()
                + " recvsendBundle=" + IoUring.isRecvsendBundleEnabled()
                + " touch=" + TOUCH);
        MultiThreadIoEventLoopGroup group =
                new MultiThreadIoEventLoopGroup(loops, IoUringIoHandler.newFactory(handlerConfig));
        try {
            Channel server = new ServerBootstrap()
                    .group(group)
                    .channel(IoUringServerSocketChannel.class)
                    .childOption(ChannelOption.ALLOCATOR, ByteBufAllocator.DEFAULT)
                    .childOption(IoUringChannelOption.IO_URING_BUFFER_GROUP_ID, bgId)
                    .childHandler(new io.netty.channel.ChannelInitializer<Channel>() {
                        @Override
                        protected void initChannel(Channel ch) {
                            ch.pipeline().addLast(new ChannelInboundHandlerAdapter() {
                                private long reads;
                                private long bytes;
                                private long sum;
                                @Override
                                public void channelRead(ChannelHandlerContext ctx, Object msg) {
                                    ByteBuf buf = (ByteBuf) msg;
                                    int n = buf.readableBytes();
                                    bytes += n;
                                    reads++;
                                    if (TOUCH) {
                                        long s = sum;
                                        int i = buf.readerIndex();
                                        int end = i + n;
                                        for (; i + 8 <= end; i += 8) {
                                            s += buf.getLong(i);
                                        }
                                        for (; i < end; i++) {
                                            s += buf.getByte(i);
                                        }
                                        sum = s;
                                    }
                                    buf.release();
                                }
                                @Override
                                public void channelReadComplete(ChannelHandlerContext ctx) {
                                    // one loop: publish per read-complete, not per read
                                    READS += reads; BYTES += bytes; reads = 0; bytes = 0;
                                    if (TOUCH) {
                                        CHECKSUM += sum; sum = 0;
                                    }
                                }
                                @Override
                                public void userEventTriggered(ChannelHandlerContext ctx, Object evt) {
                                    if (evt instanceof io.netty.channel.uring.IoUringBufferRingExhaustedEvent) {
                                        EXHAUSTED.incrementAndGet();
                                    }
                                }
                                @Override
                                public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
                                    ctx.close();
                                }
                            });
                        }
                    })
                    .bind(port).sync().channel();
            System.out.println("READY " + server.localAddress());
            Thread stat = new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        while (true) {
                            Thread.sleep(200);
                            System.out.println("STAT " + System.nanoTime() + " " + READS + " " + BYTES);
                        }
                    } catch (InterruptedException ignore) {
                        // exit
                    }
                }
            });
            stat.setDaemon(true);
            stat.start();
            final IoUringBufferRingAllocator a = allocator;
            Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
                @Override
                public void run() {
                    if (a instanceof IoUringRecyclingBufferRingAllocator) {
                        System.out.println("FALLBACKS "
                                + ((IoUringRecyclingBufferRingAllocator) a).fallbackAllocations());
                    } else {
                        System.out.println("FALLBACKS n/a");
                    }
                    System.out.println("EXHAUSTED " + EXHAUSTED.get());
                    System.out.println("CHECKSUM " + CHECKSUM);
                    System.out.flush();
                }
            }));
            server.closeFuture().sync();
        } finally {
            group.shutdownGracefully();
        }
    }
}
