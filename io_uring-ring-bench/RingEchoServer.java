// Uncommitted measurement harness for the io_uring recycling buffer ring allocator PR.
// Raw TCP echo server on io_uring with a provided buffer ring.
// args: <fixed|recycling> <slotSize> <loops> <port> <bufferRingSize>
import io.netty.bootstrap.ServerBootstrap;
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

public final class RingEchoServer {

    private static final java.util.concurrent.atomic.AtomicLong EXHAUSTED =
            new java.util.concurrent.atomic.AtomicLong();

    private static void runNio(int port, int loops) throws Exception {
        io.netty.channel.MultiThreadIoEventLoopGroup group = new io.netty.channel.MultiThreadIoEventLoopGroup(
                loops, io.netty.channel.nio.NioIoHandler.newFactory());
        Channel server = new ServerBootstrap()
                .group(group)
                .channel(io.netty.channel.socket.nio.NioServerSocketChannel.class)
                .childOption(ChannelOption.TCP_NODELAY, true)
                .childHandler(new io.netty.channel.ChannelInitializer<Channel>() {
                    @Override
                    protected void initChannel(Channel ch) {
                        ch.pipeline().addLast(new ChannelInboundHandlerAdapter() {
                            @Override
                            public void channelRead(ChannelHandlerContext ctx, Object msg) {
                                ctx.writeAndFlush(msg, ctx.voidPromise());
                            }
                        });
                    }
                })
                .bind(port).sync().channel();
        System.out.println("CONFIG type=nio loops=" + loops);
        System.out.println("READY " + server.localAddress());
        System.out.println("FALLBACKS n/a");
        server.closeFuture().sync();
    }

    public static void main(String[] args) throws Exception {
        String type = args[0];
        int slotSize = Integer.parseInt(args[1]);
        int loops = Integer.parseInt(args[2]);
        int port = Integer.parseInt(args[3]);
        short ringSize = Short.parseShort(args[4]);

        if ("nio".equals(type)) {
            runNio(port, loops);
            return;
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
                + " batchAllocation=" + ringConfig.isBatchAllocation()
                + " allocator=" + allocator.getClass().getSimpleName()
                + " channelAllocator=" + ByteBufAllocator.DEFAULT.getClass().getSimpleName()
                + " recvMultishot=" + IoUring.isRecvMultishotEnabled()
                + " recvsendBundle=" + IoUring.isRecvsendBundleEnabled());

        MultiThreadIoEventLoopGroup group =
                new MultiThreadIoEventLoopGroup(loops, IoUringIoHandler.newFactory(handlerConfig));
        try {
            Channel server = new ServerBootstrap()
                    .group(group)
                    .channel(IoUringServerSocketChannel.class)
                    .childOption(ChannelOption.ALLOCATOR, ByteBufAllocator.DEFAULT)
                    .childOption(ChannelOption.TCP_NODELAY, true)
                    .childOption(IoUringChannelOption.IO_URING_BUFFER_GROUP_ID, bgId)
                    .childOption(IoUringChannelOption.IO_URING_WRITE_ZERO_COPY_THRESHOLD, -1)
                    .childHandler(new io.netty.channel.ChannelInitializer<Channel>() {
                        @Override
                        protected void initChannel(Channel ch) {
                            ch.pipeline().addLast(new ChannelInboundHandlerAdapter() {
                                @Override
                                public void channelRead(ChannelHandlerContext ctx, Object msg) {
                                    ctx.writeAndFlush(msg, ctx.voidPromise());
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
                    System.out.flush();
                }
            }));
            server.closeFuture().sync();
        } finally {
            group.shutdownGracefully();
        }
    }
}
