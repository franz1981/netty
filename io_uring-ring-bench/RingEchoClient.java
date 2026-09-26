// Uncommitted measurement harness for the io_uring recycling buffer ring allocator PR.
// Strict ping-pong client: every connection sends the next message only after its echo came back.
// args: <host> <port> <connections> <messageSize> <warmupSeconds> <measureSeconds> <threads>
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioIoHandler;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.FixedLengthFrameDecoder;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public final class RingEchoClient {

    private static final AtomicLong MESSAGES = new AtomicLong();
    private static final AtomicLong ACTIVE = new AtomicLong();
    private static final AtomicLong ERRORS = new AtomicLong();
    private static final java.util.concurrent.atomic.AtomicInteger SEQ =
            new java.util.concurrent.atomic.AtomicInteger();
    private static final long[] READS = new long[8192];
    private static final long[] ACTIVATIONS = new long[8192];
    private static volatile boolean counting;

    public static void main(String[] args) throws Exception {
        String host = args[0];
        final int port = Integer.parseInt(args[1]);
        int connections = Integer.parseInt(args[2]);
        final int messageSize = Integer.parseInt(args[3]);
        long warmupMs = Long.parseLong(args[4]) * 1000L;
        long measureMs = Long.parseLong(args[5]) * 1000L;
        int threads = Integer.parseInt(args[6]);


        MultiThreadIoEventLoopGroup group = new MultiThreadIoEventLoopGroup(threads, NioIoHandler.newFactory());
        List<Channel> channels = new ArrayList<Channel>();
        try {
            Bootstrap bootstrap = new Bootstrap()
                    .group(group)
                    .channel(NioSocketChannel.class)
                    .option(ChannelOption.TCP_NODELAY, true)
                    .handler(new ChannelInitializer<Channel>() {
                        @Override
                        protected void initChannel(Channel ch) {
                            // One payload per channel: a ByteBuf and its duplicates share the parent's cached
                            // internalNioBuffer, so sharing one across event loops corrupts concurrent writes.
                            final ByteBuf payload = Unpooled.directBuffer(messageSize, messageSize);
                            for (int i = 0; i < messageSize; i++) {
                                payload.writeByte(i);
                            }
                            ch.closeFuture().addListener(f -> payload.release());
                            ch.pipeline().addLast(new FixedLengthFrameDecoder(messageSize));
                            final int id = SEQ.getAndIncrement();
                            ch.pipeline().addLast(new SimpleChannelInboundHandler<ByteBuf>() {
                                private long own;

                                @Override
                                public void channelActive(ChannelHandlerContext ctx) {
                                    ACTIVATIONS[id]++;
                                    ctx.writeAndFlush(payload.retainedDuplicate(), ctx.voidPromise());
                                }

                                @Override
                                protected void channelRead0(ChannelHandlerContext ctx, ByteBuf msg) {
                                    READS[id]++;
                                    if (counting) {
                                        MESSAGES.incrementAndGet();
                                        if (++own == 1) {
                                            ACTIVE.incrementAndGet();
                                        }
                                    }
                                    ctx.writeAndFlush(payload.retainedDuplicate(), ctx.voidPromise());
                                }

                                @Override
                                public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
                                    if (ERRORS.incrementAndGet() < 4) {
                                        cause.printStackTrace();
                                    }
                                    ctx.close();
                                }
                            });
                        }
                    });
            List<ChannelFuture> futures = new ArrayList<ChannelFuture>();
            for (int i = 0; i < connections; i++) {
                futures.add(bootstrap.connect(host, port));
            }
            for (ChannelFuture f : futures) {
                channels.add(f.sync().channel());
            }
            System.out.println("CONNECTED " + channels.size());
            Thread.sleep(warmupMs);
            counting = true;
            MESSAGES.set(0);
            long start = System.nanoTime();
            Thread.sleep(measureMs);
            long elapsed = System.nanoTime() - start;
            counting = false;
            long messages = MESSAGES.get();
            int open = 0;
            for (Channel ch : channels) {
                if (ch.isActive()) {
                    open++;
                }
            }
            int activated = 0;
            int everRead = 0;
            long maxReads = 0;
            for (int i = 0; i < SEQ.get(); i++) {
                if (ACTIVATIONS[i] > 0) {
                    activated++;
                }
                if (READS[i] > 0) {
                    everRead++;
                }
                maxReads = Math.max(maxReads, READS[i]);
            }
            System.out.println("DIAG channels=" + SEQ.get() + " activated=" + activated
                    + " everRead=" + everRead + " maxReadsOnOneChannel=" + maxReads);
            System.out.printf("RESULT connections=%d open=%d active=%d errors=%d messages=%d seconds=%.3f "
                            + "messagesPerSecond=%.0f%n",
                    channels.size(), open, ACTIVE.get(), ERRORS.get(), messages, elapsed / 1e9,
                    messages / (elapsed / 1e9));
        } finally {
            for (Channel ch : channels) {
                ch.close();
            }
            group.shutdownGracefully();
        }
    }
}
