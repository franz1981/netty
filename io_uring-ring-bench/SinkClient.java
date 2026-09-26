// Uncommitted: pipelined bulk sender. Each connection keeps writing 16 KiB chunks while writable.
// args: <host> <port> <connections> <warmupSeconds> <measureSeconds> <threads>
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.nio.NioIoHandler;
import io.netty.channel.socket.nio.NioSocketChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public final class SinkClient {
    private static final AtomicLong SENT = new AtomicLong();
    private static final ByteBuf CHUNK = Unpooled.unreleasableBuffer(Unpooled.directBuffer(16384).writeZero(16384));

    public static void main(String[] args) throws Exception {
        String host = args[0];
        int port = Integer.parseInt(args[1]);
        int connections = Integer.parseInt(args[2]);
        int warm = Integer.parseInt(args[3]);
        int measure = Integer.parseInt(args[4]);
        int threads = Integer.parseInt(args[5]);
        MultiThreadIoEventLoopGroup group = new MultiThreadIoEventLoopGroup(threads, NioIoHandler.newFactory());
        try {
            Bootstrap b = new Bootstrap().group(group).channel(NioSocketChannel.class)
                    .option(ChannelOption.WRITE_BUFFER_HIGH_WATER_MARK, 256 * 1024)
                    .option(ChannelOption.WRITE_BUFFER_LOW_WATER_MARK, 64 * 1024)
                    .handler(new ChannelInitializer<Channel>() {
                        @Override
                        protected void initChannel(Channel ch) {
                            ch.pipeline().addLast(new ChannelInboundHandlerAdapter() {
                                @Override
                                public void channelActive(ChannelHandlerContext ctx) {
                                    pump(ctx);
                                }
                                @Override
                                public void channelWritabilityChanged(ChannelHandlerContext ctx) {
                                    pump(ctx);
                                }
                                private void pump(ChannelHandlerContext ctx) {
                                    while (ctx.channel().isWritable()) {
                                        ctx.write(CHUNK.duplicate(), ctx.voidPromise());
                                        SENT.addAndGet(16384);
                                    }
                                    ctx.flush();
                                }
                            });
                        }
                    });
            List<ChannelFuture> futures = new ArrayList<ChannelFuture>();
            for (int i = 0; i < connections; i++) {
                futures.add(b.connect(host, port));
            }
            for (ChannelFuture f : futures) {
                f.sync();
            }
            System.out.println("CONNECTED " + connections);
            Thread.sleep(TimeUnit.SECONDS.toMillis(warm));
            long s0 = SENT.get();
            long t0 = System.nanoTime();
            Thread.sleep(TimeUnit.SECONDS.toMillis(measure));
            long s1 = SENT.get();
            double secs = (System.nanoTime() - t0) / 1e9;
            System.out.printf("RESULT connections=%d bytesSent=%d seconds=%.3f MBps=%.1f%n",
                    connections, s1 - s0, secs, (s1 - s0) / secs / 1048576.0);
            for (ChannelFuture f : futures) {
                f.channel().close();
            }
        } finally {
            group.shutdownGracefully();
        }
    }
}
