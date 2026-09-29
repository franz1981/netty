/*
 * Copyright 2026 The Netty Project
 *
 * The Netty Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package io.netty.handler.codec.dns;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.buffer.UnpooledHeapByteBuf;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.Test;

import java.lang.management.ManagementFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Counts (no timing) the work a single frame-legal DNS message can force on the decoder.
 */
public class DnsDecodeAmplificationTest {

    private static long allocatedBytes() {
        return ((com.sun.management.ThreadMXBean) ManagementFactory.getThreadMXBean())
                .getThreadAllocatedBytes(Thread.currentThread().getId());
    }

    /** The reporter's message: one 253-char question name, then ANCOUNT CNAME records "C00C ... C00C". */
    static ByteBuf reporterMessage(int answerCount) {
        return reporterMessage(answerCount, answerCount, DnsRecordType.CNAME);
    }

    static ByteBuf reporterMessage(int answerCount, int recordsToWrite, DnsRecordType type) {
        ByteBuf m = Unpooled.buffer(65535);
        m.writeShort(0);            // id
        m.writeShort(0);            // flags: query
        m.writeShort(1);            // QDCOUNT
        m.writeShort(answerCount);  // ANCOUNT
        m.writeShort(0);
        m.writeShort(0);
        int[] labels = { 63, 63, 63, 61 };
        for (int len : labels) {
            m.writeByte(len);
            for (int i = 0; i < len; i++) {
                m.writeByte('a');
            }
        }
        m.writeByte(0);
        m.writeShort(DnsRecordType.A.intValue());
        m.writeShort(1);
        int records = 0;
        while (m.writableBytes() >= 14 + 2 && records < recordsToWrite) {
            m.writeShort(0xC00C);                          // NAME -> question name
            m.writeShort(type.intValue());
            m.writeShort(1);
            m.writeInt(0);
            m.writeShort(2);
            m.writeShort(0xC00C);                          // RDATA -> question name
            records++;
        }
        assertEquals(recordsToWrite, records);
        return m;
    }

    private static long decodeOnTcp(ByteBuf message) {
        EmbeddedChannel ch = new EmbeddedChannel(new TcpDnsQueryDecoder());
        ByteBuf framed = Unpooled.buffer(2 + message.readableBytes());
        framed.writeShort(message.readableBytes());
        framed.writeBytes(message);
        message.release();
        long before = allocatedBytes();
        ch.writeInbound(framed);
        long allocated = allocatedBytes() - before;
        DnsQuery q = ch.readInbound();
        q.release();
        ch.finishAndReleaseAll();
        return allocated;
    }

    @Test
    public void reporterMessageAllocation() {
        for (int i = 0; i < 5; i++) {
            decodeOnTcp(reporterMessage(4661)); // warm up
        }
        ByteBuf msg = reporterMessage(4661);
        int wire = msg.readableBytes();
        long attack = decodeOnTcp(msg);
        long half = decodeOnTcp(reporterMessage(2330));
        long aType = decodeOnTcp(reporterMessage(4661, 4661, DnsRecordType.A));
        long control = decodeOnTcp(reporterMessage(0, 4661, DnsRecordType.CNAME));
        System.out.println("DNS reporter message: wire=" + wire + " B, allocated(ANCOUNT=4661)=" + attack
                + " B (" + (attack / wire) + "x wire), 2330 records=" + half
                + " B, 4661 A-type records=" + aType + " B, same bytes with ANCOUNT=0=" + control + " B");
        // Linear: doubling the record count must not much more than double the allocation.
        // Before the fix every name decode allocated 2 x (remaining message) and this ratio was ~3.7.
        assertTrue(attack <= 2.5 * half, "4661 records allocated " + attack + " B, 2330 records " + half + " B");
    }

    /** Counts readerIndex(int) calls; decodeDomainName does one per compression pointer it follows. */
    static final class CountingByteBuf extends UnpooledHeapByteBuf {
        long readerIndexSets;

        CountingByteBuf(int capacity) {
            super(UnpooledByteBufAllocator.DEFAULT, capacity, capacity);
        }

        @Override
        public ByteBuf readerIndex(int readerIndex) {
            readerIndexSets++;
            return super.readerIndex(readerIndex);
        }
    }

    /**
     * Record 0 carries, inside opaque A-type RDATA, a chain of L compression pointers that each point to
     * the next two bytes and end in the root label. Every following record's NAME points at the chain start,
     * so each of them follows all L pointers. The loop guard only stops a chain after writerIndex/2 hops.
     */
    @Test
    public void pointerChainHopsPerMessage() throws Exception {
        CountingByteBuf m = new CountingByteBuf(65535);
        m.writeShort(0);
        m.writeShort(0);           // query
        m.writeShort(0);           // QDCOUNT
        int ancountIndex = m.writerIndex();
        m.writeShort(0);           // ANCOUNT, patched below
        m.writeShort(0);
        m.writeShort(0);
        // record 0
        m.writeByte(0);            // root name
        m.writeShort(DnsRecordType.A.intValue());
        m.writeShort(1);
        m.writeInt(0);
        int chainStart = m.writerIndex() + 2;
        int chainLen = (0x3FFF - chainStart) / 2 - 1;      // pointers can only address the first 16 KiB
        m.writeShort(2 * chainLen + 1);
        for (int i = 0; i < chainLen; i++) {
            int next = chainStart + 2 * (i + 1);
            m.writeShort(0xC000 | next);
        }
        m.writeByte(0);            // end of chain: root
        int records = 1;
        while (m.writableBytes() >= 12) {
            m.writeShort(0xC000 | chainStart);
            m.writeShort(DnsRecordType.A.intValue());
            m.writeShort(1);
            m.writeInt(0);
            m.writeShort(0);
            records++;
        }
        m.setShort(ancountIndex, records);
        int wire = m.readableBytes();

        m.readerIndexSets = 0;
        DnsQuery q = DnsMessageUtil.decodeDnsQuery(DnsRecordDecoder.DEFAULT, m,
                new DnsMessageUtil.DnsQueryFactory() {
                    @Override
                    public DnsQuery newQuery(int id, DnsOpCode dnsOpCode) {
                        return new DefaultDnsQuery(id, dnsOpCode);
                    }
                });
        long hops = m.readerIndexSets;
        assertEquals(records, q.count(DnsSection.ANSWER));
        q.release();
        m.release();
        System.out.println("DNS pointer chain: wire=" + wire + " B, records=" + records + ", chain=" + chainLen
                + " pointers, readerIndex(int) calls=" + hops + " (" + (hops / wire) + " per wire byte)");
        // A valid name has at most 127 labels, so following more than a few hundred pointers per name is
        // never needed; allow 256 per record.
        assertTrue(hops <= 256L * records, hops + " pointer hops for " + records + " records");
    }
}
