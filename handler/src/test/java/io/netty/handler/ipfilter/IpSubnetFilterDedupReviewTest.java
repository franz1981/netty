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
package io.netty.handler.ipfilter;

import io.netty.util.internal.SocketUtils;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import static io.netty.handler.ipfilter.IpFilterRuleType.ACCEPT;
import static io.netty.handler.ipfilter.IpFilterRuleType.REJECT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Review tests for GHSA-268p-9r42-7qp4 (IpSubnetFilter dedup).
 */
public class IpSubnetFilterDedupReviewTest {

    private static IpSubnetFilterRule r(String ip, int prefix, IpFilterRuleType type) {
        return new IpSubnetFilterRule(ip, prefix, type);
    }

    private static InetSocketAddress a(String ip) {
        return SocketUtils.socketAddress(ip, 1234);
    }

    private static void assertThrowsIae(final IpSubnetFilterRule... rules) {
        assertThrows(IllegalArgumentException.class, () -> new IpSubnetFilter(false, rules),
                Arrays.toString(rules));
    }

    // ---- mixed-type containment, both input orders (should throw after the fix) ----

    @Test
    public void mixedTypeContainmentThrowsInBothOrdersV4() {
        assertThrowsIae(r("10.5.0.0", 16, ACCEPT), r("10.5.3.0", 24, REJECT));
        assertThrowsIae(r("10.5.3.0", 24, REJECT), r("10.5.0.0", 16, ACCEPT));
        assertThrowsIae(r("10.5.0.0", 16, REJECT), r("10.5.3.0", 24, ACCEPT));
        assertThrowsIae(r("10.5.3.0", 24, ACCEPT), r("10.5.0.0", 16, REJECT));
        // identical subnet, both types
        assertThrowsIae(r("10.5.0.0", 16, ACCEPT), r("10.5.0.0", 16, REJECT));
        assertThrowsIae(r("10.5.0.0", 16, REJECT), r("10.5.0.0", 16, ACCEPT));
        // same network address, different prefix (sort tie)
        assertThrowsIae(r("10.5.0.0", 24, REJECT), r("10.5.0.0", 16, ACCEPT));
        assertThrowsIae(r("10.5.0.0", 16, ACCEPT), r("10.5.0.0", 24, REJECT));
    }

    @Test
    public void mixedTypeContainmentThrowsInBothOrdersV6() {
        assertThrowsIae(r("2001:db8::", 32, ACCEPT), r("2001:db8:5::", 48, REJECT));
        assertThrowsIae(r("2001:db8:5::", 48, REJECT), r("2001:db8::", 32, ACCEPT));
        assertThrowsIae(r("2001:db8::", 32, REJECT), r("2001:db8:5::", 48, ACCEPT));
        assertThrowsIae(r("2001:db8::", 32, ACCEPT), r("2001:db8::", 32, REJECT));
        assertThrowsIae(r("2001:db8::", 48, REJECT), r("2001:db8::", 32, ACCEPT));
    }

    @Test
    public void ipv4AndIpv6RulesDoNotInteract() {
        IpSubnetFilter f = new IpSubnetFilter(true, r("0.0.0.0", 0, REJECT), r("::", 0, ACCEPT));
        assertFalse(f.accept(null, a("10.1.1.1")));
        assertTrue(f.accept(null, a("2001:db8::1")));
    }

    // ---- gaps ----

    /**
     * Same-type path the PR keeps: two REJECT rules with the same network address; Collections.sort is stable
     * and compareTo() only looks at the network address, so input order decides which one is the "parent".
     * With the /24 first, the broader /16 REJECT is dropped and 10.5.1.1 is accepted.
     */
    @Test
    public void sameTypeRejectTieKeepsBroaderReject() {
        IpSubnetFilter f = new IpSubnetFilter(true, r("10.5.0.0", 24, REJECT), r("10.5.0.0", 16, REJECT));
        assertFalse(f.accept(null, a("10.5.1.1")), "REJECT 10.5.0.0/16 was dropped");
    }

    @Test
    public void sameTypeRejectTieControlOtherOrder() {
        IpSubnetFilter f = new IpSubnetFilter(true, r("10.5.0.0", 16, REJECT), r("10.5.0.0", 24, REJECT));
        assertFalse(f.accept(null, a("10.5.1.1")));
    }

    /**
     * sortAndFilter() tests containment with parentRule.matches(childRule.getIpAddress()), i.e. the host string
     * the rule was built from, not its network address. "10.5.200.1/16" has network 10.5.0.0 (tie with the /24)
     * but its host is outside 10.5.0.0/24, so the mixed-type overlap is not detected and both rules are kept.
     * Binary search then probes the /16 ACCEPT first for 10.5.0.5.
     */
    @Test
    public void mixedTypeOverlapWithUnalignedHostIsRejectedOrThrows() {
        IpSubnetFilter f;
        try {
            f = new IpSubnetFilter(false,
                    r("10.5.0.0", 24, REJECT), r("10.5.200.1", 16, ACCEPT), r("10.6.0.0", 16, ACCEPT));
        } catch (IllegalArgumentException expected) {
            return;
        }
        assertFalse(f.accept(null, a("10.5.0.5")), "REJECT 10.5.0.0/24 bypassed");
    }

    /**
     * Randomised check: whenever construction succeeds, the verdict must equal longest-prefix-match over the
     * input rules (the only semantics under which dropping a same-type contained rule is safe), falling back to
     * acceptIfNotFound. Prints the first counterexample.
     */
    @Test
    public void randomisedAgainstLongestPrefixMatch() throws Exception {
        randomised(false);
    }

    /**
     * Same as above but every rule is written with its network address (host bits zero), i.e. the canonical way
     * to write a CIDR. Isolates the sort-tie / signed-order issues from the host-string issue.
     */
    @Test
    public void randomisedAlignedAgainstLongestPrefixMatch() throws Exception {
        randomised(true);
    }

    private static String network(String ip, int prefix) throws Exception {
        byte[] b = java.net.InetAddress.getByName(ip).getAddress();
        int v = ((b[0] & 0xff) << 24) | ((b[1] & 0xff) << 16) | ((b[2] & 0xff) << 8) | (b[3] & 0xff);
        int m = (int) (-1L << (32 - prefix));
        v &= m;
        return ((v >>> 24) & 0xff) + "." + ((v >>> 16) & 0xff) + "." + ((v >>> 8) & 0xff) + "." + (v & 0xff);
    }

    private static void randomised(boolean aligned) throws Exception {
        String[] bases = {"10.0.0.0", "10.5.0.0", "10.5.3.0", "10.5.200.1", "10.6.0.0", "0.0.0.0", "128.0.0.0",
                          "200.0.0.0", "200.1.0.0"};
        int[] prefixes = {0, 1, 8, 16, 24, 32};
        String[] probes = {"10.0.0.1", "10.5.0.5", "10.5.1.1", "10.5.3.3", "10.5.200.1", "10.6.1.1", "10.7.0.1",
                           "200.0.0.1", "200.1.0.1", "128.0.0.1", "1.1.1.1", "255.255.255.255", "0.0.0.0"};
        Random rnd = new Random(42);
        int constructed = 0;
        int threw = 0;
        List<String> mismatches = new ArrayList<>();
        int failOpen = 0;
        int failOpenMixed = 0;
        int failOpenMixedNoWide = 0;
        List<String> mixedExamples = new ArrayList<>();
        for (int iter = 0; iter < 20000; iter++) {
            int n = 1 + rnd.nextInt(4);
            IpSubnetFilterRule[] rules = new IpSubnetFilterRule[n];
            int[] pfx = new int[n];
            for (int i = 0; i < n; i++) {
                pfx[i] = prefixes[rnd.nextInt(prefixes.length)];
                String base = bases[rnd.nextInt(bases.length)];
                rules[i] = r(aligned ? network(base, pfx[i]) : base, pfx[i], rnd.nextBoolean() ? ACCEPT : REJECT);
            }
            boolean acceptIfNotFound = rnd.nextBoolean();
            IpSubnetFilter f;
            try {
                f = new IpSubnetFilter(acceptIfNotFound, Arrays.asList(rules.clone()));
            } catch (IllegalArgumentException e) {
                threw++;
                continue;
            }
            constructed++;
            for (String p : probes) {
                InetSocketAddress addr = a(p);
                int best = -1;
                boolean ambiguous = false;
                for (int i = 0; i < n; i++) {
                    if (rules[i].matches(addr)) {
                        if (best < 0 || pfx[i] > pfx[best]) {
                            best = i;
                            ambiguous = false;
                        } else if (pfx[i] == pfx[best] && rules[i].ruleType() != rules[best].ruleType()) {
                            ambiguous = true;
                        }
                    }
                }
                if (ambiguous) {
                    continue;
                }
                boolean expected = best < 0 ? acceptIfNotFound : rules[best].ruleType() == ACCEPT;
                boolean actual = f.accept(null, addr);
                if (expected != actual) {
                    if (actual) {
                        failOpen++;
                        boolean mixed = false;
                        for (IpSubnetFilterRule x : rules) {
                            mixed |= x.ruleType() != rules[0].ruleType();
                        }
                        if (mixed) {
                            failOpenMixed++;
                            boolean wide = false;
                            for (int x : pfx) {
                                wide |= x < 2;
                            }
                            if (!wide) {
                                failOpenMixedNoWide++;
                                if (mixedExamples.size() < 6) {
                                    mixedExamples.add("acceptIfNotFound=" + acceptIfNotFound + " rules="
                                            + describe(rules, pfx) + " probe=" + p);
                                }
                            }
                        }
                    }
                    mismatches.add("acceptIfNotFound=" + acceptIfNotFound + " rules=" + describe(rules, pfx)
                            + " probe=" + p + " expected=" + expected + " actual=" + actual);
                }
            }
        }
        System.out.println("randomised aligned=" + aligned + ": constructed=" + constructed + " threw=" + threw
                + " mismatches=" + mismatches.size() + " failOpen=" + failOpen + " failOpenMixedTypes=" + failOpenMixed);
        System.out.println("  failOpen mixed-type without /0 or /1 rules: " + failOpenMixedNoWide);
        for (String e : mixedExamples) {
            System.out.println("  MIXED " + e);
        }
        for (int i = 0; i < Math.min(3, mismatches.size()); i++) {
            System.out.println("  " + mismatches.get(i));
        }
        assertEquals(0, mismatches.size(), mismatches.isEmpty() ? "" : mismatches.get(0));
    }

    private static String describe(IpSubnetFilterRule[] rules, int[] pfx) {
        StringBuilder sb = new StringBuilder("[");
        for (int i = 0; i < rules.length; i++) {
            sb.append(rules[i].getIpAddress()).append('/').append(pfx[i]).append(' ')
              .append(rules[i].ruleType()).append(i + 1 < rules.length ? ", " : "]");
        }
        return sb.toString();
    }
}
