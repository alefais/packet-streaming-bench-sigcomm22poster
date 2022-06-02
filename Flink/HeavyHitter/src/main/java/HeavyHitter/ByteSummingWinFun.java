/*
 * Copyright (C) 2022 Università di Pisa
 * Copyright (C) 2022 Alessandra Fais
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 *   1. Redistributions of source code must retain the above copyright
 *      notice, this list of conditions and the following disclaimer.
 *   2. Redistributions in binary form must reproduce the above copyright
 *      notice, this list of conditions and the following disclaimer in the
 *      documentation and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE AUTHOR AND CONTRIBUTORS ``AS IS'' AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED.  IN NO EVENT SHALL THE AUTHOR OR CONTRIBUTORS BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS
 * OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
 * LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
 * OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 */

/**
 *  @author  Alessandra Fais
 *  @version 31/05/2022
 *
 * Definition of the Byte-Summing Window Function node logic.
 */
package HeavyHitter;

import Parser.PcapData;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implements the byte sum accumulator node which accumulates the number of bytes per flow per time window.
 * A flow is defined by the flow id, based on the pair <IPv4 source address, IPv4 destination address>.
 *
 * This is a user-defined WindowFunction that explicitly iterates over the values in the current window and
 * returns a new tuple with the accumulated length in bytes ({@code Tuple4<Long, String, Long, Long>}).
 */
public final class ByteSummingWinFun
        implements WindowFunction<Tuple4<Long, String, Long, Long>, Tuple4<Long, String, Long, Long>, Long, TimeWindow> {

    private static final Logger LOG = LoggerFactory.getLogger(ByteSummingWinFun.class);

    /** Given two packet tuples as input, this operation defines the sum of the lengths (in bytes) of the two packets.
     *  Packets are processed in time windows and belong to the same flow (per-key time-based window computation).
     * @param key value representing the flow identifier
     * @param win window of tuples
     * @param values set of tuples currently in the window
     * @param out results
     */
    @Override
    public void apply(
            Long key,
            TimeWindow win,
            Iterable<Tuple4<Long, String, Long, Long>> values,
            Collector<Tuple4<Long, String, Long, Long>> out) {
        String host = null;
        long ts = 0L;
        long sum = 0L;
        long win_len = 0L;
        for (Tuple4<Long, String, Long, Long> value : values) {
            sum += value.f2;
            if (host == null) {
                host = value.f1;
                ts = value.f3;
            }
            win_len++;
        }
        Tuple4<Long, String, Long, Long> result = new Tuple4<>(key, host, sum, ts);
        LOG.debug("[WinOp-#" + win_len + "] " + PcapData.print(result));
        out.collect(result);
    }
}
