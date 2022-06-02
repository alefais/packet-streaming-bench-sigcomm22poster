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
 * Definition of the Byte-Summing Reducer function logic.
 */
package HeavyHitter;

import Parser.PcapData;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implements the byte sum accumulator node which accumulates the number of bytes per flow per time window.
 * A flow is defined by the flow id, based on the pair <IPv4 source address, IPv4 destination address>.
 *
 * This is a user-defined ReduceFunction that takes two arguments of type Tuple4<Long, String, Long, Long> and
 * returns a new tuple with the accumulated length in bytes ({@code Tuple4<Long, String, Long, Long>}).
 */
public final class ByteSummingReducer implements ReduceFunction<Tuple4<Long, String, Long, Long>> {

    private static final Logger LOG = LoggerFactory.getLogger(ByteSummingReducer.class);

    /**
     * Given two packet tuples as input, this operation defines the sum of the lengths (in bytes) of the two packets.
     * Packets are processed in time windows and belong to the same flow (per-key time-based window computation).
     * @param t1 packet tuple
     * @param t2 packet tuple
     * @return result packet tuple with the accumulated byte length value
     */
    @Override
    public Tuple4<Long, String, Long, Long> reduce(Tuple4<Long, String, Long, Long> t1, Tuple4<Long, String, Long, Long> t2) {
        Tuple4<Long, String, Long, Long> result = new Tuple4<>(t1.f0, t1.f1, t1.f2 + t2.f2, t1.f3);
        LOG.debug("[WinOp] " + PcapData.print(result));
        return result;
    }
}
