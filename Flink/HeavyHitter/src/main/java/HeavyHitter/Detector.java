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
 *  @version 30/05/2022
 *
 * Definition of the Heavy Hitter Detector node logic.
 */
package HeavyHitter;

import Parser.PcapData;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implements the detector node which identifies heavy hitter flows based on the flow dimension in bytes.
 * A flow is defined by the flow id, based on the pair <IPv4 source address, IPv4 destination address>.
 * The threshold is set by the application user.
 *
 * This is a user-defined Function that takes one argument of type Tuple4<Integer, String, Long, Long> and
 * returns a boolean value which is true if the flow is identified as heavy hitter, false otherwise.
 */
public final class Detector implements FilterFunction<Tuple4<Long, String, Long, Long>> {

    private static final Logger LOG = LoggerFactory.getLogger(Detector.class);

    private long th;

    /**
     * Constructor.
     * @param threshold value used for the heavy hitter flow detection
     */
    public Detector(long threshold) {
        this.th = threshold;
    }

    /**
     * This operation identifies heavy hitter flows and filters away the rest of the traffic.
     * @param t packet tuple with the accumulated byte length value for the given flow
     * @return true if the tuple identifies a Heavy Hitter flow, false otherwise
     */
    @Override
    public boolean filter(Tuple4<Long, String, Long, Long> t) {
        if (t.f2 <= this.th) {
            LOG.debug("[Detector] discarded pkt: " + PcapData.print(t) + " (" + t.f2 + "<=" + this.th + ")");
            return false;    // discard current tuple (it identifies a normal-sized flow)
        } else {
            LOG.debug("[Detector] sent to sink pkt: " + PcapData.print(t) + " (" + t.f2 + ">" + this.th + ")");
            return true;    // include current tuple in the output stream (it identifies a Heavy Hitter flow)
        }
    }
}
