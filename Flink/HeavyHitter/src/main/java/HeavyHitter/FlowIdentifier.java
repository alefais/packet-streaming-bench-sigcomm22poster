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
 * Definition of the Flow Identifier node logic.
 */
package HeavyHitter;

import Parser.PcapData;
import Util.Flow;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implements the flow identifier node that computes a flow ID value as all the incoming traffic is analyzed.
 * A flow is defined here in a relaxed way, by only considering the pair <IPv4 source address, IPv4 destination address>.
 *
 * This is a user-defined PairFunction that takes a packet (String[]) and produces a Pair with the flow ID as key, and
 * a 4-tuple with the relevant packet fields as value ({@code Tuple2<Integer, Tuple4<Integer, String, Long, Long>>}).
 */
public final class FlowIdentifier
        implements MapFunction<Tuple4<String, String, String, String>, Tuple4<Long, String, Long, Long>> {

    private static final Logger LOG = LoggerFactory.getLogger(FlowIdentifier.class);

    /**
     * Given a packet tuple as input, the flow value is computed and a pair is produced as output:
     * the first field is the flow id (key) and the second is a four-value tuple of relevant packet content (value).
     * @param pkt_fields packet fields as parsed from input
     * @return key-value pair with flow is as key and a tuple containing fields <flowID, dstIP, pktLen, timestamp> as value
     */
    @Override
    public Tuple4<Long, String, Long, Long> map(Tuple4<String, String, String, String> pkt_fields) {
        // extract relevant values
        String srcIP = pkt_fields.f0;
        String dstIP = pkt_fields.f1;
        Long len = Long.valueOf(pkt_fields.f2);
        Long ts = Long.valueOf(pkt_fields.f3);

        // compute flow information
        Flow flow = new Flow(srcIP, dstIP, 1);
        int flowID = flow.hashCode();

        Tuple4<Long, String, Long, Long> result = new Tuple4<>((long) flowID, dstIP, len, ts);
        LOG.debug("[FlowID] " + PcapData.print(result));
        return result;
    }
}