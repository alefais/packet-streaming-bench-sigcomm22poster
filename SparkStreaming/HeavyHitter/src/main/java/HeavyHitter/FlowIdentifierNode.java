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
 *  @version 25/05/2022
 *
 * Definition of the Flow Identifier node logic.
 */
package HeavyHitter;

import Parser.PcapData;
import org.apache.spark.api.java.function.PairFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;
import scala.Tuple4;
import Util.Flow;

/**
 * Implements the flow identifier node that computes a flow ID value as all the incoming traffic is analyzed.
 * A flow is defined here in a relaxed way, by only considering the pair <IPv4 source address, IPv4 destination address>.
 *
 * This is a user-defined PairFunction that takes a packet (String[]) and produces a Pair with the flow ID as key, and
 * a 4-tuple with the relevant packet fields as value ({@code Tuple2<Integer, Tuple4<Integer, String, Long, Long>>}).
 */
public final class FlowIdentifierNode
        implements PairFunction<String[], Integer, Tuple4<Integer, String, Long, Long>> {

    private static final Logger LOG = LoggerFactory.getLogger(FlowIdentifierNode.class);

    /**
     * Given a packet tuple as input, the flow value is computed and a pair is produced as output:
     * the first field is the flow id (key) and the second is a four-value tuple of relevant packet content (value).
     * @param pkt_fields packet fields as parsed from input
     * @return key-value pair with flow is as key and a tuple containing fields <flowID, dstIP, pktLen, timestamp> as value
     * @throws Exception
     */
    @Override
    public Tuple2<Integer, Tuple4<Integer, String, Long, Long>> call(String[] pkt_fields) throws Exception {
        // extract relevant values
        String srcIP = pkt_fields[1];
        String dstIP = pkt_fields[2];
        Long len = Long.valueOf(pkt_fields[6]);
        Long ts = Long.valueOf(pkt_fields[0]);

        // compute flow information
        Flow flow = new Flow(srcIP, dstIP, 1);
        Integer flowID = flow.hashCode();

        Tuple2<Integer, Tuple4<Integer, String, Long, Long>> result = new Tuple2<>(flowID, new Tuple4<>(flowID, dstIP, len, ts));
        LOG.debug("[FlowId] sent pkt: " + PcapData.print(result._2()));
        return result;
    }
}