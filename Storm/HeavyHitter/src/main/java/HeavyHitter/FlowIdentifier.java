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
 *  @version 01/06/2022
 *
 * Definition of the Flow Identifier node logic.
 */
package HeavyHitter;

import Parser.PcapData;
import Util.Constants;
import Util.Flow;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Implements the flow identifier node that computes a flow ID value as all the incoming traffic is analyzed.
 * A flow is defined here in a relaxed way, by only considering the pair <IPv4 source address, IPv4 destination address>.
 *
 * This is a user-defined BaseRichBolt that takes a packet fields as Strings and produces a tuples marked with the flow ID.
 */
public final class FlowIdentifier extends BaseRichBolt {

    private static final Logger LOG = LoggerFactory.getLogger(FlowIdentifier.class);

    protected OutputCollector collector;
    protected TopologyContext context;

    private long op_start_time;
    private long processed;

    /**
     * Initialization method for the FlowID function, it is called before the actual working methods.
     * @param map contains configuration properties
     * @param topologyContext topology execution context
     * @param outputCollector collector of the spout operator
     */
    @Override
    public void prepare(Map<String, Object> map, TopologyContext topologyContext, OutputCollector outputCollector) {
        context = topologyContext;
        collector = outputCollector;
        LOG.debug("[FlowID] Started operator.");

        op_start_time = System.nanoTime();
        processed = 0;
    }

    /**
     * Given a packet tuple as input, the flow value is computed and sent as new output field along with
     * the received packet information.
     * @param tuple packet fields received from the spout
     * @return new tuple containing fields <flowID, dstIP, pktLen, timestamp> as value
     */
    @Override
    public void execute(Tuple tuple) {
        // extract relevant values
        String srcIP = tuple.getStringByField(Constants.Field.SRC_IP);
        String dstIP = tuple.getStringByField(Constants.Field.DST_IP);
        Long len = Long.valueOf(tuple.getStringByField(Constants.Field.PKT_LEN));
        Long ts = Long.valueOf(tuple.getStringByField(Constants.Field.TIMESTAMP));

        // compute flow information
        Flow flow = new Flow(srcIP, dstIP, 1);
        int flowID = flow.hashCode();

        var result = new Values((long) flowID, dstIP, len, ts);
        collector.emit(result);
        processed++;
        LOG.debug("[FlowID] " + PcapData.print(result));
    }

    @Override
    public void cleanup() {
        LOG.debug("[FlowID] Stopped operator.");

        // prints some throughput stats
        long execution = TimeUnit.MILLISECONDS.convert(System.nanoTime() - op_start_time, TimeUnit.NANOSECONDS);
        LOG.info("[FlowID] flowid exec time " + execution + " ms" +
                ", processed " + processed + " tuples" +
                ", bw " + (processed / execution * 1000) + " tuples/second.");
    }

    /**
     * Declares the output type of the bolt operator.
     * @param outputFieldsDeclarer type of the generated tuples
     */
    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields(Constants.Field.FLOW_ID, Constants.Field.DST_IP, Constants.Field.PKT_LEN, Constants.Field.TIMESTAMP));
    }
}