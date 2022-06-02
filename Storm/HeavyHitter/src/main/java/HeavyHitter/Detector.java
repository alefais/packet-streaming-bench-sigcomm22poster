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
 * Definition of the Heavy Hitter Detector node logic.
 */
package HeavyHitter;

import Util.Constants;
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
 * Implements the detector node which identifies heavy hitter flows based on the flow dimension in bytes.
 * A flow is defined by the flow id, based on the pair <IPv4 source address, IPv4 destination address>.
 * The threshold is set by the application user.
 *
 * This is a user-defined Function that takes one argument of type Tuple4<Integer, String, Long, Long> and
 * returns a boolean value which is true if the flow is identified as heavy hitter, false otherwise.
 */
public final class Detector extends BaseRichBolt {

    private static final Logger LOG = LoggerFactory.getLogger(Detector.class);

    protected OutputCollector collector;
    protected TopologyContext context;

    private long op_start_time;
    private long processed;
    private long detected;
    private final long th;

    /**
     * Constructor.
     * @param threshold value used for the heavy hitter flow detection
     */
    public Detector(long threshold) {
        this.th = threshold;
    }

    /**
     * Initialization method for the Detector function, it is called before the actual working methods.
     * @param map contains configuration properties
     * @param topologyContext topology execution context
     * @param outputCollector collector of the spout operator
     */
    @Override
    public void prepare(Map<String, Object> map, TopologyContext topologyContext, OutputCollector outputCollector) {
        context = topologyContext;
        collector = outputCollector;
        LOG.debug("[Detector] Started operator.");

        op_start_time = System.nanoTime();
        processed = 0;
        detected = 0;
    }

    /**
     * This operation identifies heavy hitter flows and filters away the rest of the traffic.
     * @param tuple packet tuple with the accumulated byte length value for the given flow
     * @return true if the tuple identifies a Heavy Hitter flow, false otherwise
     */
    @Override
    public void execute(Tuple tuple) {
        // extract relevant values
        Long flowID = tuple.getLongByField(Constants.Field.FLOW_ID);
        String dstIP = tuple.getStringByField(Constants.Field.DST_IP);
        Long acc_sum = tuple.getLongByField(Constants.Field.ACC_BYTE_SUM);
        Long ts = tuple.getLongByField(Constants.Field.TIMESTAMP);

        if (acc_sum <= this.th) {   // discard current tuple (it identifies a normal-sized flow)
            LOG.debug("[Detector] discarded pkt (" + acc_sum + "<=" + this.th + ")");
        } else {                    // include current tuple in the output stream (it identifies a Heavy Hitter flow)
            LOG.debug("[Detector] sent to sink pkt: (" + acc_sum + ">" + this.th + ")");
            detected++;
            collector.emit(new Values(flowID, dstIP, acc_sum, ts));
        }
        processed++;
    }

    @Override
    public void cleanup() {
        LOG.debug("[Detector] Stopped operator.");

        // prints some throughput stats
        long execution = TimeUnit.MILLISECONDS.convert(System.nanoTime() - op_start_time, TimeUnit.NANOSECONDS);
        LOG.info("[Detector] detector exec time " + execution + " ms" +
                ", processed " + processed + " tuples" +
                ", detected " + detected + " heavy hitters" +
                ", bw " + (processed / execution * 1000) + " tuples/second.");
    }

    /**
     * Declares the output type of the bolt operator.
     * @param outputFieldsDeclarer type of the generated tuples
     */
    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields(Constants.Field.FLOW_ID, Constants.Field.DST_IP, Constants.Field.ACC_BYTE_SUM, Constants.Field.TIMESTAMP));
    }
}
