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
 * Definition of the Sink node logic.
 */
package HeavyHitter;

import Parser.PcapData;

import Util.Constants;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Map;

public class Sink extends BaseRichBolt {

    private static final Logger LOG = LoggerFactory.getLogger(Sink.class);

    protected OutputCollector collector;
    protected TopologyContext context;

    private long op_start_time;
    private long processed;
    private final ArrayList<String> hosts;

    /**
     * Constructor.
     */
    public Sink() {
        hosts = new ArrayList<>();
    }

    /**
     * Initialization method for the Sink function, it is called before the actual working methods.
     * @param map contains configuration properties
     * @param topologyContext topology execution context
     * @param outputCollector collector of the spout operator
     */
    @Override
    public void prepare(Map<String, Object> map, TopologyContext topologyContext, OutputCollector outputCollector) {
        context = topologyContext;
        collector = outputCollector;
        LOG.debug("[Sink] Started operator.");

        op_start_time = System.nanoTime();
        processed = 0;
    }

    /**
     * Method called for each tuple arriving at the (last) sink operator.
     * It dumps statistics about the Heavy Hitters discovered (destination hosts hit by at least one elephant flows in the time window).
     * @param tuple packet tuple
     */
    @Override
    public void execute(Tuple tuple) {
        hosts.add(tuple.getStringByField(Constants.Field.DST_IP));
        processed++;
    }

    @Override
    public void cleanup() {
        LOG.debug("[Sink] Stopped operator.");
        PcapData.printHeavyHitters(hosts);

        // prints some throughput stats
        LOG.info("[MEASURE] sink received " + processed + " heavy hitters.");
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
