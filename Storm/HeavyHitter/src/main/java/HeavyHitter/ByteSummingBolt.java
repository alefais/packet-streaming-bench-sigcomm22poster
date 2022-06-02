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
 * Definition of the Byte-Summing Bolt node logic.
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
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Implements the byte sum accumulator node which accumulates the number of bytes per flow per time window.
 * A flow is defined by the flow id, based on the pair <IPv4 source address, IPv4 destination address>.
 *
 * This is a user-defined BaseRichBolt that explicitly iterates over the values in the current window and
 * returns a new tuple with the accumulated length in bytes ({@code Tuple4<Long, String, Long, Long>}).
 */
public final class ByteSummingBolt extends BaseRichBolt {

    private static final Logger LOG = LoggerFactory.getLogger(ByteSummingBolt.class);

    protected OutputCollector collector;
    protected TopologyContext context;

    private long op_start_time;
    private long processed;

    private final int window_len;
    private final int window_slide;
    private Map<Long, LinkedList<Long>> flowIDtoWindow;
    private Map<Long, Long> flowIDtoByteSum;

    /**
     * Constructor.
     * In the current implementation, the window slide is always 1, meaning the window always
     * moves of 1 position for each new incoming packet to process.
     * @param win_len length of the window (number of items)
     * @param win_slide number of positions discarded each time the window moves
     */
    public ByteSummingBolt(int win_len, int win_slide) {
        this.window_len = win_len;
        this.window_slide = win_slide;
    }

    /**
     * Initialization method for the ByteSummingBolt, it is called before the actual working methods.
     * @param map contains configuration properties
     * @param topologyContext topology execution context
     * @param outputCollector collector of the spout operator
     */
    @Override
    public void prepare(Map<String, Object> map, TopologyContext topologyContext, OutputCollector outputCollector) {
        context = topologyContext;
        collector = outputCollector;
        LOG.debug("[ByteSum] Started operator.");

        op_start_time = System.nanoTime();
        processed = 0;

        flowIDtoWindow = new HashMap<>();
        flowIDtoByteSum = new HashMap<>();
    }

    /**
     * Given a packet tuple as input, this operation defines the sum of the lengths (in bytes) of the packets
     * which are currently part of the respective window and belong to the same flow (per-key count-based window computation).
     * @param tuple packet fields received from the spout
     * @return new tuple containing fields <flowID, dstIP, accumulatedByteLen, timestamp> as value
     */
    @Override
    public void execute(Tuple tuple) {
        // extract relevant values
        Long flowID = tuple.getLongByField(Constants.Field.FLOW_ID);
        String dstIP = tuple.getStringByField(Constants.Field.DST_IP);
        Long len = tuple.getLongByField(Constants.Field.PKT_LEN);
        Long ts = tuple.getLongByField(Constants.Field.TIMESTAMP);

        long accumulatedByteSum = movingByteSum(flowID, len);
        var result = new Values(flowID, dstIP, accumulatedByteSum, ts);
        collector.emit(result);
        processed++;
        LOG.debug("[ByteSum] " + PcapData.print(result));
    }

    @Override
    public void cleanup() {
        LOG.debug("[ByteSum] Stopped operator.");

        // prints some throughput stats
        long execution = TimeUnit.MILLISECONDS.convert(System.nanoTime() - op_start_time, TimeUnit.NANOSECONDS);
        LOG.info("[ByteSum] bytesum exec time " + execution + " ms" +
                ", processed " + processed + " tuples" +
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

    /**
     * Manages explicitly count-based windows of packet length values related to a given flowID.
     * The current accumulated sum value is stored as well, and updated as the window moves.
     * @param flowID value identifying a flow
     * @param nextPktLen value of the length in bytes of the currently processed packet
     * @return updated byte sum value
     */
    private long movingByteSum(long flowID, long nextPktLen) {
        LinkedList<Long> window = new LinkedList<>();
        long sum = 0L;

        if (flowIDtoWindow.containsKey(flowID)) {   // known flow
            window = flowIDtoWindow.get(flowID);
            sum = flowIDtoByteSum.get(flowID);
            if (window.size() > window_len - 1) {
                double valueToRemove = window.removeFirst();
                sum -= valueToRemove;
            }
            window.addLast(nextPktLen);
            flowIDtoWindow.put(flowID, window);
            sum += nextPktLen;
            flowIDtoByteSum.put(flowID, sum);
            return sum;
        } else {    // new flow
            window.add(nextPktLen);
            flowIDtoWindow.put(flowID, window);
            flowIDtoByteSum.put(flowID, nextPktLen);
            return nextPktLen;
        }
    }
}
