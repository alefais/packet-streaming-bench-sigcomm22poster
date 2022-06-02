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
 * Definition of the Sink node logic.
 */
package HeavyHitter;

import Parser.PcapData;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;

public class Sink implements SinkFunction<Tuple4<Long, String, Long, Long>> {

    private static final Logger LOG = LoggerFactory.getLogger(Sink.class);

    private final ArrayList<String> hosts;
    private int received;

    /**
     * Constructor.
     */
    public Sink() {
        LOG.debug("[Sink] Created operator.");
        hosts = new ArrayList<>();
    }

    /**
     * Method called for each tuple arriving at the (last) sink operator.
     * It dumps statistics about the Heavy Hitters discovered (destination hosts hit by at least one elephant flows in the time window).
     * @param t input tuple
     * @param cx context related to the input record
     */
    @Override
    public void invoke(Tuple4<Long, String, Long, Long> t, SinkFunction.Context cx) {
        received++;
        hosts.add(t.f1);
    }

    @Override
    public void finish() throws Exception {
        LOG.debug("[Sink] Stopped operator.");
        PcapData.printHeavyHitters(hosts);

        // prints some throughput stats
        LOG.info("[MEASURE] sink received " + received + " heavy hitters.");
    }
}
