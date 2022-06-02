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
 * Definition of the Source node logic.
 */
package HeavyHitter;

import Parser.PcapData;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;

import java.util.ArrayList;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implements the source node that generates the input stream based on the pcap file content.
 *
 * This is a user-defined {@code RichParallelSourceFunction} class that takes a dataset of packets
 * (created by the PcapData class) and generates a stream of {@code Tuple4<String, ...>} objects
 * representing network packets' content.
 */
public class DataSource extends RichParallelSourceFunction<Tuple4<String, String, String, String>> {

    private static final Logger LOG = LoggerFactory.getLogger(DataSource.class);
    private volatile boolean running = false;

    private final String input_file;
    private ArrayList<String[]> dataset;
    private final int rate;
    private final long app_run_time;
    private long app_start_time;
    private int generated_pkts;
    private int generations;

    /**
     * Constructor.
     * @param input_pcap_file input dataset file
     * @param app_run_time application run time in seconds
     * @param gen_rate rate of generation
     *                 if the argument value is -1 then the spout generates tuples at
     *                 the maximum rate possible (measure the bandwidth under this assumption);
     *                 if the argument value is different from -1 then the source generates
     *                 tuples at the rate given by this parameter (measure the latency given
     *                 this generation rate)
     */
    public DataSource(String input_pcap_file, int app_run_time, int gen_rate) {
        this.app_run_time = TimeUnit.NANOSECONDS.convert(app_run_time, TimeUnit.SECONDS);
        this.input_file = input_pcap_file;
        this.rate = gen_rate;
        this.generated_pkts = 0;     // number of generated packets
        this.generations = 0;        // number of times the dataset is replayed
        LOG.debug("[Source] Created operator.");
    }

    /**
     * Initialization method for the Source function, it is called before the actual working methods.
     * @param config configuration
     */
    @Override
    public void open(Configuration config) {
        app_start_time = System.nanoTime();
        LOG.debug("[Source] Started operator.");

        dataset = PcapData.parseDataset(input_file);
        LOG.debug("[Source] Dataset ready to use at the source operator (" + dataset.size() + " extracted packets).");

        running = true;
    }

    /**
     * Main method of the source operator.
     * @param context source operator context to manage tuple emission/generation
     */
    @Override
    public void run(SourceContext<Tuple4<String, String, String, String>> context) {
        int interval = 1000000000;                // one second (nanoseconds)
        long current_time = System.nanoTime();    // get the current time
        long emitted_pkts = 0;
        try {
            LOG.debug("[Source]" + " check times: cur " + current_time
                    + ", app_start " + app_start_time + ", app_run " + app_run_time
                    + ", while(" + (current_time - app_start_time <= app_run_time) + ")");
            while ((current_time - app_start_time <= app_run_time) && running) {
                for (String[] pkt : dataset) {
                    Tuple4<String, String, String, String> tuple = new Tuple4<>(pkt[1], pkt[2], pkt[6], pkt[0]);
                    if (rate == -1) {       // at the maximum possible rate
                        //context.collectWithTimestamp(tuple, System.nanoTime());
                        context.collect(tuple);
                        generated_pkts++;
                    } else {                // at the given rate
                        long now = System.nanoTime();
                        if (emitted_pkts >= rate) {
                            if (now - current_time <= interval)
                                active_delay(interval - (now - current_time));
                            emitted_pkts = 0;
                        }
                        //context.collectWithTimestamp(tuple, System.nanoTime());
                        context.collect(tuple);
                        emitted_pkts++;
                        generated_pkts++;
                        active_delay((double) interval / rate);
                    }
                }
                //context.emitWatermark(new Watermark(System.nanoTime()));
                current_time = System.nanoTime();
                generations++;
                LOG.debug("[Source]" + " generated packets " + generated_pkts + ", generations " + generations);
                LOG.debug("[Source]" + " check times: cur " + current_time
                        + ", app_start " + app_start_time + ", app_run " + app_run_time
                        + ", while(" + (current_time - app_start_time <= app_run_time && running) + ")");
            }
        } catch (NullPointerException e) {
            LOG.error("Empty dataset!", e);
        }
    }

    @Override
    public void cancel() { }

    @Override
    public void close() {
        running = false;
        stats();
    }

    private void stats() {
        LOG.debug("[Source] Stopped operator.");

        // prints some throughput stats
        long execution = TimeUnit.MILLISECONDS.convert(System.nanoTime() - app_start_time, TimeUnit.NANOSECONDS);
        LOG.info("[MEASURE] source exec time " + execution + " ms" +
                ", generated " + generated_pkts + " tuples (" + generations + " gens)" +
                ", bw " + (generated_pkts / execution * 1000) + " tuples/second.");
    }


    /**
     * Add some active delay (busy-waiting function).
     * @param nsecs wait time in nanoseconds
     */
    private void active_delay(double nsecs) {
        long t_start = System.nanoTime();
        long t_now;
        boolean end = false;

        while (!end) {
            t_now = System.nanoTime();
            end = (t_now - t_start) >= nsecs;
        }
        LOG.debug("[Source] delay " + nsecs + " ns.");
    }
}