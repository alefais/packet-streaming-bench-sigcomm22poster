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
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.receiver.Receiver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Implements the source node that generates the input stream based on the pcap file content.
 *
 * This is a user-defined Receiver class that takes a dataset of packets (created by the PcapData class) and
 * generates a stream of {@code String[]} objects representing network packets' content.
 */
public class Source extends Receiver<String[]> {

    private static final Logger LOG = LoggerFactory.getLogger(Source.class);

    private final int parallelism;
    private final int rate;
    private final long app_run_time;

    private Thread replica;
    static AtomicInteger replica_id = new AtomicInteger();

    /**
     * Constructor.
     * @param sl storage level type
     * @param input_pcap_file input dataset file
     * @param source_pardeg parallelism degree
     * @param gen_rate rate of generation
     */
    public Source(StorageLevel sl, int app_run_time, String input_pcap_file, int source_pardeg, int gen_rate) {
        super(sl);
        this.parallelism = source_pardeg;
        this.rate = gen_rate;
        this.app_run_time = TimeUnit.NANOSECONDS.convert(app_run_time, TimeUnit.SECONDS);

        PcapData.parseDataset(input_pcap_file);
    }

    /**
     * Implements the logic to execute when the source is started.
     */
    @Override
    public void onStart() {
        LOG.debug("[Source] on Start()");

        // creates one source replica thread
        this.replica = new Thread(new SourceReplica(replica_id.getAndIncrement()));
        this.replica.start();
    }

    /**
     * Implements the logic to execute when the source is stopped.
     * @throws RuntimeException
     */
    @Override
    public void onStop() throws RuntimeException {
        try {
            // stops the source replica thread
            this.replica.join();
            LOG.debug("[Source] Stopped replica: " + isStopped());
        } catch (InterruptedException e) {
                throw new RuntimeException(e);
        }
    }

    /**
     * This class defines the implementation of a source replica thread.
     */
    class SourceReplica implements Runnable {
        private final int thread_id;
        private long src_start_time;

        private int generated_pkts;
        private int generations;

        /**
         * Constructor.
         * @param th_id integer id of the thread replica
         */
        SourceReplica(int th_id) {
            this.thread_id = th_id;
            this.generated_pkts = 0;     // number of generated packets
            this.generations = 0;        // number of times the dataset is replayed
            LOG.debug("[Source] Created replica #" + thread_id);
        }

        /**
         * Main method of the source operator.
         */
        @Override
        public void run() {
            LOG.debug("[Source] Started replica #" + thread_id);
            this.src_start_time = System.nanoTime();         // source thread start time

            // entire generation logic
            this.generate();
            LOG.debug("[Source] Stopped replica #" + thread_id);

            // prints some throughput stats
            long execution = TimeUnit.MILLISECONDS.convert(System.nanoTime() - src_start_time, TimeUnit.NANOSECONDS);
            System.out.println("[MEASURE] source#" + thread_id +
                            ", exec time " + execution + " ms" +
                            ", generated " + generated_pkts + " tuples (" + generations + " gens)" +
                            ", bw " + (generated_pkts / execution * 1000) + " tuples/second.");
        }

        /**
         * Returns information on the generated data.
         * @return the pair of values corresponding to the total number of generated tuples
         *         and the number of times the dataset has been replayed
         */
        public Tuple2<Integer, Integer> getGenInfo() {
            return new Tuple2<>(generated_pkts, generations);
        }

        /**
         * Defines the entire source operator logic to generate the stream.
         */
        private void generate() {
            int interval = 1000000000;                // one second (nanoseconds)
            long current_time = System.nanoTime();    // get the current time
            long stored_pkts = 0;
            try {
                LOG.debug("[Source-#" + thread_id + "] check times: cur " + current_time
                        + ", app_start " + src_start_time + ", app_run " + app_run_time
                        + ", while(" + (current_time - src_start_time <= app_run_time) + " && " + !isStopped() + ")");
                while ((current_time - src_start_time <= app_run_time) && !isStopped()) {
                    for (String[] pkt : PcapData.PACKETS) {
                        if (rate == -1) {       // at the maximum possible rate
                            store(pkt);
                            LOG.debug("[Source-#" + thread_id + "] stored pkt: " + PcapData.print(pkt));
                            generated_pkts++;
                        } else {                // at the given rate
                            long now = System.nanoTime();
                            if (stored_pkts >= rate) {
                                if (now - current_time <= interval)
                                    active_delay(interval - (now - current_time));
                                stored_pkts = 0;
                            }
                            store(pkt);
                            LOG.debug("[Source-#" + thread_id + "] stored pkt: " + PcapData.print(pkt));
                            stored_pkts++;
                            generated_pkts++;
                            active_delay((double) interval / rate);
                        }
                    }
                    current_time = System.nanoTime();
                    generations++;
                    LOG.debug("[Source-#" + thread_id + "] generated packets " + generated_pkts + ", generations " + generations);
                    LOG.debug("[Source-#" + thread_id + "] check times: cur " + current_time
                            + ", app_start " + src_start_time + ", app_run " + app_run_time
                            + ", while(" + (current_time - src_start_time <= app_run_time) + " && " + !isStopped() + ")");
                }
            } catch (NullPointerException e) {
                //restart("Empty dataset!", e);
                LOG.error(("[Source] Empty dataset: " + e.getMessage()));
            } catch (Throwable e) {
                //restart("Error in source node...", e);
                LOG.error("[Source] Error: " + e.getMessage());
            }
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
}