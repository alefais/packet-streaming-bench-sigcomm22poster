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
 * @author Alessandra Fais
 * @version 01/06/2022
 *
 * Main class of the Heavy Hitter application.
 */
package HeavyHitter;

import Util.AppConfig;
import Util.Constants;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class HeavyHitter {

    private static final Logger LOG = LoggerFactory.getLogger((HeavyHitter.class));

    /**
     * Main class implementing the Heavy Hitter Storm application.
     * @param args command line arguments
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        // Check and parse input parameters
        if (args.length == 1 && args[0].equals(Constants.InputParameters.HELP)) {
            String alert =
                    "In order to correctly run HeavyHitter app you can pass the following (optional) arguments:\n" +
                    "Optional arguments (default values are specified in hh.properties or defined as constants):\n" +
                    " file path\n" +
                    " source parallelism degree\n" +
                    " flowid parallelism degree\n" +
                    " accumulator parallelism degree\n" +
                    " detector parallelism degree\n" +
                    " sink parallelism degree\n" +
                    " window length (milliseconds)\n" +
                    " window slide (milliseconds)\n" +
                    " batch length (milliseconds)\n" +
                    " generation rate (default -1, source generates at the max possible rate)\n" +
                    " threshold\n";
            LOG.error(alert);
        } else {
            // load default configuration
            Config conf = new Config();
            conf.setDebug(false);
            conf.setNumWorkers(1);
            conf.setMaxTaskParallelism(16);

            // create object to manage app configuration properties
            AppConfig app_conf = new AppConfig();
            try {
                // load default app/topology configuration from hh.properties file
                Properties properties = loadProperties(Constants.ExecutionConf.PROPERTIES_FILE);
                app_conf.fromProperties(properties);
                LOG.debug("Loaded app configuration file {}.", Constants.ExecutionConf.PROPERTIES_FILE);
            } catch (IOException e) {
                LOG.error("Unable to load app configuration file.", e);
                throw new RuntimeException("Unable to load app configuration file.", e);
            }

            // parse command line arguments
            String input_pcap_file = (args.length > 0) ? args[0] : app_conf.getString(Constants.ExecutionConf.INPUT_FILE);
            int source_pardeg = (args.length > 1) ? Integer.parseInt(args[1]) : app_conf.getInt(Constants.ExecutionConf.SOURCE_THREADS);
            int flowid_pardeg = (args.length > 2) ? Integer.parseInt(args[2]) : app_conf.getInt(Constants.ExecutionConf.FLOWID_THREADS);
            int acc_pardeg = (args.length > 3) ? Integer.parseInt(args[3]) : app_conf.getInt(Constants.ExecutionConf.ACC_THREADS);
            int detector_pardeg = (args.length > 4) ? Integer.parseInt(args[4]) : app_conf.getInt(Constants.ExecutionConf.DETECTOR_THREADS);
            int sink_pardeg = (args.length > 5) ? Integer.parseInt(args[5]) : app_conf.getInt(Constants.ExecutionConf.SINK_THREADS);

            int win_len = (args.length > 6) ? Integer.parseInt(args[6]) : app_conf.getInt(Constants.AppConf.WIN_LEN);
            int win_slide = (args.length > 7) ? Integer.parseInt(args[7]) : app_conf.getInt(Constants.AppConf.WIN_SLIDE);
            int th = (args.length > 8) ? Integer.parseInt(args[8]) : app_conf.getInt(Constants.AppConf.THRESHOLD);
            boolean chaining = (args.length > 9) ? Boolean.parseBoolean(args[9]) : app_conf.getBoolean(Constants.AppConf.CHAINING);
            int gen_rate = (args.length > 10) ? Integer.parseInt(args[10]) : app_conf.getInt(Constants.AppConf.GEN_RATE); // source generation rate (for tests)
            int app_runtime = app_conf.getInt(Constants.AppConf.APP_RUNTIME);

            // set up the execution environment

            LOG.info("[main] Command line arguments parsed and configuration set.");

            // prepare the application topology

            TopologyBuilder builder = new TopologyBuilder();
            builder
                .setSpout(Constants.Components.SOURCE,
                        new DataSource(input_pcap_file, app_runtime, gen_rate),
                        source_pardeg);

            builder
                .setBolt(Constants.Components.FLOW_ID,
                        new FlowIdentifier(),
                        flowid_pardeg)
                .shuffleGrouping(Constants.Components.SOURCE);

            builder
                .setBolt(Constants.Components.BYTE_ACC,
                        new ByteSummingBolt(win_len, win_slide),
                        acc_pardeg)
                .fieldsGrouping(Constants.Components.FLOW_ID, new Fields(Constants.Field.FLOW_ID));

            builder
                .setBolt(Constants.Components.DETECTOR,
                        new Detector(th),
                        detector_pardeg)
                .shuffleGrouping(Constants.Components.BYTE_ACC);

            builder
                .setBolt(Constants.Components.SINK,
                        new Sink(),
                        sink_pardeg)
                .shuffleGrouping(Constants.Components.DETECTOR);

            // build the topology
            StormTopology topology = builder.createTopology();

            // print app info
            StringBuilder summary = new StringBuilder("[CONFIG] HH application configured as: ");
            summary.append("* source rate: ").append(gen_rate).append(", ");
            summary.append("* chaining: ").append((chaining) ? "ON" : "OFF").append(", ");
            summary.append("* topology: source(").append(source_pardeg).append(") -> ");
            summary.append("flow_id(").append(flowid_pardeg).append(") -> ");
            summary.append("byte_len_acc(").append(acc_pardeg).append(") -> ");
            summary.append("detector(").append(detector_pardeg).append(") -> ");
            summary.append("sink(").append(sink_pardeg).append(")").append(", ");
            summary.append("* windows: length ").append(win_len).append(" ms, slide ").append(win_slide).append(" ms").append("\n");
            LOG.info(summary.toString());

            // start topology on local cluster
            long start_time = System.nanoTime();
            try (LocalCluster cluster = new LocalCluster()) {
                LOG.info("[main] Starting Storm in local mode to run for {} seconds...", app_runtime);

                cluster.submitTopology(Constants.Components.TOPOLOGY, conf, topology);
                LOG.info("[main] Topology {} submitted.", Constants.Components.TOPOLOGY);
                Thread.sleep((long) app_runtime * 1000);

                cluster.killTopology(Constants.Components.TOPOLOGY);
                LOG.info("[main] Topology {} finished, exiting...", Constants.Components.TOPOLOGY);

                cluster.shutdown();
                LOG.info("[main] Local Storm cluster was shutdown.");
            } catch (InterruptedException e) {
                LOG.error("Error in running topology locally.", e);
            }
            long stop_time = System.nanoTime();
            LOG.info("[MEASURE] graph execution time: " + TimeUnit.SECONDS.convert(stop_time - start_time, TimeUnit.NANOSECONDS) + "s");
        }
    }

    /**
     * Load configuration properties for the application.
     * @param filename the name of the properties file
     * @return the persistent set of properties loaded from the file
     * @throws IOException
     */
    private static Properties loadProperties(String filename) throws IOException {
        Properties properties = new Properties();
        InputStream is = HeavyHitter.class.getResourceAsStream(filename);
        if (is != null) {
            LOG.debug("[main] Reading all properties from property file hh.properties...");
            properties.load(is);
            is.close();
        } else {
            LOG.debug("[main] Property file hh.properties is null.");
        }
        LOG.debug("[main] Properties loaded: {}.", properties.toString());
        return properties;
    }
}

