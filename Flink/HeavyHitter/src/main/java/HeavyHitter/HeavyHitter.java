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
 * @version 31/05/2022
 *
 * Main class of the Heavy Hitter application.
 */
package HeavyHitter;

import Util.AppConfig;
import Util.Constants;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class HeavyHitter {

    private static final Logger LOG = LoggerFactory.getLogger((HeavyHitter.class));

    /**
     * Main class implementing the Heavy Hitter Flink application.
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
            final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();    // = new LocalStreamEnvironment();
            StreamExecutionEnvironment.setDefaultLocalParallelism(app_conf.getInt(Constants.ExecutionConf.ALL_THREADS));
            env.setMaxParallelism(app_conf.getInt(Constants.ExecutionConf.ALL_THREADS));
            env.setRestartStrategy(RestartStrategies.noRestart());
            env.setRuntimeMode(RuntimeExecutionMode.STREAMING);

            // load configuration properties and display them in the Flink UI
            ParameterTool defaultConf = ParameterTool.fromPropertiesFile(
                    HeavyHitter.class.getResourceAsStream(Constants.ExecutionConf.PROPERTIES_FILE));
            env.getConfig().setGlobalJobParameters(defaultConf);

            LOG.info("[main] Command line arguments parsed and configuration set.");

            // prepare the application topology

            DataStreamSource<Tuple4<String, String, String, String>> source =
                    env.addSource(new DataSource(input_pcap_file, app_runtime, gen_rate));
            source
                .name(Constants.Components.SOURCE)
                .setParallelism(source_pardeg)
                .returns(Types.TUPLE(Types.STRING, Types.STRING, Types.STRING, Types.STRING));

            source.disableChaining();

            DataStream<Tuple4<Long, String, Long, Long>> flowid =
                    source
                        .map(new FlowIdentifier())
                        .name(Constants.Components.FLOW_ID)
                        .setParallelism(flowid_pardeg)
                        .returns(Types.TUPLE(Types.LONG, Types.STRING, Types.LONG, Types.LONG));

            // two ways of implementing the third operator byteacc
            DataStream<Tuple4<Long, String, Long, Long>> byteacc;
            if (app_conf.getInt(Constants.AppConf.WIN_TYPE) == 0) {
                 byteacc =
                    flowid
                        .keyBy(value -> value.f0)
                        .window(SlidingProcessingTimeWindows.of(
                                Time.milliseconds(win_len), Time.milliseconds(win_slide)))
                        .reduce(new ByteSummingReducer())
                        .name(Constants.Components.BYTE_ACC)
                        .setParallelism(acc_pardeg)
                        .returns(Types.TUPLE(Types.LONG, Types.STRING, Types.LONG, Types.LONG));
            } else {
                byteacc =
                    flowid
                        .keyBy(value -> value.f0)
                        .window(SlidingProcessingTimeWindows.of(
                                Time.milliseconds(win_len), Time.milliseconds(win_slide)))
                        .apply(new ByteSummingWinFun())
                        .name(Constants.Components.BYTE_ACC)
                        .setParallelism(acc_pardeg)
                        .returns(Types.TUPLE(Types.LONG, Types.STRING, Types.LONG, Types.LONG));
            }

            DataStream<Tuple4<Long, String, Long, Long>> detector =
                    byteacc
                        .filter(new Detector(th))
                        .name(Constants.Components.DETECTOR)
                        .setParallelism(detector_pardeg)
                        .returns(Types.TUPLE(Types.LONG, Types.STRING, Types.LONG, Types.LONG));

            if (!chaining)
                ((SingleOutputStreamOperator<Tuple4<Long, String, Long, Long>>) detector).disableChaining();
            else
                ((SingleOutputStreamOperator<Tuple4<Long, String, Long, Long>>) detector).startNewChain();

            DataStreamSink<Tuple4<Long, String, Long, Long>> sink =
                    detector
                        .addSink(new Sink())
                        .name(Constants.Components.SINK)
                        .setParallelism(sink_pardeg);

            StringBuilder summary = new StringBuilder("[CONFIG] HH application configured as:\n");
            summary.append("* source rate: ").append(gen_rate).append("\n");
            summary.append("* chaining: ").append((chaining) ? "ON" : "OFF").append("\n");
            summary.append("* topology: source(").append(source_pardeg).append(") -> ");
            summary.append("flow_id(").append(flowid_pardeg).append(") -> ");
            summary.append("byte_len_acc(").append(acc_pardeg).append(") -> ");
            summary.append("detector(").append(detector_pardeg).append(") -> ");
            summary.append("sink(").append(sink_pardeg).append(")").append("\n");
            summary.append("* windows: length ").append(win_len).append(" ms, slide ").append(win_slide).append(" ms").append("\n");
            System.out.println(summary);

            // start topology
            long start_time = System.nanoTime();
            System.out.println("Starting...");
            env.execute(Constants.Components.TOPOLOGY);
            long stop_time = System.nanoTime();
            System.out.println("Exiting...");
            System.out.println("[MEASURE] graph execution time: " +
                    TimeUnit.SECONDS.convert(stop_time - start_time, TimeUnit.NANOSECONDS) + "s");
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

