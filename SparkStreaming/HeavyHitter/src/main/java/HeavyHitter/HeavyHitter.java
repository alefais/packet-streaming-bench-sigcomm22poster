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
 * @version 26/05/2022
 *
 * Main class of the Heavy Hitter application.
 */
package HeavyHitter;

import Util.AppConfig;
import Util.Constants;
import Parser.PcapData;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.*;
import org.apache.spark.streaming.api.java.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple4;

import java.io.*;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;


public class HeavyHitter {

    private static final Logger LOG = LoggerFactory.getLogger((HeavyHitter.class));
    private static final Pattern SPACE = Pattern.compile(" ");
    private static final Pattern COMMA = Pattern.compile(",");

    /**
     * Main class implementing the Heavy Hitter Spark Streaming application.
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

            // create a local JavaSparkContext (master is set up to a special "local[*]" string to run in local mode,
            // meaning that Spark will run locally with as many worker threads as logical cores available on the machine.
            SparkConf spark_conf = new SparkConf().setAppName("HeavyHitter").setMaster("local[*]");
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
            int batch_size = (args.length > 8) ? Integer.parseInt(args[8]) : app_conf.getInt(Constants.AppConf.BATCH_LEN);
            int gen_rate = (args.length > 9) ? Integer.parseInt(args[9]) : app_conf.getInt(Constants.AppConf.GEN_RATE); // source generation rate (for tests)
            int th = (args.length > 10) ? Integer.parseInt(args[10]) : app_conf.getInt(Constants.AppConf.THRESHOLD);
            int app_runtime = app_conf.getInt(Constants.AppConf.APP_RUNTIME);

            // spark conf properties
            spark_conf.set("spark.eventLog.enabled", "false");
            //spark_conf.set("spark.executor.extraJavaOptions", "-XX:+PrintGCDetails -XX:+PrintGCTimeStamps");
            spark_conf.set("spark.executor.cores", String.valueOf(app_conf.getInt(Constants.ExecutionConf.ALL_THREADS)));
            spark_conf.set("spark.default.parallelism", String.valueOf(app_conf.getInt(Constants.ExecutionConf.ALL_THREADS)));
            spark_conf.set("spark.executor.memory", "8g");

            // create the JavaStreamingContext object from the existing JavaSparkContext (batch interval set to batch_size)
            try (JavaStreamingContext context = new JavaStreamingContext(spark_conf, Durations.milliseconds(batch_size))) {
                context.checkpoint("./checkpoint");

                // prepare the application topology

                // custom receiver (customized source node)
                JavaDStream<String[]> source = context.receiverStream(
                        new Source(StorageLevel.MEMORY_AND_DISK_SER_2(), app_runtime, input_pcap_file, source_pardeg, gen_rate));

                // define the streaming computation by applying transformation and output operations to DStreams

                // flow identifier node producing a stream of key-value (flow, pkt) pairs
                JavaPairDStream<Integer, Tuple4<Integer, String, Long, Long>> flowid =
                        source.mapToPair(new FlowIdentifierNode());

                // byte summing node computing the byte sum for each flowid-based window of packets
                JavaPairDStream<Integer, Tuple4<Integer, String, Long, Long>> byteacc =
                        flowid.reduceByKeyAndWindow(
                                new ByteSumAccumulatorNode(),
                                Durations.milliseconds(win_len), Durations.milliseconds(win_slide));//, acc_pardeg);

                // detector node filtering flows with small byte sum value (keep only elephant flows)
                JavaPairDStream<Integer, Tuple4<Integer, String, Long, Long>> detector =
                        byteacc.filter(new Detector(th));

                // sink node (print all results)
                final long[] host_num = {0};
                detector.foreachRDD((pkt) -> {
                        pkt.repartition(1).saveAsTextFile("./results/heavy_hitters" + host_num[0]++);
                        //pkt.collect().forEach(p -> System.out.println("[Sink] Heavy Hitters report RDD #" + host_num[0]++ + " : " + p._2._2()));
                    });

                // start and stop topology
                long start_time = System.nanoTime();
                context.start();              // start the computation (start receiving data and processing it)
                context.awaitTermination();   // wait for the computation to terminate (processing can be stopped manually or due to any error)
                long stop_time = System.nanoTime();
                System.out.println("Exiting...");
                System.out.println("[MEASURE] graph execution time: " +
                        TimeUnit.SECONDS.convert(stop_time - start_time, TimeUnit.NANOSECONDS) + "s");
            }
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