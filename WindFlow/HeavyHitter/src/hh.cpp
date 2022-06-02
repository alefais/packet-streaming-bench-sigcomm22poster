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
 *  @file   hh.cpp
 *  @author Alessandra Fais
 *  @date   25/05/2022
 *
 *  @brief Main class of the Heavy Hitter application.
 */

#include <iomanip>
#include <iostream>
#include <string>
#include <regex>
#include <cstddef>
#include <csignal>
#include <vector>
#include <windflow.hpp>
#include "nodes/source.hpp"
#include "nodes/flow_identifier.hpp"
#include "nodes/accumulator.hpp"
#include "nodes/detector.hpp"
#include "nodes/sink.hpp"
#include "parser/pcap_parser.hpp"
#include "util/metric.hpp"
#include "util/hh_stats.hpp"
#include "util/util.hpp"

/// global variables (for input PCAP file parsing)
std::vector<wf_tuple_t> dataset;            // dataset of all the tuples in memory

/// global variables (for performance metrics evaluation)
std::atomic<long> sent_tuples;              // total number of tuples sent by all the sources
std::atomic<long> received_tuples;          // total number of tuples received by all the sinks
metrics::Metrics_Aggregator latency_aggr;   // aggregates the latency samples collected in each of the sink's replicas
metrics::Atomic_Double source_exec_time;    // sum of the execution times (milliseconds) of all the source replicas
metrics::Atomic_Double sink_exec_time;      // sum of the execution times (milliseconds) of all the sink replicas
volatile unsigned long app_start_time;
volatile unsigned long app_run_time;

/// global variables (application logic)
long threshold;                             // threshold used for the heavy hitter detection
hh_stats::Results_Aggregator result_aggr;   // heavy hitter results aggregator

/// manage SIGINT and SIGTERM signals: terminate the topology
void exit_app(int exit_signal) {
    Source_Functor::terminate = true;
    std::cout << "Signal handler: started app termination..." << std::endl;
}

/// main
int main(int argc, char* argv[]) {
    /// parse options from command line
    int option = 0;
    int index = 0;
    std::string input_pcap_file = "./dump.pcap";
    std::size_t source_pardeg = 0;
    std::size_t flowid_pardeg = 0;
    std::size_t winacc_pardeg = 0;
    std::size_t detector_pardeg = 0;
    std::size_t sink_pardeg = 0;
    bool chaining = false;
    std::size_t batch_size = 0;
    std::size_t win_length = 0;
    std::size_t win_slide = 0;
    int rate = 0;
    threshold = 0;

    /// parse command line options
    opterr = 1; // turn on/off getopt error messages
    if (argc >= 7) {
        while ((option = getopt_long(argc, argv, "i:p:b:w:s:r:t:c", cli::long_opts, &index)) != -1) {
            switch (option) {
                case 'i':       // pcap file to open (optional argument, default is ../dump.pcap)
                    input_pcap_file = optarg;
                    break;
                case 'p': {     // operators parallelism (required)
                    std::vector<size_t> pardegs;
                    std::string pars(optarg);
                    std::stringstream ss(pars);
                    for (size_t i; ss >> i;) {
                        pardegs.push_back(i);
                        if (ss.peek() == ',')
                            ss.ignore();
                    }
                    if (pardegs.size() != 5) {
                        std::cout << cli::parsing_error << std::endl;
                        exit(EXIT_FAILURE);
                    } else {
                        source_pardeg = pardegs[0];
                        flowid_pardeg = pardegs[1];
                        winacc_pardeg = pardegs[2];
                        detector_pardeg = pardegs[3];
                        sink_pardeg = pardegs[4];
                    }
                    break;
                }
                case 'w':       // set up window size
                    win_length = atoi(optarg);
                    break;
                case 's':       // set up window sliding factor
                    win_slide = atoi(optarg);
                    break;
                case 'b':       // set up batching (optional argument, default disabled)
                    batch_size = atoi(optarg);
                    break;
                case 'c':       // enable chaining (optional argument, default disabled)
                    chaining = true;
                    break;
                case 'r':       // set up generation rate (optional argument, default full speed tuples/second)
                    rate = atoi(optarg);
                    break;
                case 't':
                    threshold = atol(optarg);
                    break;
                default:
                    std::cout << cli::parsing_error << std::endl;
                    exit(EXIT_FAILURE);
            }
        }
    } else if (argc == 2) {
        while ((option = getopt_long(argc, argv, "h", cli::long_opts, &index)) != -1) {
            if (option == 'h') {
                std::cout << cli::help << std::endl;
                exit(EXIT_SUCCESS);
            }
        }
    } else {
        std::cout << cli::parsing_error << std::endl;
        exit(EXIT_FAILURE);
    }

    /// performance metrics and results management
    sent_tuples = 0;
    received_tuples = 0;
    latency_aggr.set_sink_replicas(sink_pardeg);
    result_aggr.set_sink_replicas(sink_pardeg);

    /// data pre-processing
    PcapTransformer pcap_tran(input_pcap_file);
    //pcap_tran.toHumanReadableCsv(std::regex_replace(input_pcap_file, std::regex("pcap"), "csv")); // generate a human-readable csv file from the original pcap file
    dataset = pcap_tran.toTupleDataset(-1);         // generate the entire tuple dataset from the original pcap file
    
    /// register termination signals SIGINT and SIGTERM
    signal(SIGINT, exit_app);
    signal(SIGTERM, exit_app);

    /// application starting time and run time
    app_start_time = wf::current_time_nsecs();    // nanoseconds
    app_run_time = 60 * 1000000000L;              // 60 seconds

    Source_Functor::terminate = false;

    /// create nodes and topology
    wf::PipeGraph topology("HeavyHitter", wf::Execution_Mode_t::DEFAULT, wf::Time_Policy_t::INGRESS_TIME);

    Source_Functor source_fun(dataset, rate);           // source operator
    wf::Source source = wf::Source_Builder(source_fun)
            .withParallelism(source_pardeg)
            .withName("Source")
            .withOutputBatchSize(batch_size)
            .build();
    wf::MultiPipe &mp = topology.add_source(source);

    FlowId_Functor flowid_fun;                             // flow identifier operator
    wf::Map flowid = wf::Map_Builder(flowid_fun)
            .withParallelism(flowid_pardeg)
            .withName("FlowIdentifier")
            .withOutputBatchSize(batch_size)
            .build();

    WinAcc_Functor winacc_fun;                             // per-flow byte length accumulator
    wf::Keyed_Windows win_acc = wf::Keyed_Windows_Builder(winacc_fun)
            .withParallelism(winacc_pardeg)
            .withName("ByteLenAccumulator")
            .withKeyBy([](const wf_tuple_t& t) -> unsigned long { return t.flow_key; })     // stream is logically partitioned on keys (flow id)
            .withTBWindows(std::chrono::microseconds(win_length * 1000), std::chrono::microseconds(win_slide * 1000))
            .withOutputBatchSize(batch_size)
            .build();
#ifndef TWO_OPS
    mp.add(flowid);
    mp.add(win_acc);
#endif

    if (!chaining) {        // chaining disabled
        Detector_Functor detector_fun;                     // heavy hitter detector operator (batching enabled)
        wf::Filter detector = wf::Filter_Builder(detector_fun)
                .withParallelism(detector_pardeg)
                .withName("HeavyHitterDetector")
                .withOutputBatchSize(batch_size)
                .build();
#ifndef TWO_OPS
        mp.add(detector);
#endif
        Sink_Functor sink_fun;                             // sink operator (added to the multipipe)
        wf::Sink sink = wf::Sink_Builder(sink_fun)
                .withParallelism(sink_pardeg)
                .withName("Sink")
                .build();
        mp.add_sink(sink);
    } else {        // chaining enabled
        Detector_Functor detector_fun;                     // heavy hitter detector operator (batching disabled)
        wf::Filter detector = wf::Filter_Builder(detector_fun)
                .withParallelism(detector_pardeg)
                .withName("HeavyHitterDetector")
                .build();
#ifndef TWO_OPS
        mp.add(detector);
#endif
        Sink_Functor sink_fun;                             // sink operator (chained to the detector in the multipipe)
        wf::Sink sink = wf::Sink_Builder(sink_fun)
                .withParallelism(sink_pardeg)
                .withName("Sink")
                .build();
        mp.chain_sink(sink);
    }

    /// execution summary
    std::stringstream summary;
    summary << "Executing HH application configured as:\n"
            << "* source rate: " << ((rate == 0) ? "max speed" : std::to_string(rate)) << " (tuples/s)\n"
            << "* batch size: " << batch_size << "\n"
            << "* chaining: " << ((chaining) ? "ON" : "OFF") << "\n"
            << "* topology: source(" << source_pardeg << ") -> ";
#ifndef TWO_OPS
    summary << "flow_id(" << flowid_pardeg << ") -> "
            << "byte_len_acc(" << winacc_pardeg << ") -> "
            << "detector(" << detector_pardeg << ") -> ";
#endif
    summary << "sink(" << sink_pardeg << ")\n";
#ifndef TWO_OPS
    summary << "* windows: length " << win_length << " ms, slide " << win_slide << " ms\n";
#endif
    std::cout << summary.str() << std::endl;;

    /// evaluate topology execution time
    volatile unsigned long start_time_main_usecs = wf::current_time_usecs();
    topology.run();
    volatile unsigned long end_time_main_usecs = wf::current_time_usecs();
    double elapsed_time_seconds = (double)(end_time_main_usecs - start_time_main_usecs) / (1000000.0);
    std::cout << "Exiting..." << std::endl;

    /// evaluate throughput
    double throughput = (double)sent_tuples / elapsed_time_seconds;
    double source_bw = (double)sent_tuples / ((source_exec_time.get() / 1000.0) / (double)source_pardeg);
    double sink_bw = (double)received_tuples / ((sink_exec_time.get() / 1000.0) / (double)latency_aggr.get_active_sinks());
    std::cout << "[MEASURE] throughput: " << (int) throughput << " tuples/second" << std::endl;
    std::cout << "[MEASURE] throughput at source node: " << (int) source_bw << " tuples/second" << std::endl;
    std::cout << "[MEASURE] throughput at sink node: " << (int) sink_bw << " tuples/second" << std::endl;

    /// print heavy hitter reports
    result_aggr.dump_per_sink();
    std::size_t hh_hosts = result_aggr.dump_aggregated();

    /// evaluate latency (average time required by a tuple to traverse the whole system)
    //start_time_main_usecs = current_time_usecs();
    double latency = latency_aggr.dump();
    //end_time_main_usecs = current_time_usecs();
    //std::cout << "[MEASURE] latency evaluation required " << (double)(end_time_main_usecs - start_time_main_usecs) / (1000000.0) << " seconds." << std::endl;
    std::cout << "[MEASURE] average latency: " << std::fixed << std::setprecision(5) << latency << " ms" <<  std::endl;
    std::cout << "[RESULTS] heavy hitter hosts (no duplicates): " << hh_hosts << std::endl;

    return 0;
}