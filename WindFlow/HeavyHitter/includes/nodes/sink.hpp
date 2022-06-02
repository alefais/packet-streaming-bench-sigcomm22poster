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
 *  @file    sink.hpp
 *  @author  Alessandra Fais
 *  @date    04/05/2022
 *
 *  @brief Sink node terminating the application processing graph.
 */

#pragma once
#ifndef HH_SINK_HPP
#define HH_SINK_HPP

#include <algorithm>
#include <iostream>
#include <cstring>
#include "tuples/wf_tuple.hpp"
#include "util/metric.hpp"
#include "util/hh_stats.hpp"

extern std::atomic<long> received_tuples;           // total number of tuples received by all the sinks in the Data-Flow graph
extern metrics::Metrics_Aggregator latency_aggr;    // latency statistics aggregator
extern metrics::Atomic_Double sink_exec_time;       // sum of the execution times (milliseconds) of all the sink replicas
extern hh_stats::Results_Aggregator result_aggr;    // heavy hitter results aggregator
extern volatile unsigned long app_start_time;

/**
 * @class Sink_Functor
 * 
 * @brief Define the logic of the Sink receiving the final results.
 */
class Sink_Functor {
private:
    /// statistics, results, runtime info
    long processed_tuples;
    hh_stats::Results_Collector res_coll;
    std::vector<double> tuple_latencies;
    metrics::Metrics_Collector metrics_coll;
    std::size_t replica_id;

public:
    /**
     * @brief Constructor.
     *
     */
    Sink_Functor() :
            processed_tuples(0),
            replica_id(0) {}

    /**
     * @brief Prints results and evaluates latency statistics.
     *
     * @param t input wf tuple
     * @param rc RuntimeContext providing information on parallelism degree and replica id
     */
    void operator()(std::optional<wf_tuple_t>& t, wf::RuntimeContext& rc) {
        if (t.has_value()) {    // valid tuple
            if (processed_tuples == 0) {
                replica_id = rc.getReplicaIndex();
                metrics_coll.set_sink(replica_id);
                res_coll.set_sink(replica_id);
            }
#ifdef DEBUG_PRINT
            std::cout << "[Sink-" << replica_id << "] received packet " << processed_tuples << ", " << t->print_essential(1) << std::endl;
#endif
            /// update global tuple counter
            processed_tuples++;

            /// update latency samples
            metrics_coll.update(t.value());

            /// update heavy hitter statistics
            res_coll.update(t.value());

        } else {
            /// stream is terminated here (EOS)
#ifdef PRINT_OP_RESULTS
            std::cout << "[Sink-" << replica_id << " started termination... (processed tuples: "
                      << processed_tuples
                      << ", heavy hitters detected: "
                      << res_coll.get_collection_size() << ")" << std::endl;
#endif
            sink_exec_time.fetch_add((double)(wf::current_time_nsecs() - app_start_time) / 1000000L);  // sink replica execution time (milliseconds)

            /// manage metrics and results as last thing
            if (processed_tuples == 0) {
                latency_aggr.add_empty_sink();
                result_aggr.add_empty_sink();
            }
            else {
                received_tuples.fetch_add(processed_tuples);
                //metrics_coll.add(std::move(tuple_latencies));     // it is now done in an incremental way with metrics_coll.update()
                latency_aggr.add_collector(std::move(metrics_coll));
                result_aggr.add_res_collector(std::move(res_coll));
            }
        }
    }

    /**
     * @brief Destructor.
     */
    ~Sink_Functor() = default;
};

#endif //HH_SINK_HPP