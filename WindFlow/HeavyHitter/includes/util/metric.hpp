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
 *  @file    metric.hpp
 *  @author  Alessandra Fais
 *  @date    04/05/2022
 *
 *  @brief Utility file defining data structures and functions for managing performance metrics.
 */

#pragma once
#ifndef HH_METRIC_HPP
#define HH_METRIC_HPP

#include <iomanip>
#include <iostream>
#include <fstream>
#include <string>
#include <sstream>
#include <util/alglib/src/statistics.h>
#include <mutex>
#include <atomic>

namespace metrics {

    /**
     * @class Atomic_Double
     * @brief Class implementing atomic fetch and add operation on a double.
     *
     * The class is used to evaluate the average execution time (milliseconds) of source and sink operators.
     * All the replicas of the source node update an Atomic_Double value by adding the execution time of the
     * replica. At the end, an average of all these values is computed to extract the operator bandwidth.
     * All the replicas of the sink node do the same.
     */
    class Atomic_Double {
    private:
        double value;           // sum of the execution time values measured in each of the operator's replicas
        std::mutex m;           // mutex

    public:
        /**
         * @brief Default constructor.
         */
        Atomic_Double() : value(0) {}

        /**
         * @brief Implements atomic fetch and add operation on the internal state.
         *
         * @param _value new value to be added atomically
         * @return old state value
         */
        double fetch_add(double _value) {
            std::unique_lock<std::mutex> lock(m);
            double old_value = value;
            value = value + _value;

            return old_value;
        }

        double get() {
            std::unique_lock<std::mutex> lock(m);
            return value;
        }

        /**
         * @brief Destructor.
         */
        ~Atomic_Double() = default;
    };

    /**
     * @class Metrics_Collector
     * @brief Class maintaining a collection of the tuples sampled in a single sink replica for evaluating latency.
     *
     * An object of type Metrics_Collector is updated by each of the sink replicas.
     * Metrics are then evaluated and aggregated in the Results_Aggregator object.
     *
     * NOTE: to have accurate values for evaluating latency ypu need to compile with FF_BOUNDED_BUFFER MACRO set
     */
    class Metrics_Collector {
    private:
        std::vector<double> tuple_latencies;
        std::size_t sink_id;
        long tuples;
        long samples;

    public:
        /**
         * @brief Default constructor.
         */
        Metrics_Collector() : sink_id(0), tuples(0), samples(0) {}

        /**
         * @brief Gets ID of the sink replica.
         *
         * @return sink ID
         */
        std::size_t get_sink() {
            return sink_id;
        }

        /**
         * @brief Sets ID of the sink replica.
         *
         * @param _sink_id sink ID
         */
        void set_sink(const std::size_t& _sink_id) {
            sink_id = _sink_id;
        }

        /**
         * @brief Moves the vector of samples gathered from a sink replica to the internal collection.
         *
         * @param tuple_latencies the vector of latency values in milliseconds
         */
        void add(std::vector<double>&& _tuple_latencies) {
             tuple_latencies = std::move(_tuple_latencies);
#ifdef DEBUG_PRINT_METRIC
             std::cout << "[Collector_Sink" << sink_id << "] stored " << tuple_latencies.size() << " samples." << std::endl;
#endif
        }

        /**
         * @brief Updates the internal collection of latency samples for this sink replica.
         *
         * @param result_tuple a new result tuple from the detector operator
         */
        void update(const wf_tuple_t& _tuple) {
            /// sample 1M tuples for computing latency statistics
            if ((tuples % 16) == 0 && samples < 1000000) {
                unsigned long tuple_latency = wf::current_time_nsecs() - _tuple.ts;  // nanoseconds
                tuple_latencies.insert(tuple_latencies.end(), ((double) tuple_latency / 1000000L)); // milliseconds
                samples++;
            }
            tuples++;
        }

        /**
         * @brief Computes latency statistics
         *
         * @return mean latency value
         *
         * Evaluate latency statistics:
         * - mean value (average time needed by a tuple in order to traverse the whole pipeline),
         * - 5th, 25th, 50th, 75th, 95th percentiles,
         * - minimum value,
         * - maximum value.
         */
        double compute_latency_statistics() {
            if (tuple_latencies.empty()) {
                std::cout << "[Metrics_Collector] no latency statistics available." << std::endl;
                return 0;
            }

            /// vector containing all latency values
            alglib::real_1d_array tuple_lat_array;
            tuple_lat_array.setcontent(tuple_latencies.size(), &(tuple_latencies.at(0)));

            /// percentiles
            double perc_5 = 0.05;
            double perc_25 = 0.25;
            double perc_50 = 0.5;
            double perc_75 = 0.75;
            double perc_95 = 0.95;

            /// statistics
            double mean;
            double min;
            double perc_5_val;
            double perc_25_val;
            double perc_50_val;
            double perc_75_val;
            double perc_95_val;
            double max;

            /// statistics computation
            mean = alglib::samplemean(tuple_lat_array);
            alglib::samplepercentile(tuple_lat_array, perc_5, perc_5_val);
            alglib::samplepercentile(tuple_lat_array, perc_25, perc_25_val);
            alglib::samplepercentile(tuple_lat_array, perc_50, perc_50_val);
            alglib::samplepercentile(tuple_lat_array, perc_75, perc_75_val);
            alglib::samplepercentile(tuple_lat_array, perc_95, perc_95_val);
            min = *min_element(tuple_latencies.begin(), tuple_latencies.end());
            max = *max_element(tuple_latencies.begin(), tuple_latencies.end());

            /// write latency summary to output file
            std::stringstream out_file;
            out_file << "latency_sink" << sink_id << ".txt";
            std::ofstream out(out_file.str());
            out << "[Sink" << sink_id << "] latency (ms): "
                  << mean << " (mean) "
                  << min << " (min) "
                  << perc_5_val << " (5th) "
                  << perc_25_val << " (25th) "
                  << perc_50_val << " (50th) "
                  << perc_75_val << " (75th) "
                  << perc_95_val << " (95th) "
                  << max << " (max)." << std::endl;
            out.close();

            return mean;
        }

        /**
         * @brief Destructor.
         */
        ~Metrics_Collector() = default;
    };

    /**
     * @class Metrics_Aggregator
     * @brief Class aggregating the collected latency samples from all the sink replicas.
     *
     * There is a single Results_Aggregator object for the entire application. It collects all the Metrics_Collector
     * objects from the sink replicas and then evaluates the latency statistics for the entire topology.
     * Detailed reports are exported as files for each replica, while the average latency value is returned to the caller
     * of the dump() method.
     */
    class Metrics_Aggregator {
    private:
        std::size_t sink_replicas;                  // sink parallelism degree
        std::vector<Metrics_Collector> aggregator;  // aggregator of all the collected latency samples
        std::atomic_size_t sink_zero_processed;     // number of sink's replicas that processed zero tuples
        std::mutex m;                               // mutex
        double average_latency_sum;                 // sum of the average latency values measured in each of the sink's replicas

    public:
        /**
         * @brief Default constructor.
         */
        Metrics_Aggregator() : sink_replicas(0), sink_zero_processed(0), average_latency_sum(0) {}

        /**
         * @brief Set the number of sink replicas in the topology.
         *
         * @param _sink_replicas the number of sink replicas
         */
        void set_sink_replicas(const std::size_t& _sink_replicas) {
            sink_replicas = _sink_replicas;
        }

        /**
         * @brief Increments the number of sink replicas which processed zero tuples.
         */
        void add_empty_sink() {
            sink_zero_processed.fetch_add(1);
        }

        /**
         * @brief Adds the Metrics_Collector related to a sink replica.
         *
         * @param tuple_latency the latency value in milliseconds
         */
        void add_collector(Metrics_Collector&& mc) {
            std::unique_lock<std::mutex> lock(m);
            aggregator.emplace_back(std::move(mc));
        }

        /**
         * @brief Evaluates the number of sink replicas which processed more than zero tuples.
         *
         * @return the number of active sink replicas
         */
        std::size_t get_active_sinks() {
            return sink_replicas - sink_zero_processed;
        }

        /**
         * @brief Dumps all the computed latency statistics.
         *
         * @return the global average latency value
         */
        double dump() {
#ifdef DEBUG_PRINT_METRIC
            std::cout << "[Aggregator] dumping latency statistics for " << aggregator.size() << " sinks..." << std::endl;
#endif
            if (aggregator.empty()) {
                std::cout << "[Aggregator] no latency statistics available." << std::endl;
                return 0;
            }

            if (aggregator.size() == (sink_replicas - sink_zero_processed)) {
                for (auto& coll: aggregator) {
                    double avg_lat = coll.compute_latency_statistics();
                    average_latency_sum += avg_lat;
#ifdef DEBUG_PRINT_METRIC
                    std::cout << "[Collector_Sink" << collection.get_sink() << "] avg latency " << avg_lat << std::endl;
#endif
                }
            } else {
                std::cout << "[Aggregator] waiting for some sink replica to terminate." << std::endl;
            }

            return average_latency_sum / (double)(sink_replicas - sink_zero_processed);
        }

        /**
         * @brief Destructor.
         */
        ~Metrics_Aggregator() = default;
    };
}

#endif //HH_METRIC_HPP
