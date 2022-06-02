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
 *  @file    hh_stats.hpp
 *  @author  Alessandra Fais
 *  @date    04/05/2022
 *
 *  @brief Utility file defining data structures and functions for collecting and aggregating final heavy hitters statistics.
 */

#pragma once
#ifndef HH_STATS_HPP
#define HH_STATS_HPP

#include <iomanip>
#include <iostream>
#include <fstream>
#include <string>
#include <sstream>
#include <unordered_map>
#include <set>
#include <mutex>
#include <atomic>
#include "tuples/wf_tuple.hpp"

namespace hh_stats {

    using hh_map = std::unordered_map<std::size_t, std::tuple<std::string, std::string, std::size_t>>;

    /**
     * @class Result_Collector
     * @brief Class maintaining a collection of the results collected by a single sink replica.
     *
     * An object of type Result_Collector is updated by each of the sink replicas.
     * Final heavy hitters results are then produced in the Result_Aggregator object.
     */
    class Results_Collector {
    private:
        /**
         * Structure of a single entry in the heavy_hitters map:
         * - key: flow id key
         * - value: IPv4 source address (string repr.), IPv4 destination address (string repr.), total bytes carried
         *  (this is the max byte sum experienced in a given window of time)
         */
        hh_map heavy_hitters;
        std::size_t sink_id;

    public:
        /**
         * @brief Default constructor.
         */
        Results_Collector() : sink_id(0) {}

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
         * @brief Updates the internal collection of results for this sink replica.
         *
         * @param result_tuple a new result tuple from the detector operator
         */
        void update(const wf_tuple_t& result_tuple) {
            if (heavy_hitters.find(result_tuple.flow_key) == heavy_hitters.end()) {     // create new entry (new flow!)
                heavy_hitters.insert(
                        std::make_pair(result_tuple.flow_key,
                                       std::make_tuple(
                                               result_tuple.local_addr_to_string(0),    // IPv4 source address
                                               result_tuple.local_addr_to_string(1),    // IPv4 destination address
                                               result_tuple.acc_len)
                        ));
            } else {   // existing entry found (known flow)
                /// update byte sum if the new one computed is higher than the one saved
                if (std::get<2>(heavy_hitters.at(result_tuple.flow_key)) < result_tuple.acc_len) {
                    std::get<2>(heavy_hitters.at(result_tuple.flow_key)) = result_tuple.acc_len;
                }
            }
        }

        /**
         * @brief Gets the result collection size.
         *
         * @return result collection size
         */
        std::size_t get_collection_size() {
            return heavy_hitters.size();
        }

        /**
         * @brief Gets the result collection.
         *
         * @return result collection
         */
         hh_map get_collection() {
            return heavy_hitters;
        }

        /**
         * @brief Dumps heavy hitters statistics collected by the current sink.
         *
         * @return result collection size
         */
        std::size_t dump_sink_results() {
            if (heavy_hitters.empty()) {
                std::cout << "[Results_Collector] no heavy hitters found." << std::endl;
                return 0;
            }

            // write per-sink heavy hitter summary to output file
            std::stringstream out_file;
            out_file << "report_sink" << sink_id << ".txt";
            std::ofstream out(out_file.str());
            out << "[Sink" << sink_id << "-REPORT]" << std::endl;
            for (const auto& res_item : heavy_hitters) {
                out << std::get<1>(res_item.second)   // ipv4 dst address
                    << " from " << std::get<0>(res_item.second)   // ipv4 src address
                    << " : max peak " << std::get<2>(res_item.second)  // total bytes
                    << " exchanged bytes" << std::endl;
            }
            out.close();

            return get_collection_size();
        }

        /**
         * @brief Destructor.
         */
        ~Results_Collector() = default;
    };

    /**
     * @class Results_Aggregator
     * @brief Class aggregating the collected heavy hitter results from all the sink replicas.
     *
     * There is a single Results_Aggregator object for the entire application. It collects all the Results_Collector
     * objects from the sink replicas and then evaluates the results for the entire application.
     * Detailed reports can be exported as files for each replica or as a single aggregated report document.
     */
    class Results_Aggregator {
    private:
        std::size_t sink_replicas;                  // sink parallelism degree
        std::vector<Results_Collector> aggregator;  // aggregator of all the collected heavy hitter results
        std::atomic_size_t sink_zero_processed;     // number of sink's replicas that processed zero tuples
        std::mutex m;                               // mutex
        hh_map aggregated_hh_results;               // map containing all the heavy hitter results
        std::set<std::string> hh_hosts;             // set containing the targeted hosts (no duplicates)

    public:
        /**
         * @brief Default constructor.
         */
        Results_Aggregator() : sink_replicas(0), sink_zero_processed(0) {}

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
         * @brief Adds the Results_Collector related to a sink replica.
         *
         * @param rc the results collector of a sink
         */
        void add_res_collector(Results_Collector&& rc) {
            std::unique_lock<std::mutex> lock(m);
            aggregator.emplace_back(std::move(rc));
        }

        /**
         * @brief Evaluates the number of sink replicas which found some heavy hitters.
         *
         * @return the number of sink replicas with heavy hitter results
         */
        std::size_t get_hh_sinks() {
            return sink_replicas - sink_zero_processed;
        }

        /**
         * @brief Dumps the heavy hitter statistics for each sink.
         *
         * @return the number of hh sinks
         */
        std::size_t dump_per_sink() {
#ifdef DEBUG_PRINT_METRIC
            std::cout << "[Aggregator] dumping heavy hitter results from " << get_hh_sinks() << " sinks..." << std::endl;
#endif
            if (aggregator.empty()) {
                std::cout << "[Aggregator] no heavy hitter results available." << std::endl;
                return 0;
            }

            if (aggregator.size() == (sink_replicas - sink_zero_processed)) {
                for (auto& coll: aggregator) {
                    coll.dump_sink_results();
                }
            } else {
                std::cout << "[Aggregator] waiting for some sink replica to terminate." << std::endl;
            }

            return get_hh_sinks();
        }

        /**
         * @brief Dumps all the heavy hitter results.
         *
         * @return the global number of heavy hitters (no duplicates)
         */
        std::size_t dump_aggregated() {
#ifdef DEBUG_PRINT_METRIC
            std::cout << "[Aggregator] dumping heavy hitter aggregated results for " << get_hh_sinks() << " sinks..." << std::endl;
#endif
            if (aggregator.empty()) {
                std::cout << "[Aggregator] no heavy hitter results available." << std::endl;
                return 0;
            }

            // there will be no duplicated flows after the merge in the aggregated_hh_results map
            if (aggregator.size() == (sink_replicas - sink_zero_processed)) {
                for (auto& coll: aggregator) {
                    aggregated_hh_results.merge(coll.get_collection());     // heavy hitter items are moved into the aggregated map
                }
            } else {
                std::cout << "[Aggregator] waiting for some sink replica to terminate." << std::endl;
            }

            // however, since we simply print a list of destination hosts, there can be duplicated hosts if, for example,
            // the same destination address is targeted by more heavy hitter flows (same destination but several sources starting different flows)
            for (const auto& res_item : aggregated_hh_results) {
                hh_hosts.insert(std::get<1>(res_item.second));          // duplicated hosts are removed in the set
            }

            // write global heavy hitter summary to output file (with no duplicates)
            std::ofstream out("heavy_hitters.txt");
            out << "[Heavy Hitters - GLOBAL REPORT]\nList of destination hosts targeted:\n";
            for (const auto& host : hh_hosts) {
                out << host << std::endl;
            }
            out.close();

            return hh_hosts.size();
        }

        /**
         * @brief Destructor.
         */
        ~Results_Aggregator() = default;
    };
}

#endif //HH_STATS_HPP
