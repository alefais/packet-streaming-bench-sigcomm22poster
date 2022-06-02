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
 *  @file    source.hpp
 *  @author  Alessandra Fais
 *  @date    04/05/2022
 *
 *  @brief Source node generating the input stream from live packet capturing.
 * 
 *  Source node which reads packets from a pre-filled dataset in memory and
 *  generates the tuple stream to feed the application processing graph.
 */

#pragma once
#ifndef HH_STANDARD_SOURCE_HPP
#define HH_STANDARD_SOURCE_HPP

#include <iostream>
#include <cstring>
#include <string>
#include <windflow.hpp>
#include "tuples/wf_tuple.hpp"
#include "util/metric.hpp"

extern std::atomic<long> sent_tuples;                   // total number of tuples emitted by all the sources in the Data-Flow graph
extern metrics::Atomic_Double source_exec_time;         // sum of the execution times (milliseconds) of all the source replicas
extern volatile unsigned long app_start_time;
extern volatile unsigned long app_run_time;

/**
 * @class Source_Functor
 * 
 * @brief Define the logic of the Source.
 */
class Source_Functor {
private:
    /// operator state & statistics
    std::vector<wf_tuple_t> dataset;    // contains all the packet tuples
    std::size_t next_tuple_idx;         // index of the next tuple to be sent
    int generations;                    // counts the times the input pcap file is generated
    long generated_tuples;              // total number of generated tuples
    int rate;                           // stream generation rate
    
    /// runtime info
    std::size_t replica_id;

    /// time variables
    unsigned long current_time;

    /**
     *  @brief Add some active delay (busy-waiting function)
     *
     *  @param waste_time wait time in nanoseconds
     */
    static void active_delay(unsigned long waste_time) {
        auto start_time = wf::current_time_nsecs();
        bool end = false;
        while (!end) {
            auto end_time = wf::current_time_nsecs();
            end = (end_time - start_time) >= waste_time;
        }
    }

public:
    /// the termination condition can be a received SIGINT/SIGTERM or the expiration of the time frame defined by app_run_time (set in fc.cpp)
    inline static volatile bool terminate;

    /**
     * @brief Constructor.
     *
     * @param _dataset all the tuples that will compose the stream
     * @param _rate stream generation rate
     */
    Source_Functor(std::vector<wf_tuple_t>& _dataset, const int _rate) :
            dataset(std::move(_dataset)),
            current_time(app_start_time),
            next_tuple_idx(0),
            generations(0),
            generated_tuples(0),
            rate(_rate),
            replica_id(0) {}

    /**
     * @brief Sends packet tuples in a item-by-item fashion.
     *
     * @param shipper Source_Shipper object used for generating input tuples
     * @param rc RuntimeContext providing information on parallelism degree and replica id
     */
    void operator()(wf::Source_Shipper<wf_tuple_t>& shipper, wf::RuntimeContext& rc) {
        current_time = wf::current_time_nsecs(); // get the current time

        if (generated_tuples == 0) replica_id = rc.getReplicaIndex();

        /// generation loop
    	while ((current_time - app_start_time <= app_run_time) && !terminate) {

            /// count the number of generations
            if (next_tuple_idx == 0) generations++;

            /// generate new tuple
            wf_tuple_t t(dataset.at(next_tuple_idx));
            t.ts = wf::current_time_nsecs();
            shipper.push(t);     // send the tuple

            /// index of the next tuple to generate
            next_tuple_idx = (next_tuple_idx + 1) % dataset.size();

#ifdef DEBUG_PRINT
            std::cout << "[Source-" << replica_id << "] sent packet " << generated_tuples
                      << ", " << t.print() << std::endl;
#endif
            /// update global tuple counter
            generated_tuples++;
            
            if (rate != 0) { // active waiting to respect the generation rate
	            long delay_nsec = (long) ((1.0 / rate) * 1e9);
	            active_delay(delay_nsec);
	        }
            current_time = wf::current_time_nsecs(); // get the new current time

            /// EOS is reached here, start source termination
            if ((current_time - app_start_time > app_run_time) || terminate) {
                /// update throughput statistics
                source_exec_time.fetch_add((double)(wf::current_time_nsecs() - app_start_time) / 1000000L);
                sent_tuples.fetch_add(generated_tuples);

#ifdef PRINT_OP_RESULT
                std::cout << "[Source-" << replica_id << " started termination..."
                          << " (generated tuples: " << generated_tuples << ")" << std::endl;
#endif
            }
        }
    }

    /**
     * @brief Destructor.
     */
    ~Source_Functor() = default;
};

#endif //HH_STANDARD_SOURCE_HPP