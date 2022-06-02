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
 *  @file    detector.hpp
 *  @author  Alessandra Fais
 *  @date    04/05/2022
 * 
 *  @brief Detector node which identifies the heavy hitter (elephant) flows.
 *
 *  The operator sends the information on these flows to the sink while filters away the others.
 */

#pragma once
#ifndef HH_DETECTOR_HPP
#define HH_DETECTOR_HPP

#include <iostream>
#include <cstring>
#include <string>
#include <windflow.hpp>
#include "tuples/wf_tuple.hpp"
#include "util/flow.hpp"

extern long threshold;

/**
 * @class Detector_Functor
 * 
 * @brief Define the logic of the operator which identifies heavy hitter flows as all the incoming TCP traffic is analyzed.
 */
class Detector_Functor {
private:
    /// statistics & runtime info
    long processed_tuples;
    long heavy_hitters;
    std::size_t replica_id;
    bool op_running;

public:
    /**
     * @brief Constructor.
     *
     */
    Detector_Functor() :
            processed_tuples(0),
            heavy_hitters(0),
            op_running(true),
            replica_id(0) {}

    /**
     * @brief Identifies heavy hitter flows and filters away the rest of the traffic.
     *
     * @param t input wf tuple
     * @param rc RuntimeContext providing information on parallelism degree and replica id
     * @return true if a SYN packet has been processed, false otherwise
     */
    bool operator()(wf_tuple_t& t, wf::RuntimeContext& rc) {
        if (processed_tuples == 0) replica_id = rc.getReplicaIndex();

        if (!t.ts) return false;    // invalid tuple (empty window in accumulator)

        /// filter normal sized flows
        if (t.acc_len <= threshold)
            return false;

#ifdef DEBUG_PRINT
        std::cout << "[Detector-" << replica_id << "] received packet " << processed_tuples
                  << " [hh #" << (heavy_hitters + 1) << ": " << t.print_essential(1) << "]" << std::endl;
#endif
        /// update global tuple counter
        processed_tuples++;

        /// detected heavy hitter
        heavy_hitters++;
        return true;
    }

    /**
     * @brief Destructor.
     */
    ~Detector_Functor() {
        if (op_running && processed_tuples > 0) {
            op_running = false;
#ifdef PRINT_OP_SUMMARY
            std::cout << "[Detector-" << replica_id << "] a total number of "
                      << heavy_hitters << " heavy hitters have been detected out of "
                      << processed_tuples << " processed packets."
                      << std::endl;
#endif
        }
    }
};

#endif //HH_DETECTOR_HPP