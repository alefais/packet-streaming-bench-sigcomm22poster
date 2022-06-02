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
 *  @file    accumulator.hpp
 *  @author  Alessandra Fais
 *  @date    04/05/2022
 * 
 *  @brief Accumulator operator which counts the total amount of bytes transported by each flow each given interval of time.
 *
 *  The operator works on key-based (key: flow id) timing windows, over which a total byte sum is performed.
 */

#pragma once
#ifndef HH_WIN_ACC_HPP
#define HH_WIN_ACC_HPP

#include <iostream>
#include <cstring>
#include <unordered_map>
#include <netinet/in.h>
#include <windflow.hpp>
#include "tuples/wf_tuple.hpp"
#include "util/flow.hpp"

/**
 * @class WinAcc_Functor
 * 
 * @brief Define the logic of the operator which accumulates the number of bytes per flow per time window.
 * 
 */
class WinAcc_Functor {
private:
    /// statistics & runtime info
    std::size_t processed_tuples;
    std::size_t replica_id;
    bool op_running;

public:
    /**
     * @brief Constructor.
     *
     */
    WinAcc_Functor() :
        processed_tuples(0),
        op_running(true),
        replica_id(0) {}

    /**
     * @brief Computes the sum of transported bytes over a window of packets belonging to the same flow.
     *
     * This implementation uses the non incremental version of the operator.
     *
     * @param win window of wf tuples
     * @param t result wf tuple
     * @param rc RuntimeContext providing information on parallelism degree and replica id
     */
    void operator()(const wf::Iterable<wf_tuple_t>& win, wf_tuple_t& t, wf::RuntimeContext& rc) {
        if (processed_tuples == 0) replica_id = rc.getReplicaIndex();

        if (win.size() > 0) {
            t.ts = win[win.size() - 1].ts;
            t.acc_len = 0;
            t.flow_key = win[win.size() - 1].flow_key;
            t.ip_src = win[win.size() - 1].ip_src;
            t.ip_dst = win[win.size() - 1].ip_dst;
            for (const auto& i : win) {
                t.acc_len += i.total_len;
            }

            /// update packet counter
            processed_tuples += win.size();
#ifdef DEBUG_PRINT
            std::cout << "[WinAcc-" << replica_id << "] processed win[" << win.size() << "], "
                      << "sent result (flow: " << t.flow_key << ", bytes/win: " << t.acc_len << ")" << std::endl;
#endif
        }
    }

    /**
     * @brief Destructor.
     */
    ~WinAcc_Functor() {
        if (op_running && processed_tuples > 0) {
            op_running = false;
#ifdef PRINT_OP_SUMMARY
            std::cout << "[WinAcc-" << replica_id << "] a total number of " << processed_tuples << " packets have been processed." << std::endl;
#endif
        }
    }
};

#endif //HH_WIN_ACC_HPP