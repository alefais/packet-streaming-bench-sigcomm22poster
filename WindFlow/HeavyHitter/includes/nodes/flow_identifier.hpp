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
 *  @file    flow_identifier.hpp
 *  @author  Alessandra Fais
 *  @date    04/05/2022
 * 
 *  @brief FlowId node which identifies different flows in the stream of packets.
 */

#pragma once
#ifndef HH_FLOWID_HPP
#define HH_FLOWID_HPP

#include <iostream>
#include <cstring>
#include <string>
#include <windflow.hpp>
#include "tuples/wf_tuple.hpp"
#include "util/flow.hpp"

/**
 * @class FlowId_Functor
 * 
 * @brief Define the logic of the operator which identifies different network traffic flows as all the incoming traffic is analyzed. 
 * 
 * A relaxed flow is defined by the tuple <IPv4 source address, IPv4 destination address>.
 */
class FlowId_Functor {
private:
    /// statistics & runtime info
    long processed_tuples;
    std::size_t replica_id;
    bool op_running;

public:
    /**
     * @brief Constructor.
     *
     */
    FlowId_Functor() :
            processed_tuples(0),
            op_running(true),
            replica_id(0) {}

    /**
     * @brief Identifies a flow and sets up the flow key field of each incoming tuple before forwarding it.
     *
     * @param t input wf tuple
     * @param rc RuntimeContext providing information on parallelism degree and replica id
     */
    void operator()(wf_tuple_t& t, wf::RuntimeContext& rc) {
        if (processed_tuples == 0) replica_id = rc.getReplicaIndex();
        
        /// identify flow and set up the corresponding field in the tuple
        const relaxed_flow::relaxed_flow_t f = std::make_tuple(t.ip_src, t.ip_dst);
        t.flow_key = relaxed_flow::key_hash()(f);
        t.total_len = 18 + ntohs(t.ip_len);

#ifdef DEBUG_PRINT
        std::cout << "[FlowId-" << replica_id << "] received packet " << processed_tuples
                  << " [" << t.print_essential(0) << "]" << std::endl;
#endif
        /// update global tuple counter
        processed_tuples++;
    }

    /**
     * @brief Destructor.
     */
    ~FlowId_Functor() {
        if (op_running && processed_tuples > 0) {
            op_running = false;
#ifdef PRINT_OP_SUMMARY
            std::cout << "[FlowId-" << replica_id << "] a total number of " << processed_tuples << " packets have been processed." << std::endl;
#endif
        }
    }
};

#endif //HH_FLOWID_HPP