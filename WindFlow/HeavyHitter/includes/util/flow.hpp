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
 *  @file    flow.hpp
 *  @author  Alessandra Fais
 *  @date    03/05/2022
 *
 *  @brief Utility file defining data structures and functions for handling network traffic flows.
 * 
 *  A flow is identified by its 5-tuple, which contains complete source and destination addresses, 
 *  source and destination ports, and the transport layer protocol identifier.
 *  A relaxed flow is only identified by the couple of values source and destination IP addresses.
 */

#pragma once
#ifndef HH_FLOW_HPP
#define HH_FLOW_HPP

#include <tuple>

namespace flow {

    /**
     * @brief Flow definition.
     * Flow fields are: source IP address, destination IP address, source port, destination port and protocol.
     */
    typedef std::tuple<uint32_t, uint32_t, uint16_t, uint16_t, uint8_t> flow_t;

    /**
     * @brief Hashing function defined for a flow tuple.
     */
    struct key_hash : public std::unary_function<flow_t, size_t> {
        size_t operator()(const flow_t& k) const {
            return std::get<0>(k) ^ std::get<1>(k) ^ std::get<2>(k) ^ std::get<3>(k) ^ std::get<4>(k);
        }
    };

}

namespace relaxed_flow {

    /**
     * @brief RelaxedFlow definition.
     * RelaxedFlow fields are: source IP address, destination IP address.
     */
    typedef std::tuple<uint32_t, uint32_t> relaxed_flow_t;

    /**
     * @brief Hashing function defined for a relaxed_flow tuple.
     */
    struct key_hash : public std::unary_function<relaxed_flow_t, size_t> {
        size_t operator()(const relaxed_flow_t& k) const {
            return std::get<0>(k) ^ std::get<1>(k);
        }
    };

}

#endif //HH_FLOW_HPP
