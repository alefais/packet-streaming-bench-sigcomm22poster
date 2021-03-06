/*
 * Copyright (C) 2022 Universit√† di Pisa
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
 *  @file    wf_tuple.hpp
 *  @author  Alessandra Fais
 *  @date    04/05/2022
 *  
 *  @brief Structure of a tuple.
 *  
 *  This file defines the structure of standard windflow tuples generated by the source.
 */

#pragma once
#ifndef HH_WF_TUPLE_HPP
#define HH_WF_TUPLE_HPP

#include <string>
#include <cstring>
#include <sstream>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <windflow.hpp>

struct wf_tuple_t
{
    /* general fields */
    uint32_t ip_src, ip_dst;       // identifies the source/destination IP address (binary representation)
    uint16_t port_src, port_dst;   // identifies the source/destination port (network representation)
    uint8_t protocol;              // contains the protocol field (17 UDP, 6 TCP)

    /* IP */
    uint16_t ip_hdrlen;            // length of the IP header in bytes
    uint16_t ip_len;               // length of the entire IP packet in bytes (header + data) (network representation)

    /* TCP */
    uint16_t tcp_hdrlen;           // length of the TCP header in bytes
    uint32_t seq;                  // sequence number (network representation)
    uint32_t ack;                  // ack number (network representation)
    uint16_t win;                  // window size
    uint16_t syn;                  // syn flag

    /* metadata fields (application specific) */
    uint64_t ts;                   // timestamp (tuple generation time set in the source)
    uint64_t flow_key;             // flow identifier (computed and set in the FlowId operator)
    uint16_t total_len;            // total length in bytes of the IPv4 packet
    uint64_t acc_len;              // total length in bytes of the IPv4 packets belonging to this flow in the current time window

    /**
     * @brief Constructor I.
     */
    wf_tuple_t() : ip_src(0), ip_dst(0), port_src(0), port_dst(0), protocol(0),
                   ip_hdrlen(0), ip_len(0),
                   tcp_hdrlen(0), seq(0), ack(0), win(0), syn(0),
                   ts(0), flow_key(0), total_len(0), acc_len(0) {}

    /**
     * @brief Constructor II.
     */
    wf_tuple_t(uint32_t _ip_src, uint32_t _ip_dst, uint16_t _port_src, uint16_t _port_dst, uint8_t _protocol) :
            ip_src(_ip_src), ip_dst(_ip_dst),
            port_src(_port_src), port_dst(_port_dst),
            protocol(_protocol),
            ip_hdrlen(0), ip_len(0),
            tcp_hdrlen(0), seq(0), ack(0), win(0), syn(0),
            ts(0), flow_key(0), total_len(0), acc_len(0) {}

    /**
    * @brief Constructor III.
    */
    wf_tuple_t(uint64_t _key, uint64_t _id) :
            ip_src(0), ip_dst(0), port_src(0), port_dst(0), protocol(0),
            ip_hdrlen(0), ip_len(0),
            tcp_hdrlen(0), seq(0), ack(0), win(0), syn(0),
            ts(0), flow_key(_key), total_len(0), acc_len(_id) {}

    /**
     * @brief Translates an IPv4 address from binary to text form.
     *
     * The address is a local field of the current tuple object.
     *
     * @param addr_field flag used to select between ip_src (0) and ip_dst (1)
     * @return the address as a string
     */
    [[nodiscard]] std::string local_addr_to_string(const int& addr_field) const {
        char buf[16];
        uint32_t addr = (!addr_field) ? ip_src : ip_dst;

        // convert IPv4 address from binary to text form
        inet_ntop(AF_INET, reinterpret_cast<const void*>(&addr), buf, sizeof(buf));

        return std::string{buf};
    }

    /**
     * @brief Translates an IPv4 address from binary to text form.
     *
     * @param addr the address in binary format
     * @return the address as a string
     */
    static std::string addr_to_string(const uint32_t& addr) {
        char buf[16];

        // convert IPv4 address from binary to text form
        inet_ntop(AF_INET, reinterpret_cast<const void*>(&addr), buf, sizeof(buf));

        return std::string{buf};
    }

    /**
     * @brief Prints the content of a wf tuple on stdout.
     *
     * @return string representing the tuple content
     */
    [[nodiscard]] std::string print() const {
        std::stringstream ss;

        /// timestamp (microseconds)
        ss << "ts: " << ts << ", ";

        /// source and destination (IP addresses)
        ss << "src: " << addr_to_string(ip_src) << ", dst: " << addr_to_string(ip_dst) << ", ";

        /// transport layer protocol
        ss << "proto: " << htons(ntohs(protocol)) << ", ";

        /// entire packet length
        ss << "length: " << ntohs(ip_len) + 18 << " ";      // 14 bytes MAC header (6 src + 6 dst + 2 ethertype) + 4 bytes CRC checksum

        /// IP length (total, header, payload)
        ss << "[IP - len: " << ntohs(ip_len) << ", ";           // convert from network byte order to host byte order (short, 2 bytes)
        ss << "hdrlen: " << ip_hdrlen << ", datalen: " << ntohs(ip_len) - ip_hdrlen << "] ";

        /// TCP length (header, payload)
        ss << "[TCP - hdrlen: " << tcp_hdrlen << ", datalen: " << ntohs(ip_len) - ip_hdrlen - tcp_hdrlen << "], ";

        /// ports and info
        ss << "[INFO: " << ntohs(port_src) << "->" << ntohs(port_dst) << ", ";
        ss << "seq: " << ntohl(seq) << ", ";         // convert from network byte order to host byte order (long, 4 bytes)
        ss << "ack: " << ntohl(ack) << ", ";
        ss << "win: " << win << ", ";
        ss << "syn: " << syn;

        ss << "]" << std::endl;

        return ss.str();
    }

    /**
     * @brief Prints the content of a wf tuple essential for the application on stdout.
     *
     * @return string representing the essential tuple content
     */
    [[nodiscard]] std::string print_essential(const int& len_field) const {
        std::stringstream ss;

        /// timestamp (microseconds)
        ss << "ts: " << ts << ", ";

        /// source and destination (IP addresses)
        ss << "src: " << addr_to_string(ip_src) << ", dst: " << addr_to_string(ip_dst) << ", ";

        /// flow
        ss << "flow: " << flow_key << ", ";

        /// byte lengths
        if (len_field == 0) {
            ss << "len: " << total_len;
        } else if (len_field == 1) {
            ss << "flow_len: " << acc_len;
        }

        return ss.str();
    }
};

#endif //HH_WF_TUPLE_HPP