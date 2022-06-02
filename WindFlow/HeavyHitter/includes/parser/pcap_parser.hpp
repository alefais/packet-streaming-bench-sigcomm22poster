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
 *  @file    pcap_parser.hpp
 *  @author  Alessandra Fais
 *  @date    09/03/2022
 *
 *  @brief Helper class implementing a parser for pcap dump files.
 */

#pragma once
#ifndef PCAP_PARSER_HPP
#define PCAP_PARSER_HPP

#include <iostream>
#include <fstream>
#include <optional>
#include <string>
#include <tuple>
#include <vector>
#include <stdexcept>
#include <ctime>
#include <pcap.h>
#include <arpa/inet.h>
#include <net/ethernet.h>
#include <netinet/ether.h>
#include <netinet/ip_icmp.h>
#include <netinet/tcp.h>
#include <netinet/udp.h>
#include "tuples/wf_tuple.hpp"

/**
 * @brief Tokenize the content of a single packet from the csv file.
 * 
 * FieldFormat (where seq, ack, win are valid for TCP packets only) and corresponding access positions:
 * 
 *      ts, ip_src, ip_dst, ip_hdrlen, ip_len, protocol, port_src, port_dst, hdrlen, length, seq, ack, win
 *       0     1      2         3         4       5        6         7         8       9     10   11   12
 * 
 */
typedef std::tuple<uint64_t, uint32_t, uint32_t, uint16_t, uint16_t, uint8_t, 
                    uint16_t, uint16_t, uint16_t, uint16_t, uint32_t, uint32_t, uint16_t> tokenized_packet;

/**
 * @class PcapTransformer
 * 
 * @brief Class which can parse a pcap file and transform it to a csv formatted text file.
 *        It can also generate a dataset of wf tuples from the packets of the pcap dump file. 
 */
class PcapTransformer {
private:
    //----------------------- subclasses ---------------------------------------//

    /**
     * @class PcapParser 
     * 
     * @brief Sub-class to parse the initial pcap file.
     */
    class PcapParser {
    private:
        /// 802.1Q header structure.
        struct vlan_ethhdr {
            u_char h_dest[ETHER_ADDR_LEN];
            u_char h_source[ETHER_ADDR_LEN];
            u_int16_t h_vlan_proto;
            u_int16_t h_vlan_TCI;
            u_int16_t h_vlan_encapsulated_proto;
        };

        //----------------------- variables ------------------------------------//

        /// pcap handle
        pcap_t* pcap_handle;

        /// stringstream to store the pcap file relevant content
        std::stringstream pcap_ss;

        /// stringstream to store the pcap file relevant content in a human readable format
        std::stringstream pcap_ss_read;

        /// tuple dataset to store the pcap file relevant content
        std::vector<wf_tuple_t> pcap_dataset;

        //----------------------- auxiliary methods ----------------------------//

        /**
         * @brief Callback function called on every packet read from the pcap file.
         * 
         * @param user 
         * @param p_hdr 
         * @param p_data 
         */
        static void userRoutine(u_char *user, const struct pcap_pkthdr *p_hdr, const u_char *p_data) {
            PcapParser* this_obj = reinterpret_cast<PcapParser*>(user);
            this_obj->parsePacket(p_hdr, p_data);
        }

        /**
         * @brief Packet handler executed from inside the callback function.
         * 
         * @param p_hdr 
         * @param p_data 
         */
        void parsePacket(const struct pcap_pkthdr* p_hdr, const u_char* p_data) {
            wf_tuple_t t;       // fill this wf tuple with the content of the current packet

            /// here we have a valid packet to process
            t.ts = p_hdr->ts.tv_sec * (uint64_t)1000000 + p_hdr->ts.tv_usec;   // ts in microseconds (epoch time)

            /// access ethernet header
            const struct ether_header *e_hdr = reinterpret_cast<const struct ether_header *>(p_data);
            if (e_hdr == nullptr) throw std::invalid_argument("[PcapParser] ERR: failed access to ethernet header while parsing packet.");

            /// check presence of vlan header
            const struct vlan_ethhdr *vlan_hdr = nullptr;
            if (e_hdr->ether_type == htons(ETHERTYPE_VLAN))
                vlan_hdr = reinterpret_cast<const struct vlan_ethhdr *>(p_data);

            /// access IP header
            const struct ip *ip_hdr = (vlan_hdr) ?
                                    reinterpret_cast<const struct ip *>(vlan_hdr + 1) :
                                    reinterpret_cast<const struct ip *> (e_hdr + 1);
            if (ip_hdr == nullptr) throw std::invalid_argument("[PcapParser] ERR: failed access to IP header while parsing packet.");
            t.ip_src = reinterpret_cast<uint32_t>(ip_hdr->ip_src.s_addr);      // IP src (keep binary format, print fun will convert address to text form)
            t.ip_dst = reinterpret_cast<uint32_t>(ip_hdr->ip_dst.s_addr);      // IP dst (keep binary format, print fun will convert address to text form)
            t.ip_len = reinterpret_cast<uint16_t>(ip_hdr->ip_len);             // IP packet length in bytes, including header and data (network representation)
            t.ip_hdrlen = ip_hdr->ip_hl * 4;    // IP header length in bytes (HdrLen field specifies the size of the IP header in 32-bit words), assume no options

            /// access transport layer header
            const struct tcphdr *tcp_hdr = reinterpret_cast<const struct tcphdr *>(reinterpret_cast<const char *>(ip_hdr) + (ip_hdr->ip_hl * 4));

            /// select TCP packets only
            if (ip_hdr->ip_p == IPPROTO_TCP) {
                t.protocol = IPPROTO_TCP;
                if (tcp_hdr != nullptr) {
                    t.port_src = tcp_hdr->th_sport;   // network representation
                    t.port_dst = tcp_hdr->th_dport;   // network representation
                    t.tcp_hdrlen = tcp_hdr->doff *
                                   4; // length in bytes of TCP header (DataOffset field specifies the size of the TCP header in 32-bit words)
                    t.seq = tcp_hdr->seq;             // sequence number of the first data byte (network representation)
                    t.ack = ((tcp_hdr->ack) ? tcp_hdr->ack_seq
                                            : 0);  // acknowledgment number (first sequence number that the sender of the ACK is expecting, if ACK=1) (network representation)
                    t.win = tcp_hdr->window;          // size of the receive window in bytes
                    t.syn = tcp_hdr->syn;          // SYN flag (if set to 1 it identifies a newly opened TCP connection)
                }

                pcap_dataset.push_back(t);
                pushString(t);
                pushReadableString(t);
            }
        }

        /**
         * @brief Define the format of the tuple fields for generating the standard csv file.
         *
         * @param t packet tuple to add in the csv standard file
         */
        void pushString(const wf_tuple_t& t) {
            /// timestamp (microseconds)
            pcap_ss << t.ts << ",";

            /// source and destination (IP addresses)
            pcap_ss << t.ip_src << ",";                 // keep IPv4 address in binary format
            pcap_ss << t.ip_dst << ",";

            /// transport layer protocol
            pcap_ss << htons(ntohs(t.protocol)) << ",";

            /// entire packet length
            pcap_ss << ntohs(t.ip_len) + 18 << ",";      // 14 bytes MAC header (6 src + 6 dst + 2 ethertype) + 4 bytes CRC checksum

            /// IP length (total, header, payload)
            pcap_ss << ntohs(t.ip_len) << ",";           // convert from network byte order to host byte order (short, 2 bytes)
            pcap_ss << t.ip_hdrlen << ",";
            pcap_ss << ntohs(t.ip_len) - t.ip_hdrlen << ",";

            /// TCP length (header, payload)
            pcap_ss << t.tcp_hdrlen << ",";
            pcap_ss << ntohs(t.ip_len) - t.ip_hdrlen - t.tcp_hdrlen << ",";
            pcap_ss << ntohs(t.port_src) << ",";
            pcap_ss << ntohs(t.port_dst) << ",";
            pcap_ss << ntohl(t.seq) << ",";         // convert from network byte order to host byte order (long, 4 bytes)
            pcap_ss << ntohl(t.ack) << ",";
            pcap_ss << t.win << ",";
            pcap_ss << t.syn;

            pcap_ss << std::endl;
        }

        /**
         * @brief Define the format of the tuple fields for generating the human-readable csv file.
         *
         * @param t packet tuple to add in the csv human-readable file
         */
        void pushReadableString(const wf_tuple_t& t) {
            /// timestamp (microseconds)
            pcap_ss_read << t.ts << ",";
            
            /// source and destination (IP addresses)
            char ipsrc_buf[16], ipdst_buf[16];
            inet_ntop(AF_INET, reinterpret_cast<const void*>(&t.ip_src), ipsrc_buf, sizeof(ipsrc_buf));  // convert IPv4 address from binary to text form
            inet_ntop(AF_INET, reinterpret_cast<const void*>(&t.ip_dst), ipdst_buf, sizeof(ipdst_buf));  // convert IPv4 address from binary to text form
            pcap_ss_read << ipsrc_buf << ",";
            pcap_ss_read << ipdst_buf << ",";

            /// transport layer protocol
            pcap_ss_read << htons(ntohs(t.protocol)) << ",";
            
            /// entire packet length
            pcap_ss_read << ntohs(t.ip_len) + 18 << ",";      // 14 bytes MAC header (6 src + 6 dst + 2 ethertype) + 4 bytes CRC checksum

            /// IP length (total, header, payload)
            pcap_ss_read << ntohs(t.ip_len) << ",";           // convert from network byte order to host byte order (short, 2 bytes)
            pcap_ss_read << t.ip_hdrlen << ",";
            pcap_ss_read << ntohs(t.ip_len) - t.ip_hdrlen << ",";

            /// TCP length (header, payload)
            pcap_ss << t.tcp_hdrlen << ",";
            pcap_ss << ntohs(t.ip_len) - t.ip_hdrlen - t.tcp_hdrlen << ",";
            pcap_ss << ntohs(t.port_src) << ",";
            pcap_ss << ntohs(t.port_dst) << ",";
            pcap_ss << ntohl(t.seq) << ",";         // convert from network byte order to host byte order (long, 4 bytes)
            pcap_ss << ntohl(t.ack) << ",";
            pcap_ss << t.win << ",";
            pcap_ss << t.syn;

            pcap_ss_read << std::endl;
        }
    
    public:
        //--------------------- constructors & methods -------------------------//

        /**
         * @brief Construct a new PcapParser object.
         * 
         * @param _pcap_file 
         */
        PcapParser(const std::string& _pcap_file) {
            /// open pcap dump file
            char errbuf[PCAP_ERRBUF_SIZE];
            pcap_handle = pcap_open_offline(_pcap_file.c_str(), errbuf);
            if (pcap_handle == nullptr)
                throw std::invalid_argument("[PcapParser] ERR: null pcap handle");
        }

        /**
         * @brief Read all the packets from the pcap dump file until EOF is reached.
         * 
         */
        void parseAll() {
            if (pcap_loop(pcap_handle, -1, userRoutine, reinterpret_cast<u_char*>(this)) == -1) {
                throw std::runtime_error("[PcapParser] ERR: failure in pcap parsing loop");
            }
        }

        /**
         * @brief Get the string representation of the pcap file relevant content.
         *        Packet fields are represented in csv format. 
         *        Packets are separated from each other by a newline character.
         * 
         * @return string containing all the packets
         */
        std::string getStrContent() {
            return std::move(pcap_ss.str());
        }

        /**
         * @brief Get the string representation of the pcap file relevant content.
         *        Packet fields are represented in csv format. 
         *        Packets are separated from each other by a newline character.
         *        All values are converted in human readable format.
         * 
         * @return string containing all the packets in human readable format
         */
        std::string getStrReadableContent() {
            return std::move(pcap_ss_read.str());
        }

        /**
         * @brief Get the dataset of wf tuples generated from the pcap file relevant content.
         * 
         * @return std::vector<wf_tuple_t> set of packet tuples
         */
        std::vector<wf_tuple_t> getDatasetContent() {
            return std::move(pcap_dataset);
        }

        /**
         * @brief Print the first n entries of the dataset of wf tuples generated from the initial pcap file.
         * 
         * @param _n entries to print (-1 value to print all the dataset content)
         */
        void printDatasetContent(const long long _n) {
            size_t n = (_n < 0) ? pcap_dataset.size() : _n;
#ifdef DEBUG_PRINT_PARSER
            std::cout << "Dataset size: " << pcap_dataset.size() << ", received _n: " << _n << ", new n: " << n << std::endl;
#endif
            for (int i = 0; i < n; i++) {
                std::cout << pcap_dataset.at(i).print() << std::endl;
            }
        }

        /**
         * @brief Destroy the PcapParser object
         * 
         */
        ~PcapParser() {
            pcap_close(pcap_handle);
        }
    };

    //----------------------- variables ----------------------------------------//

    /// parser object to parse the initial pcap file
    PcapParser pcap_parser;

public:
    //----------------------- constructors & methods ---------------------------//

    /**
     * @brief Construct a new PcapTransformer object.
     * 
     * @param _pcap_file 
     */
    PcapTransformer(const std::string& _pcap_file) :
        pcap_parser(_pcap_file) {
        try {
            /// parse pcap file
            pcap_parser.parseAll();
#ifdef DEBUG_PRINT_PARSER
            std::cout << "PcapTransformer constructor: pcap file parsed" << std::endl;
#endif
        } catch(const std::invalid_argument& e) {
            std::cerr << e.what() << std::endl;
            throw e;
        } catch (const std::runtime_error& e) {
            std::cerr << e.what() << std::endl;
            throw e;
        }
    }

    /**
     * @brief Generate a csv file from the pcap file content.
     * 
     * @param _csv_file 
     */
    void toCsv(const std::string& _csv_file) {
        /// generate csv file
        std::ofstream csv(_csv_file);
        if (csv.is_open()) {
            csv << pcap_parser.getStrContent();
            csv.close();
        }
    }

    /**
     * @brief Generate a human-readable csv file from the pcap file content.
     * 
     * @param _csv_file 
     */
    void toHumanReadableCsv(const std::string& _csv_file) {
        /// generate csv file
        std::ofstream csv(_csv_file);
        if (csv.is_open()) {
            csv << pcap_parser.getStrReadableContent();
            csv.close();
        }
    }

    /**
     * @brief Generate a dataset of wf tuples from the pcap file packet content.
     * 
     * @param _n option for dataset printing on stdout
     *        -1 -> print all the tuples in the dataset
     *         0  -> disable dataset printing
     *        >0 -> print out the first _n tuples in the dataset only
     * @return std::vector<wf_tuple_t> dataset of packet tuples
     */
    std::vector<wf_tuple_t> toTupleDataset(const long long _n) {
#ifdef DEBUG_PRINT_PARSER
        pcap_parser.printDatasetContent(_n);
#endif
        return pcap_parser.getDatasetContent();
    }

    /**
     * @brief Generate a dataset of wf tuples from a custom csv file representing the content of a pcap dump file.
     * 
     * @param _csv_file input csv file
     * @return std::vector<wf_tuple_t> dataset of packet tuples
     */
    std::vector<wf_tuple_t> toTupleDataset(const std::string& _csv_file) {
        std::vector<wf_tuple_t> dataset;
        /*
        std::ifstream csv(_csv_file);
        if (csv.is_open()) {
            std::string packet;
            tokenized_packet tok_packet;
            while (getline(csv, packet)) {
                size_t tok_idx = 0;
                size_t old_pos = 0;
                size_t pos = 0;
                while ((pos = packet.find(",", old_pos)) != std::string::npos) {
                    //std::get<0>(tok_packet) = packet.substr(old_pos, pos);        TODO complete implementation
                    old_pos = pos;
                }

            }
            csv.close();
        }*/
        return dataset;
    }

    /**
     * @brief Destroy the PcapTransformer object
     */
    ~PcapTransformer() {}
};

#endif //PCAP_PARSER_HPP