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
 *  @author  Alessandra Fais
 *  @version 01/06/2022
 *
 *  Class that parses the input file and formats the input data for the computation.
 */
package Parser;

import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

/**
 * Provides the default dataset used for the HeavyHitter application.
 * The default dataset is used if no input file parameter is given to the program.
 */
public class PcapData {

    private static final Logger LOG = LoggerFactory.getLogger(PcapData.class);

    private static final ArrayList<String[]> PACKETS = new ArrayList<>();

    /**
     * Parses network traffic data from pcap input file and populate the source dataset.
     * The parsing phase splits each line (packet) of the input file in separated fields and only keeps the relevant ones for the computation.
     *
     * Input csv data format and corresponding access positions:
     *  ts, ip_src, ip_dst, protocol, pkt_len, ip_len_tot, ip_len_hdr, ip_len_pld, tr_len_hdr, tr_len_pld, port_src, port_dst, seq, ack, win
     *  0     1       2        3         4         5            6           7           8            9          10       11     12   13   14
     * (where seq, ack, win are valid for TCP packets only)
     *
     * Output field sequence:
     * - first field is the pcap timestamp
     * - from the second field to the fifth are the 5-tuple <srcIP, dstIP, srcPort, dstPort, protocol> that identify the network flow
     * - seventh field is the entire packet length in bytes
     *
     * @param input_file path of the input pcap file
     * @return a list of String sequences containing the parsed information ({@code ArrayList<String[]>})
     */
    public static ArrayList<String[]> parseDataset(String input_file) {
        try {
            long generated = 0;
            Scanner scan = new Scanner(new File(input_file));
            while (scan.hasNextLine()) {
                String[] pkt = scan.nextLine().split(",");
                // debug
                // System.out.println("[PcapData] pkt #" + generated + " (" + pkt.length + " fields)");
                // for (String field: pkt) {
                //    System.out.println(field);
                // }
                if (pkt.length > 11) {
                    // Extract and reorder packet fields [timestamp, srcIP, dstIP, srcPort, dstPort, protocol, pktLen]
                    String[] ordered_pkt = new String[]{pkt[0], pkt[1], pkt[2], pkt[10], pkt[11], pkt[3], pkt[4]};

                    // Insert in the database
                    PACKETS.add(ordered_pkt);
                    generated++;

                    LOG.debug("[PcapData] pkt: " + print(ordered_pkt));
                }
            }
            scan.close();
            LOG.debug("[PcapData] dataset size: " + PACKETS.size() + " packets");
            LOG.debug("[PcapData] generated tuples: " + generated);
            return new ArrayList<>(PACKETS);
        } catch (FileNotFoundException | NullPointerException e) {
            LOG.error("[PcapData] The file {} does not exist", input_file);
            throw new RuntimeException("The file '" + input_file + "' does not exist");
        }
    }

    /**
     * Generates a formatted string representing the packet content.
     * @param packet packet (sequence of String fields) to print
     * @return string representation
     */
    public static String print(String[] packet) {
        if (packet == null)
            return "empty";

        return "ts " + packet[0] + ", "
                + "src->dst " + packet[1] + ":" + packet[3] + " -> " + packet[2] + ":" + packet[4] + ", "
                + "prot " + packet[5] + ", "
                + "len " + packet[6];
    }

    /**
     * Generates a formatted string representing some application relevant packet fields.
     * @param packet stream element (tuple of fields) to print
     * @return string representation
     */
    public static String print(Values packet) {
        if (packet == null)
            return "empty";

        return "ts " + packet.get(3) + ", "
                + "dst " + packet.get(1) + ", "
                + "flow " + packet.get(0) + ", "
                + "len " + packet.get(2);
    }

    /**
     * Generates a formatted string representing the heavy hitter hosts detected.
     * @param results list of tuples containing the heavy hitters (destination hosts targeted by elephant flows) IP address
     * @return string representation of all the collected heavy-hit hosts
     */
    public static String printHeavyHitters(List<String> results) {
        if (results == null)
            return "";

        // collect results and create a string
        StringBuilder sb = new StringBuilder("[Heavy Hitters - GLOBAL REPORT]\nList of destination hosts targeted:\n");
        for (String r : results) {
            sb.append(r).append("\n");
        }

        return sb.toString();
    }
}
