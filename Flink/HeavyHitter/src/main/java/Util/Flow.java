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
 *  @version 25/05/2022
 *
 * Definition of a minimal network flow.
 */
package Util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

/**
 * A Flow is defined here by two values: source IP address and destination IP address.
 */
public class Flow {

    private static final Logger LOG = LoggerFactory.getLogger(Flow.class);

    private final String srcIP;
    private final String dstIP;
    private final int hash;

    public Flow(String srcIP_addr, String dstIP_addr, int hash_type) {
        this.srcIP = srcIP_addr;
        this.dstIP = dstIP_addr;
        this.hash = hash_type;
    }

    /**
     * Hashing function defined for objects of type Flow.
     * @return the result as an integer value
     */
    @Override
    public int hashCode() {
        return hash(this.hash);     // select hash algorithm here
    }

    /** Defines the actual hash algorithm to be applied.
     * @param selection hash algorithm to use
     * @return computed hash value
     */
    private int hash(int selection) {
        if (selection == 0) {
            return Objects.hash(this.srcIP, this.dstIP);
        } else {
            int hash = 17;
            hash = hash * 17 + this.srcIP.hashCode();
            hash = hash * 17 + this.dstIP.hashCode();
            return hash;
        }
    }
}
