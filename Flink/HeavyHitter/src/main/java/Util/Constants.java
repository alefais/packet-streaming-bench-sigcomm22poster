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
 *  @version 31/05/2022
 *
 *  Constants peculiar of the HeavyHitter application.
 */
package Util;

public interface Constants {

    interface InputParameters {
        String HELP = "help";
        String HELP_MSG = "Run Heavy Hitter with the following parameters:\n" +
                            "[ -i interface ] " +
                            "[ -p nSource,nFlowId,nWinAcc,nDetector,nSink ] " +
                            "[ -b batch ] [ -w win length usec ] [ -s win slide usec ] [ -c ]";
        String PARSING_ERR = "Error in parsing input arguments. Use the --help option to see how to run the application.";
    }

    interface ExecutionConf {
        String PROPERTIES_FILE = "/hh.properties";
        String INPUT_FILE = "hh.input.file";
        String SOURCE_THREADS = "hh.source.threads";
        String FLOWID_THREADS = "hh.flowid.threads";
        String ACC_THREADS = "hh.acc.threads";
        String DETECTOR_THREADS = "hh.detector.threads";
        String SINK_THREADS = "hh.sink.threads";
        String ALL_THREADS = "hh.all.threads";
    }

    interface AppConf {
        String CHAINING = "hh.chaining";
        String WIN_LEN = "hh.win.len";
        String WIN_SLIDE = "hh.win.slide";
        String WIN_TYPE = "hh.win.implementation";

        String BATCH_LEN = "hh.batch.len";
        String GEN_RATE = "hh.gen.rate";
        String THRESHOLD = "hh.threshold";
        String APP_RUNTIME = "hh.app.runtime";       // topology is alive for X seconds
    }

    interface Components {
        String TOPOLOGY = "HeavyHitter";
        String SOURCE = "Source";
        String FLOW_ID = "FlowIdentifier";
        String BYTE_ACC = "ByteAccumulator";
        String DETECTOR = "Detector";
        String SINK = "Sink";
    }

}
