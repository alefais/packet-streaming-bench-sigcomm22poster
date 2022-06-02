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
 *  @file    util.hpp
 *  @author  Alessandra Fais
 *  @date    25/05/2022
 *
 *  @brief Utility file defining utilities for parsing command line options and printing information on stdout.
 */

#pragma once
#ifndef HH_UTIL_HPP
#define HH_UTIL_HPP

#include <getopt.h>
#include <string>

namespace cli {
    /// manage command line options
    typedef enum {NONE, REQUIRED} opt_arg;    // an option can require one argument or none

    const struct option long_opts[] = {
            {"help", NONE, 0, 'h'},
            {"interface", REQUIRED, 0, 'i'},
            {"parallelism", REQUIRED, 0, 'p'},
            {"batch", REQUIRED, 0, 'b'},
            {"win", REQUIRED, 0, 'w'},
            {"slide", REQUIRED, 0, 's'},
            {"threshold", REQUIRED, 0, 't'},
            {"chaining", NONE, 0, 'c'},
            {0, 0, 0, 0}
    };

    /// instructions to run the application
    const std::string help = "Run Heavy Hitter with the following parameters:\n[ -i input ] " \
                             "[ -p nSource,nFlowId,nWinAcc,nDetector,nSink ]  [ -b batch ] " \
                             "[ -w win length ms ] [ -s win slide ms ] [ -c ]";

    /// error message
    const std::string parsing_error = "Error in parsing input arguments. Use the --help option to see how to run the application.";
}

#endif //HH_UTIL_HPP
