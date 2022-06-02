# Author: Alessandra Fais
# Date:   04/05/2022

BUILD_DIR		= ../build
LOCAL_INCLUDES	= ../includes

FF_INCLUDES		= $(HOME)/fastflow
WF_INCLUDES		= $(HOME)/WindFlow/wf
NET_INCLUDES	= /usr/local/include/nethuns
PCAP_INCLUDES	= /usr/include/pcap

CXX 			= g++
CXXFLAGS		= -std=c++17
INCLUDES		= -I $(FF_INCLUDES) -I $(WF_INCLUDES) -I $(NET_INCLUDES) -I $(PCAP_INCLUDES) -I $(LOCAL_INCLUDES)
MACRO           = -DFF_BOUNDED_BUFFER #-DPRINT_OP_RESULTS #-DPRINT_OP_SUMMARY #-DTWO_OPS #-DDEBUG_PRINT #-DDEBUG_PRINT_METRIC -DDEBUG_PRINT_PARSER
OPTFLAGS		= -g -O3 -finline-functions
LDFLAGS			= -pthread
LIBFLAGS		= -lpcap -L $(LOCAL_INCLUDES)/util/alglib -lalglib

all: hh

# compile every *.cpp to *.o ($@ evaluates to %.o, $< evaluates to %.cpp)
hh.o: hh.cpp
	$(CXX) $(CXXFLAGS) $(INCLUDES) $(MACRO) $(OPTFLAGS) -o $@ -c $<

# create the application executable from all the *.o
hh: hh.o
	$(CXX) $^ -o ../hh.out $(LDFLAGS) $(LIBFLAGS)
	if [ ! -d $(BUILD_DIR) ]; then mkdir -p $(BUILD_DIR); mv ./*.o $(BUILD_DIR); fi

clean:
	rm -f $(BUILD_DIR)/*.o
	rm -rf $(BUILD_DIR)
	rm -f ../hh.out

.DEFAULT_GOAL := all
.PHONY: all clean