# Author: Alessandra Fais
# Date:   17/02/2022

# Makefile to build alglib descriptive statistics package as a static library

ALGLIB_BUILD_DIR = ./build

ALGLIB_SRC_FILES = $(wildcard src/*.cpp)
ALGLIB_OBJ_FILES = $(patsubst src/%.cpp,$(ALGLIB_BUILD_DIR)/%.o,$(ALGLIB_SRC_FILES))

CXX 			= g++
CXXFLAGS		= -fPIC -O0 -g -Wall -c -fpermissive

all: libalglib.a

# compile every *.cpp to *.o
$(ALGLIB_BUILD_DIR)/%.o: src/%.cpp
	if [ ! -d $(ALGLIB_BUILD_DIR) ]; then mkdir -p $(ALGLIB_BUILD_DIR); fi
	$(CXX) $(CXXFLAGS) -I ./src -o $@ -c $<

# create the static library from all the *.o
libalglib.a: $(ALGLIB_OBJ_FILES)
	ar -r -o $@ $^

clean:
	rm -f $(ALGLIB_BUILD_DIR)/*.o
	rm -rf $(ALGLIB_BUILD_DIR)
	rm -f libalglib.a

.DEFAULT_GOAL := all
.PHONY: all clean