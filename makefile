############################
# Copyright (c) 2005 - 2016 Hewlett Packard Enterprise Development LP
#
# Makefile to build the Flex Table package
#
############################

######################################################################
# Variables Setup
######################################################################

### Package Name
PACKAGE_NAME=FlexTable

## Set to the location of the SDK installation
SDK?=/opt/vertica/sdk
SDK_LIB?=$(RPM_BUILDROOT)/Linux64/bin
VSQL?=/opt/vertica/bin/vsql

## Source files to compile
SOURCE_FILES=src/FlexTable.h src/VMapPair.h src/VMap.h src/Library.cpp src/Dict.h src/Delimited.h src/JSONParseStack.h src/JSONParseHandlers.h src/JSONParser.h src/CEF.h src/DelimitedPair.h src/IDX.h src/VMap.cpp src/Dict.cpp src/Delimited.cpp src/JSONParser.cpp src/JSONParseStack.cpp src/CEF.cpp src/DelimitedPair.cpp src/IDX.cpp src/Regex.h src/Regex.cpp src/FlexTokenizer.h src/FlexTokenizer.cpp src/Csv.h src/Csv.cpp src/AvroDatumParsers.h src/AvroParser.cpp src/AvroParser.h src/CRReader.cc src/CRReader.hh src/CRStream.cc src/CRStream.hh $(SDK)/examples/HelperLibraries/StringParsers.h $(SDK)/examples/HelperLibraries/DelimitedRecordChunker.h src/FlexParsersSupport.h

## Set to the locations of where to #include the Vertica SDK and third-party libraries
VERTICA_SDK_INCLUDE = $(SDK)/include
THIRD_PARTY         = $(shell pwd)/third-party
THIRD_PARTY_INCLUDE = build/include
THIRD_PARTY_LIB     = build/lib
JSONCPP_DIR = $(THIRD_PARTY)/jsoncpp
JSONCPP_TBZ = $(JSONCPP_DIR)/jsoncpp-src-0.6.0-rc2.tar.gz
JSONCPP_SRC = $(JSONCPP_DIR)/jsoncpp-src-0.6.0-rc2
SCONS_DIR = $(THIRD_PARTY)/scons
SCONS_TBZ = $(SCONS_DIR)/scons-local-2.1.0.tar.gz
SCONS_SRC = $(SCONS_DIR)/scons-local-2.1.0
SCONS = $(SCONS_DIR)/scons.py
TAR = tar
INSTALL_DIR ?= $(THIRD_PARTY)/../build
SNAPPY_DIR = $(THIRD_PARTY)/snappy
SNAPPY_TBZ = $(SNAPPY_DIR)/snappy-1.1.2.tar.gz
SNAPPY_SRC = $(SNAPPY_DIR)/snappy-1.1.2

### Set to the path for PCRE; used by the RegEx parser
PCRELIB = $(THIRD_PARTY)/install/lib/libpcre.a
# Load PCRE configuration options. Add target installation directory
PCRE_CONF = $(shell cat $(THIRD_PARTY)/pcre.conf) --prefix=$(THIRD_PARTY)/install

CXX=g++
CXXFLAGS+=-I $(VERTICA_SDK_INCLUDE) -I $(SDK)/examples/HelperLibraries -I $(THIRD_PARTY)/pcre -g -Wall -Wno-unused-value -shared -fPIC -I $(THIRD_PARTY_INCLUDE) -I $(THIRD_PARTY_INCLUDE)/avro

## Needed by 'make test'
SHELL=/bin/bash

ifndef DEBUG
## UDLs should be compiled with compiler optimizations in release builds
CXXFLAGS:=$(CXXFLAGS) -O2 -fno-strict-aliasing
endif

## Set to the desired destination directory for .so output files
BUILD_DIR?=lib

## Set to the path where you installed Boost library
BOOST_ROOT ?= $(INSTALL_DIR)

## Set to the path for cmake; used to compile yajl
## We need cmake 2.8.  Check for a "cmake28" binary;
## or use the system "cmake" binary if it's new enough,
## or fall back to building one if needed.
CMAKE := $(shell which cmake28 2>/dev/null || (test -n "$$(cmake --version 2>/dev/null | grep 2.8)" && which cmake 2>/dev/null) || echo "$(THIRD_PARTY)/cmake-2.8.10.2/bin/cmake")

######################################################################
# Targets
######################################################################

## Top-level, default target building the release package
all: flextable_package

# Compile the CMake third-party tool used to compile YAJL, if not present on the target system
$(CMAKE):
ifeq ($(shell which cmake28 2>/dev/null),)
	(cd $(THIRD_PARTY); tar xzf cmake-2.8.10.2.tar.gz)
	(cd $(THIRD_PARTY)/cmake-2.8.10.2; ./configure && $(MAKE))
else
	CMAKE?=$(shell which cmake28)
endif

# Make sure the output directory exists
$(BUILD_DIR)/.exists:
	test -d $(BUILD_DIR) || mkdir -p $(BUILD_DIR)
	touch $(BUILD_DIR)/.exists

$(INSTALL_DIR)/.exists:
	test -d $(INSTALL_DIR) || mkdir -p $(INSTALL_DIR)
	@mkdir -p $(INSTALL_DIR)/src
	@mkdir -p $(INSTALL_DIR)/include
	@mkdir -p $(INSTALL_DIR)/lib
	touch $@

###
# Release flextable_package compile
###
flextable_package: $(BUILD_DIR)/.exists $(BUILD_DIR)/flextable_package.so

$(BUILD_DIR)/flextable_package.so: $(SNAPPY_SRC)/.done $(INSTALL_DIR)/src/boost_1_50_0/.done $(INSTALL_DIR)/src/avro-cpp-1.7.0/.done $(THIRD_PARTY)/yajl/build/yajl-2.0.5/lib/libyajl_s.a $(JSONCPP_SRC)/libs/libjson.a $(PCRELIB) $(SOURCE_FILES) $(VERTICA_SDK_INCLUDE)/Vertica.cpp $(VERTICA_SDK_INCLUDE)/BuildInfo.h $(BUILD_DIR)/.exists
	@if [ -a $(THIRD_PARTY_LIB)/libavrocpp_s.a ] ; \
	then \
		echo "Compiling FlexTable library..." ;\
		$(CXX) $(CXXFLAGS) -I $(THIRD_PARTY)/yajl/build/yajl-2.0.5/include/ -I $(JSONCPP_SRC)/include/ -I $(SNAPPY_SRC)/include/ -I $(THIRD_PARTY_INCLUDE)  -o $@ $(SOURCE_FILES) $(VERTICA_SDK_INCLUDE)/Vertica.cpp $(THIRD_PARTY)/yajl/build/yajl-2.0.5/lib/libyajl_s.a $(JSONCPP_SRC)/libs/libjson.a $(PCRELIB) $(THIRD_PARTY_LIB)/libavrocpp_s.a $(SNAPPY_SRC)/lib/libsnappy.a ;\
	else \
		echo "Compiling FlexTable library (without Avro parser)..." ;\
		$(CXX) -DNAVRO $(CXXFLAGS) -I $(THIRD_PARTY)/yajl/build/yajl-2.0.5/include/ -I $(JSONCPP_SRC)/include/ -I $(SNAPPY_SRC)/include/ -I $(THIRD_PARTY_INCLUDE)  -o $@ $(SOURCE_FILES) $(VERTICA_SDK_INCLUDE)/Vertica.cpp $(THIRD_PARTY)/yajl/build/yajl-2.0.5/lib/libyajl_s.a $(JSONCPP_SRC)/libs/libjson.a $(PCRELIB) $(SNAPPY_SRC)/lib/libsnappy.a ;\
	fi;

# Build YAJL for the FJSONParser (third-party, requires CMake to build)
$(THIRD_PARTY)/yajl/build/yajl-2.0.5/lib/libyajl_s.a: $(CMAKE)
	(cd $(THIRD_PARTY); mkdir -p yajl; tar -zxf yajl-2.0.4.tar.gz  -C yajl --strip-components=1; cp -f yajl-CMakeLists-staticlink yajl/CMakeLists.txt)
	(cd $(THIRD_PARTY)/yajl/; mkdir -p build)
	(cd $(THIRD_PARTY)/yajl/build; $(CMAKE) ..; $(MAKE))

## Build PCRE support for the FRegexParser (third-party)
$(PCRELIB): $(THIRD_PARTY)/pcre.conf
	(cd $(THIRD_PARTY); mkdir -p pcre; tar xfj pcre-8.33.tar.bz2 -C pcre --strip-components=1)
	(cd $(THIRD_PARTY)/pcre/; CFLAGS='-fPIC' ./configure $(PCRE_CONF); $(MAKE); $(MAKE) install)

# SCons is used to build JSONCPP
$(SCONS_DIR)/scons-local-2.1.0:
	@echo "Unpacking, configuring and installing scons...";
	(cd $(SCONS_DIR); tar xzf $(SCONS_TBZ))

# Build JSONCPP support for MapToString() (third-party)
$(JSONCPP_SRC)/libs/libjson.a:  $(SCONS_DIR)/scons-local-2.1.0
	@echo "Unpacking, configuring and installing jsoncpp...";
	(cd $(JSONCPP_DIR); tar xzf $(JSONCPP_TBZ))
	(cd $(JSONCPP_DIR); patch -p0 < $(JSONCPP_DIR)/json.patch)
	(cd $(JSONCPP_SRC); $(SCONS) platform=linux-gcc)
	(cd $(JSONCPP_SRC); cp `find libs -name "*.a" | head -n1 ` $(JSONCPP_SRC)/libs/libjson.a)

#Boost and Avro
.PHONY: boost
boost: $(INSTALL_DIR)/src/boost_1_50_0/.done

$(INSTALL_DIR)/src/boost_1_50_0/.done: $(INSTALL_DIR)/.exists $(INSTALL_DIR)/.exists
	@if [ -a $(THIRD_PARTY)/boost/boost_1_50_0.tar.gz ] ;\
	then \
		echo "Unpacking, configuring and installing boost..." ;\
		(cd $(INSTALL_DIR)/src; $(TAR) xzf $(THIRD_PARTY)/boost/boost_1_50_0.tar.gz) ;\
		(cd $(INSTALL_DIR)/src/boost_1_50_0; ./bootstrap.sh --prefix=$(INSTALL_DIR) ) ;\
		(cd $(INSTALL_DIR)/src/boost_1_50_0; cxxflags=-fPIC ./b2 cxxflags="-fPIC" --exec-prefix=$(INSTALL_DIR) --link=static --libdir=$(INSTALL_DIR)/lib --includedir=$(INSTALL_DIR)/include --with-filesystem --with-program_options install) ;\
		touch $@ ;\
	else if [ $(BOOST_ROOT) == $(INSTALL_DIR) ] ; \
	then \
		echo "WARNING: C++ Boost not found..." ;\
		mkdir -p $(INSTALL_DIR)/src/boost_1_50_0 ;\
		touch $@ ;\
	else \
		echo "Using C++ Boost root directory defined at:" $(BOOST_ROOT) ;\
		mkdir -p $(INSTALL_DIR)/src/boost_1_50_0 ;\
		touch $@ ;\
	fi \
	fi;

.PHONY: avro
avro: $(INSTALL_DIR)/src/avro-cpp-1.7.0/.done

$(INSTALL_DIR)/src/avro-cpp-1.7.0/.done: $(INSTALL_DIR)/src/boost_1_50_0/.done
	@if [ -a $(BOOST_ROOT)/include/boost ] ; \
	then \
		echo "Unpacking, configuring and installing avro..." ;\
		(cd $(INSTALL_DIR)/src; $(TAR) xzf $(THIRD_PARTY)/avro-cpp-1.7.0.tar.gz) ;\
		(cd $(INSTALL_DIR)/src/avro-cpp-1.7.0; BOOST_ROOT=$(BOOST_ROOT) $(CMAKE) -G "Unix Makefiles" -DCMAKE_CXX_FLAGS_RELEASE:STRING="-fPIC -Wall -O3 -DNDEBUG"; make package; ./avrocpp-1.7.0.sh --prefix=$(INSTALL_DIR) --skip-license --exclude-subdir) ;\
		touch $@ ;\
	else \
		echo "WARNING: Flex Avro Parser will not be installed. C++ Boost not found..." ;\
		mkdir -p $(INSTALL_DIR)/src/avro-cpp-1.7.0 ;\
		touch $@ ;\
	fi;

.PHONY: snappy
snappy: $(SNAPPY_SRC)/.done

$(SNAPPY_SRC)/.done:
	@echo "Unpacking, configuring and installing snappy...";
	(cd $(SNAPPY_DIR); tar xzf $(SNAPPY_TBZ))
	(cd $(SNAPPY_SRC); CXXFLAGS="-fPIC -Wall -DNDEBUG" ./configure --disable-shared --prefix=$(SNAPPY_SRC); $(MAKE); $(MAKE) install)
	touch $@

###
# Doxygen documentation
###
.PHONY: doc
doc:
	(cd doc; doxygen)

###
# FlexTable examples
###
TEST_DIR=$(shell pwd)/test
.PHONY: flex_test

flex_test: flextable_package
	@echo 'Installing Test FlexTable library and functions...'
	@FLEX_LIBFILE=$(shell readlink -f $(BUILD_DIR)) $(VSQL) -f $(TEST_DIR)/test_install.sql
	@echo 'Running test FlexTable queries...'
	@$(VSQL) -f $(TEST_DIR)/queries.sql
	@echo 'Uninstalling Test FlexTable library and functions...'
	@$(VSQL) -f $(TEST_DIR)/test_uninstall.sql

######################################################################
# Cleaning targets
######################################################################

# Remove Flexible Tables-specific build outputs
clean:
	@rm -rf lib
	@rm -rf doc/html
	@rm -rf doc/latex
	@rm -rf doc/rtf

# Remove Flexible Tables and dependent build outputs
clean_all: clean
	@rm -rf $(THIRD_PARTY)/cmake-2.8.10.2
	@rm -rf $(THIRD_PARTY)/yajl
	@rm -rf $(THIRD_PARTY)/pcre
	@rm -rf $(THIRD_PARTY)/install
	@rm -rf $(THIRD_PARTY)/scons/scons-local-2.1.0
	@rm -rf $(THIRD_PARTY)/jsoncpp/jsoncpp-src-0.6.0-rc2
	@rm -rf avro/.done
	@rm -rf $(INSTALL_DIR)
	@rm -rf $(THIRD_PARTY)/snappy/snappy-1.1.2

-include makefile.ver
