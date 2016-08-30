/**
 * Copyright (c) 2005 - 2015 Hewlett Packard Enterprise Development LP
 *
 * Creation Date: July 31, 2014
 */
/**
 * Flex Table Regex parser
 */


#ifndef REGEX_H_
#define REGEX_H_


#include "VMap.h"

#include "Vertica.h"
#include "ContinuousUDParser.h"
#include "StringParsers.h"
#include "FlexParsersSupport.h"

#include "pcre.h"

namespace flextable
{


/**
 * Parser for Flex Tables, with data files in a simple Regex format.
 */
class FlexTableRegexParser : public ContinuousUDParser {
public:
        FlexTableRegexParser(std::string pattern, 
                             std::string recordTerminator = "\n", 
                             std::string loglinecolumn = "",
                             bool useJit = true);

/// Delete any char*'s constructucted during initialize()
    ~FlexTableRegexParser();


private:
    // Keep a copy of the information about each column.
    // Note that Vertica doesn't let us safely keep a reference to
    // the internal copy of this data structure that it shows us.
    // But keeping a copy is fine.
    SizedColumnTypes colInfo;

    // Size (in bytes) of the current record (row) that we're looking at.
    size_t currentRecordSize;

    // Configurable parsing parameters
    // Set by the constructor
    std::string pattern;
    std::string recordTerminator;
    std::string loglinecolumn;
    int32 loglinecol_index;

    // PCRE bits
    int maxColumns;
    int *ovec;
    int options;
    pcre *pattern_regex;
    pcre_extra *pattern_extra;
    pcre *rt_regex;
    pcre_extra *rt_extra;
    std::map<int,std::string> captureToColMap;
    std::map<int,int> colIndexToCaptureMap;

    // An instance of the class containing the methods that we're
    // using to parse strings to the various relevant data types
    //VFormattedStringParsers sp;
    StringParsers sp;

    ColLkup real_col_lookup;

    // 7.0/Crane map structure
    // The column number of the map data column (__raw__)
    int32 map_col_index;
    char* map_data_buf;
    ssize_t map_data_buf_len;
    char* pair_buf;

    bool is_flextable;

    ServerInterface *srvInterface;

    // Auxiliary buffer to normalize keys and store null-terminated values
    BufferWithAllocator<char> aux_buf;

    /**
     * Make sure (via reserve()) that the full upcoming row is in memory.
     * Assumes that getDataPtr() points at the start of the upcoming row.
     * (This is guaranteed by run(), prior to calling fetchNextRow().)
     *
     * Returns true if we stopped due to a record terminator;
     * false if we stopped due to EOF.
     */
    bool fetchNextRow();

    /**
     * Given a field in string form (a pointer to the first character and
     * a length), submit that field to Vertica.
     * `colNum` is the column number from the input file; how many fields
     * it is into the current record.
     *
     * Ordinarily, this method will probably want to do some parsing and
     * conversion of the input string.  For now, though, this example
     * only supports char and varchar columns, and only outputs strings,
     * so no parsing is necessary.
     */
    bool handle_field(size_t colNum, char* start, size_t len, bool hasPadding = false);

    void rejectRecord(const std::string &reason);


public:
    virtual void run();

    virtual void initialize(ServerInterface &srvInterface, SizedColumnTypes &returnType);
};


class FRegexParserFactory : public ParserFactory {
public:
    virtual void plan(ServerInterface &srvInterface,
            PerColumnParamReader &perColumnParamReader,
            PlanContext &planCtxt);

    virtual UDParser* prepare(ServerInterface &srvInterface,
            PerColumnParamReader &perColumnParamReader,
            PlanContext &planCtxt,
            const SizedColumnTypes &returnType);

    virtual void getParserReturnType(ServerInterface &srvInterface,
            PerColumnParamReader &perColumnParamReader,
            PlanContext &planCtxt,
            const SizedColumnTypes &argTypes,
            SizedColumnTypes &returnType);

    virtual void getParameterType(ServerInterface &srvInterface,
                                  SizedColumnTypes &parameterTypes);
};


/**
 * Field extractor from string using regex with capture targets.  Output a VMap
 */
class RegexExtractor : public ScalarFunction {
public:
    RegexExtractor(std::string pattern, 
                   bool useJit = true);

/// Delete any char*'s constructucted during initialize()
    ~RegexExtractor();


private:
    // Configurable parsing parameters
    // Set by the constructor
    std::string pattern;

    // PCRE bits
    int maxColumns;
    int *ovec;
    int options;
    pcre *pattern_regex;
    pcre_extra *pattern_extra;
    pcre *rt_regex;
    pcre_extra *rt_extra;
    std::map<int,std::string> captureToColMap;

    // 7.0/Crane map structure
    char* map_data_buf;
    ssize_t map_data_buf_len;
    char* pair_buf;

public:
    virtual void setup(ServerInterface &srvInterface, const SizedColumnTypes &argTypes);

    virtual void processBlock(ServerInterface &srvInterface,
                               BlockReader &arg_reader,
                               BlockWriter &result_writer);

private:
    /*
     * Simple heuristic for determining how much space is needed for a given VMap when using the FRegexExtractor.
     *
     * XXX TODO XXX ¿Better heuristic for this? Depends on how much of the record is matched and the length of the group names. ¿Re-try iff too small? XXX TODO XXX
     */
static inline size_t estimateNeededSpaceForMap(size_t inputRegexSize);
};


class RegexExtractorFactory : public ScalarFunctionFactory {
public:
    RegexExtractorFactory();

    virtual void getPrototype(ServerInterface &srvInterface, ColumnTypes &argTypes, ColumnTypes &returnType);

    virtual void getReturnType(ServerInterface &srvInterface,
                               const SizedColumnTypes &input_types,
                               SizedColumnTypes &output_types);
    virtual void getParameterType(ServerInterface &srvInterface,
                                  SizedColumnTypes &parameterTypes);

    virtual ScalarFunction * createScalarFunction(ServerInterface &srvInterface);

    virtual void getPerInstanceResources(ServerInterface &srvInterface, VResources &res) {
        // This function does not use file handles
        res.nFileHandles = 0;
    }
};


} /// END namespace flextable


/// END define REGEX_H_
#endif

