/**
 * Copyright (c) 2005 - 2015 Hewlett Packard Enterprise Development LP
 *
 * Creation Date: December 10, 2013
 */
/**
 * Description: Top-level UDl plugin for json parser.
 *              Also uses JSONParseStack and JSONParseHandlers
 */


#ifndef JSON_PARSER_H_
#define JSON_PARSER_H_


#include "Vertica.h"
#include "StringParsers.h"

#include "JSONParseStack.h"
#include "JSONParseHandlers.h"
#include "FlexParsersSupport.h"


namespace flextable
{


/**
 * JSONParser
 *
 * Using JSONParseHandlers/JSONParseContext to parse json strings into the Flex Table map format.
 */
class JSONParser : public UDParser {
public:
    JSONParser(vbool flattenMaps = vbool_null,
               vbool flattenArrays = vbool_null,
               vbool omitEmptyKeys = vbool_null,
               std::string startPoint = "",
               int spo = 1,
               vbool rejectOnDuplicate = vbool_null,
               vbool rejectOnMaterializedTypeError=  vbool_null,
               vbool rejectOnEmptyKey = vbool_null,
               vbool alphanumKeys = vbool_null,
               std::string separator = ".",
	       vbool enforceLength = vbool_null,
               bool skipRecordAfterYajlError = false,
               char recordTerminator = 0);

    ~JSONParser();

private:
    // Keep a copy of the information about each column.
    // Note that Vertica doesn't let us safely keep a reference to
    // the internal copy of this data structure that it shows us.
    // But keeping a copy is fine.
    SizedColumnTypes colInfo;

    // An instance of the class containing the methods that we're
    // using to parse strings to the various relevant data types
    StringParsers sp;

    typedef std::map<ImmutableStringPtr, size_t> ColLkup;
    ColLkup real_col_lookup;

    // 7.0/ Crane map structure
    // The column number of the map data column (__raw__)
    int32 map_col_index;
    char* map_data_buf;
    size_t map_data_buf_len;
    char* tuple_buf;

    // Auxiliary buffer to normalize keys and store null-terminated values
    BufferWithAllocator<char> aux_buf;

    // JSON parsing context stack
    JSONParseContext* parse_context;
    vbool flatten_maps;
    vbool flatten_arrays;
    vbool omit_empty_keys;
    std::string start_point;
    int start_point_occurrence;
    vbool reject_on_duplicate;
    vbool reject_on_materialized_type_error;
    vbool reject_on_empty_key;
    vbool suppress_nonalphanumeric_key_chars;
    std::string separator;
    vbool enforcelength;
    bool reject_enforce_length;

    bool skipRecordAfterYajlError; // Skip JSON records Yajl cannot parse and
                                   // keep parsing from the next record?
                                   // When set to TRUE, it means recordTerminator is also set
    char recordTerminator; // Character to identify the next JSON record after parse error

    /**
     * Advance data until the next character after the record terminator,
     * or until the end of the buffer. It allows to skip bogus JSON records
     * It reduces the buffer lenght accordingly
     *
     * @return The number of skipped bytes
     */
    size_t skipCurrentRecord(const unsigned char *&data, size_t &len);

    void cleanup();

/// BEGIN Map Emitters
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

public:
/*
 * Generates map data and emits a row in the target table containing it including potentially applying to real columns.
 */
    static int handle_record(void* _this, VMapPairWriter &map_writer);
    int handle_record_impl(VMapPairWriter &map_writer);

/*
 * Generates map data and sets its location and length.
 */
    static int handle_nested_map(void* _this, VMapPairWriter &map_writer, const char*& results_buf_loc, size_t & results_buf_size);
    int handle_nested_map_impl(VMapPairWriter &map_writer, const char*& results_buf_loc, size_t & results_buf_size);

/*
 * Resets the parsing context for when the start_point is found to ignore an undesired JSON wrapper.
 */
    static int handle_start_point(void* _this, ServerInterface &srvInterface);
    int handle_start_point_impl(ServerInterface &srvInterface);

/*
 * Generates the Flex Table map type char* block.
 */
    size_t construct_map(ServerInterface &srvInterface, VMapPairReader map_reader);
/// END Map Emitters

/// BEGIN Data Processors
/// BEGIN UDParser Overrides
    virtual StreamState process(ServerInterface &srvInterface, DataBuffer &input, InputState input_state);

    virtual void setup(ServerInterface &srvInterface, SizedColumnTypes &returnType);
    int reset_parse_context(ServerInterface &srvInterface);

    virtual void destroy(ServerInterface &srvInterface, SizedColumnTypes &returnType);
/// END UDParser Overrides

private:
/*
 * Entry point for COPY commands when new data is ready to be parsed.
 */
    uint add_data(const unsigned char* data, size_t len, ServerInterface &srvInterface, yajl_status &status);
/// END Data Processors

};

// Helper class to free the string returned by yajl_get_error().
// Users must ensure that the passed yajl_handle pointer
// is valid for the lifetime of instances of this class.
class YajlFreeErrorWrapper {
private:
    YajlFreeErrorWrapper() : yajl_hand(NULL), str(NULL) { }

    yajl_handle yajl_hand;
    unsigned char *str;

public:
    // str must be returned by yajl_get_error()
    YajlFreeErrorWrapper(yajl_handle yajl_hand, unsigned char *str) :
        yajl_hand(yajl_hand), str(str) { }

    const char *getStr() {
        return (const char*) str;
    }

    ~YajlFreeErrorWrapper() { 
        yajl_free_error(yajl_hand, str);
    }
};

class FJSONParserFactory : public ParserFactory {
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

    UDChunker* prepareChunker(ServerInterface &srvInterface,
                              PerColumnParamReader &perColumnParamReader,
                              PlanContext &planCtxt,
                              const SizedColumnTypes &returnType);
};


/**
 * JSONExtractor
 *
 * Using JSONParseHandlers/JSONParseContext to parse json strings into the Flex Table map format.
 */
class JSONExtractor : public ScalarFunction {
public:
    JSONExtractor(vbool flattenMaps = vbool_null,
               vbool flattenArrays = vbool_null,
               vbool omitEmptyKeys = vbool_null,
               std::string startPoint = "",
               vbool rejectOnDuplicate = vbool_null,
               vbool rejectOnEmptyKey = vbool_null,
               std::string rejectAction = "Warn",
               std::string errorAction = "Abort",
	       vbool enforceLength = vbool_null);

    /*
     * Converts between a string and a FailAction enum value for parameter parsing.
     */
    static FailAction parse_failure_action(std::string failAction);

    ~JSONExtractor();

private:
    typedef std::map<ImmutableStringPtr, size_t> ColLkup;
    ColLkup real_col_lookup;

    // 7.0/Crane map structure
    // The column number of the map data column (__raw__)
    char* map_data_buf;
    size_t map_data_buf_len;
    char* tuple_buf;

    // JSON parsing context stack
    JSONParseContext* parse_context;
    vbool flatten_maps;
    vbool flatten_arrays;
    vbool omit_empty_keys;
    std::string start_point;
    vbool reject_on_duplicate;
    vbool reject_on_empty_key;
    FailAction reject_action;
    FailAction error_action;
    vbool enforcelength;

    char* warnings_buf; // XXX FUTURE XXX Bucket warnings rather than appending
    uint warnings_buf_len;

    void cleanup();

/// BEGIN Map Emitters
public:
/*
 * Generates map data and emits a row in the target table containing it including potentially applying to real columns.
 */
    static int handle_record(void* _this, VMapPairWriter &map_writer);
    int handle_record_impl(VMapPairWriter &map_writer);

/*
 * Generates map data and sets its location and length.
 */
    static int handle_nested_map(void* _this, VMapPairWriter &map_writer, const char*& results_buf_loc, size_t & results_buf_size);
    int handle_nested_map_impl(VMapPairWriter &map_writer, const char*& results_buf_loc, size_t & results_buf_size);

/*
 * Resets the parsing context for when the start_point is found to ignore an undesired JSON wrapper.
 */
    static int handle_start_point(void*_this, ServerInterface &srvInterface);
    int handle_start_point_impl(ServerInterface &srvInterface);

/*
 * Generates the Flex Table map type char* block.
 */
    size_t construct_map(ServerInterface &srvInterface, VMapPairReader map_reader);
/// END Map Emitters

/// BEGIN Data Processors
/// BEGIN ScalarFunction Overrides
    virtual void processBlock(ServerInterface &srvInterface,
                      BlockReader &argReader,
                      BlockWriter &resultWriter);

    virtual void setup(ServerInterface &srvInterface, const SizedColumnTypes &argTypes);
    int reset_parse_context(ServerInterface &srvInterface);

    virtual void destroy(ServerInterface &srvInterface, const SizedColumnTypes &returnType);
/// END ScalarFunction Overrides

private:
/*
 * Entry point for COPY commands when new data is ready to be parsed.
 */
    uint add_data(const unsigned char* data, size_t len, ServerInterface &srvInterface, yajl_status &status);
/// END Data Processors

    /*
     * Simple heuristic for determining how much space is needed for a given VMap when using the JSONExtractor.
     *
     * XXX TODO XXX ¿Better heuristic for this?. ¿Re-try iff too small? XXX TODO XXX
     */
static inline size_t estimateNeededSpaceForMap(size_t inputJSONSize, bool flattenMaps, bool flattenArrays);

};



class FJSONExtractorFactory : public ScalarFunctionFactory {
public:
    FJSONExtractorFactory();

    virtual void getPrototype(ServerInterface &srvInterface, ColumnTypes &argTypes, ColumnTypes &returnType);

    virtual void getReturnType(ServerInterface &srvInterface,
                               const SizedColumnTypes &inputTypes,
                               SizedColumnTypes &outputTypes);
    virtual void getParameterType(ServerInterface &srvInterface,
                                  SizedColumnTypes &parameterTypes);

    virtual ScalarFunction* createScalarFunction(ServerInterface &srvInterface);

    virtual void getPerInstanceResources(ServerInterface &srvInterface, VResources &res) {
        // This function does not use file handles
        res.nFileHandles = 0;
    }
};



} /// END namespace flextable

/// End define JSON_PARSER_H_
#endif
