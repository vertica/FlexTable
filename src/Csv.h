/**
 * Copyright (c) 2005 - 2015 Hewlett Packard Enterprise Development LP
 *
 * Creation Date: December 10, 2014
 */
/**
 * Flex Table Csv parser
 */


#ifndef CSV_H_
#define CSV_H_


#include "VMap.h"

#include "Vertica.h"
#include "ContinuousUDParser.h"
#include "StringParsers.h"
#include "FlexParsersSupport.h"

namespace flextable {

/**
 * Parser for Flex Table tables, with data files in CSV format.
 */
class FlexTableCsvParser : public ContinuousUDParser {
public:
    FlexTableCsvParser(char delimiter = ',',
                       char recordTerminator = '\n',
                       bool headerRow = true,
                       std::vector<std::string> formatStrings = std::vector<std::string>(),
                       bool shouldTrim = true,
                       bool omitEmptyKeys = false,
                       bool rejectOnDuplicate = false,
                       bool rejectOnMaterializedTypeError = false,
                       bool rejectOnEmptyKey = false,
                       char enclosed_by = '\"',
                       char escape = '\\',
                       std::string standardType = "rfc4180",
                       std::string NULLST = "NULL",
                       bool containsNULLparam = false);

    ~FlexTableCsvParser();
    //state to keep track if we are inquote (inside a "field") or outquote (outside)
    enum State { INQUOTE, OUTQUOTE };

    //when a field starts and ends with quotes we need to skip characters to store
    //the start index of the next field if any (e.g. "field1","field2") for field2 we want to
    //store the index of the 'f' character of "field2"
    static const size_t lastQuotedSkipSize = sizeof("\",\"");
    static const size_t lastNotQuotedSkipSize = sizeof(",\"");
    static const size_t lastQuotedActualNotQuotedSkipSize = sizeof("\",");
    static const size_t lastNotQuotedActualNotQuotedSkipSize = sizeof(",");

private:

    /// BEGIN data members
    enum State state;
    /* Keep a copy of the information about each column.
     * Note that Vertica doesn't let us safely keep a reference to
     * the internal copy of this data structure that it shows us.
     *  But keeping a copy is fine.
     */
    SizedColumnTypes colInfo;

    /*
     *  Size (in bytes) of the current record (row) that we're looking at.
     */
    size_t currentRecordSize;


    /// Configurable parsing parameters
    /// Set by the constructor
    char delimiter;
    char recordTerminator;
    char enclosed_by;
    char escape;
    std::string standardType;
    std::string NULLST;

    /*
     * Whether the first row contains column headers.
     */
    bool headerRow;
    bool containsNULLparam;

    //to store positions (start-end) of fields detected in a row
    std::vector<size_t> fieldsPosition;
    //to store positions of characters that need to be removed as they are escape
    std::vector<std::vector<size_t> > ignoreEscapedCharPosition;
    //store index of fields of a row that start and end with quotes
    //we need to identify them to make decisions on ignoring staring-ending quotes
    std::vector<size_t> fieldsStartingAndFinishWithQuotes;
    std::vector<bool> parsedRealCols;

    bool startedWithQuote;
    bool finishWithQuote;

    /*
     * An instance of the class containing the methods that we're
     * using to parse strings to the various relevant data types
     * VFormattedStringParsers sp;
     */
    StringParsers sp;

    /*
     * Real column name lookup
     */
    ColLkup real_col_lookup;

    // 7.0/Crane map structure
    // The column number of the map data column (__raw__) and buffers to hold
    // the final map and intermediate pairs
    int32 map_col_index;
    char* map_data_buf;
    ssize_t map_data_buf_len;
    char* pair_buf;

    bool is_flextable;

    std::vector<std::string> formatStrings;

    bool should_trim;
    bool omit_empty_keys;
    bool reject_on_duplicate;
    bool reject_on_materialized_type_error;
    bool reject_on_empty_key;

    // Auxiliary buffer to normalize keys and store null-terminated values
    BufferWithAllocator<char> aux_buf;

    // column headers
    typedef std::vector<std::string> DataColLookupMap;
    DataColLookupMap header_names;

    void storePreviousCharToEscape(size_t fieldsDetected, size_t position);

    /*
     * when previous char was an escae (e.g \), chech actual char to be e.g. 'n' so
     * we identify an escape sequence
     */
    int checkEscape(char * ptr, size_t fieldsDetected, size_t position );

    std::string removeEscapedChars(std::string field, size_t indexField);

    /**
     * Make sure (via reserve()) that the full upcoming row is in memory.
     * Assumes that getDataPtr() points at the start of the upcoming row.
     * (This is guaranteed by run(), prior to calling fetchNextRow().)
     *
     * Returns true if we stopped due to a record terminator;
     * false if we stopped due to EOF.
     */
    bool fetchNextRowRFC4180();
    bool fetchNextRowTraditional();

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

    virtual void deinitialize(ServerInterface &srvInterface, SizedColumnTypes &returnType);

    VTAllocator *getAllocator();

};

class FCsvParserFactory : public ParserFactory {
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
    //std::string checkTwoCharacterEscapeParam(std::string param);
};
} /// END namespace flextable


/// END define CSV_H_
#endif
