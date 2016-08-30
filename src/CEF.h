/**
 * Copyright (c) 2005 - 2015 Hewlett Packard Enterprise Development LP
 *
 * Creation Date: March 3, 2014
 */
/**
 * Flex Table Cef parser
 */


#ifndef CEF_H_
#define CEF_H_

#define PREFIX_INDICATOR "CEF:"
#define PREFIX_FIELDS 7
#define VERSION_NUMBER_POSITION 4
#define PREFIX_DELIMITER '|'
#define DEFAULT_DELIMITER ' '
#define ESCAPE_CHARACTER '\\' 
#define PREFIX_COL1 "version"
#define PREFIX_COL2 "devicevendor"
#define PREFIX_COL3 "deviceproduct"
#define PREFIX_COL4 "deviceversion"
#define PREFIX_COL5 "signatureid"
#define PREFIX_COL6 "name"
#define PREFIX_COL7 "severity"


#include "VMap.h"

#include "Vertica.h"
#include "ContinuousUDParser.h"
#include "StringParsers.h"
#include "FlexParsersSupport.h"


namespace flextable
{

/**
 * Parser for Flex Tables, with data files in a simple cef format.
 */
class FlexTableCefParser : public ContinuousUDParser {
public:
    FlexTableCefParser(char delimiter = ' ',
                          char recordTerminator = '\n',
                          std::vector<std::string> formatStrings = std::vector<std::string>(),
                          bool shouldTrim = true,
                          bool rejectOnUnescapedDelimiter = false);

/// Delete any char*'s constructucted during initialize()
    ~FlexTableCefParser();

    static ImmutableStringPtr prefixColNames[];

private:
    // Keep a copy of the information about each column.
    // Note that Vertica doesn't let us safely keep a reference to
    // the internal copy of this data structure that it shows us.
    // But keeping a copy is fine.
    SizedColumnTypes colInfo;

    // Size (in bytes) of the current record (row) that we're looking at.
    size_t currentRecordSize;

    // Start-position and size of the current column, within the current row,
    // relative to getDataPtr().
    // We read in each row one row at a time,
    size_t currentColPosition;
    size_t currentColSize;

    // Configurable parsing parameters
    // Set by the constructor
    char delimiter;
    char recordTerminator;

    // An instance of the class containing the methods that we're
    // using to parse strings to the various relevant data types
    StringParsers sp;

    // Used to match keys to column names when data is written to Vertica
    ColLkup real_col_lookup;

    // 7.0/Crane map structure
    // The column number of the map data column (__raw__)
    int32 map_col_index;
    char* map_data_buf;
    ssize_t map_data_buf_len;
    char* pair_buf;
    char* tmp_escape_buf; // in between buffer for escaped characters
    size_t tmp_escape_buf_len; // sizeof(tmp_escape_buf)

    bool is_flextable;
    bool has_escape_character; // Backslash detected

    std::vector<std::string> formatStrings;

    bool should_trim;

    // Count the number of prefix fields we are parsing
    size_t counter;

    bool reject_on_unescaped_delimiter;
    bool hasPrefix;

    bool rejected;

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
     * Fetch the next column.
     * Returns false if we stopped due to hitting the record terminator;
     * true if we stopped due to hitting a column delimiter.
     * Should depend on (and/or set) the values:
     *
     * - currentColPosition -- The number of bytes from getDataPtr() to
     *   the start of the current column field.  Should not be set.
     *
     * - currentColSize -- Should be set to the distance from the start
     *   of the column to the last non-record-terminator character
     */
    bool fetchNextColumn(char _delimiter);

    /**
     * Performs special error checking on the first character of the
     * next column
     *
     * Calls fetchNextColumn()
     */
    bool fetchNextPrefixColumn(char _delimiter);

    /**
     * Spaces can delimit the file and they do not have to be escaped
     * This method searches for more spaces after one is found
     *
     * The basic logic is to find two spaces then search for an equals
     * sign in the string that seperates them.  If one exists and is not
     * escaped then the first location of the spaces delimits the last value
     * from the next key
     */
    void delimiterLookAhead(char* &pos);

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
    bool handleField(size_t colNum, char* start, size_t len, bool hasPadding = false);

    /**
     * If a character is escaped (by a backslash) then we need to remove the 
     * backslash before we write the data into a VMap pair
     */
    void removeBackslashes(char* &value, size_t &valbound);


    /**
     * Reset the various per-column state; start over with a new row
     */
    void initCols();

    /**
     * Advance to the next column
     */
    void advanceCol();

    void rejectRecord(const std::string &reason);

    bool rejectMessage();

public:
    virtual void run();

    virtual void initialize(ServerInterface &srvInterface, SizedColumnTypes &returnType);
};


class FCefParserFactory : public ParserFactory {
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


} /// End namespace flextable


/// End define CEF_H_
#endif

