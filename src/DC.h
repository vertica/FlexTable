/**
 * Copyright (c) 2005 - 2015 Hewlett Packard Enterprise Development LP
 *
 * Creation Date: XXX TODO XXX Fill in when releasing this code
 */
/**
 * Flex Table DC parser
 */


#ifndef DC_H_
#define DC_H_


#include "VMap.h"

#include "Vertica.h"
#include "ContinuousUDParser.h"
#include "StringParsers.h"
#include "FlexParsersSupport.h"


namespace flextable
{


/**
 * Parser for Flex Tables, with data files in a simple dc format.
 */
class FlexTableDCParser : public ContinuousUDParser {
public:
    FlexTableDCParser(std::vector<std::string> formatStrings = std::vector<std::string>());
    
/// Delete any char*'s constructucted during initialize()
    ~FlexTableDCParser();


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

    std::string recordTerminator;

    // An instance of the class containing the methods that we're
    // using to parse strings to the various relevant data types
    VFormattedStringParsers sp;
    //StringParsers sp;

    ColLkup real_col_lookup;

    // 7.0/Crane map structure
    // The column number of the map data column (__raw__)
    int32 map_col_index;
    char* map_data_buf;
    ssize_t map_data_buf_len;
    char* pair_buf;

    bool is_flextable;

    std::vector<std::string> formatStrings;

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
    bool fetchNextColumn();

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

    /**
     * Reset the various per-column state; start over with a new row
     */
    void initCols();

    /**
     * Advance to the next column
     */
    void advanceCol();

    void rejectRecord(const std::string &reason);

public:
    virtual void run();

    virtual void initialize(ServerInterface &srvInterface, SizedColumnTypes &returnType);
};


class FDCParserFactory : public ParserFactory {
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


/// End define DC_H_
#endif

