/**
 * Copyright (c) 2005 - 2015 Hewlett Packard Enterprise Development LP
 *
 * Creation Date: December 10, 2013
 */
/**
 * Flex Table Delimited parser
 */


#ifndef DELIMITED_H_
#define DELIMITED_H_


#include "VMap.h"

#include "Vertica.h"
#include "ContinuousUDParser.h"
#include "FlexStringParsers.h"
#include "FlexParsersSupport.h"


namespace flextable
{


/**
 * Parser for Flex Table tables, with data files in simple delimited format.
 */
class FlexTableDelimitedParser : public ContinuousUDParser {
public:
    FlexTableDelimitedParser(char delimiter = '|',
                             char recordTerminator = '\n',
                             bool headerRow = true,
                             std::vector<std::string> formatStrings = std::vector<std::string>(),
                             bool shouldTrim = true,
                             bool omitEmptyKeys = false,
                             bool rejectOnDuplicate = false,
                             bool rejectOnMaterializedTypeError = false,
                             bool rejectOnEmptyKey = false,
                             bool treatEmptyValAsNull = true,
                             vbool enforceLength = vbool_null);

/// Delete any char*'s constructucted during initialize()
    ~FlexTableDelimitedParser();


private:
    /// BEGIN data members
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

    /*
     * Start-position of the current column, within the current row, relative to getDataPtr().
     * We read in each row one row at a time,
     */
    size_t currentColPosition;
    /*
     * Size of the current column, within the current row, relative to getDataPtr().
     * We read in each row one row at a time,
     */
    size_t currentColSize;

    /// Configurable parsing parameters
    /// Set by the constructor
    char delimiter;
    char recordTerminator;

    /*
     * Whether the first row contains column headers.
     */
    bool headerRow;

    /*
     * An instance of the class containing the methods that we're
     * using to parse strings to the various relevant data types
     * VFormattedStringParsers sp;
     */
    FlexStringParsers sp;

    /*
     * Real column name lookup
     */
    ColLkup real_col_lookup;

    // 7.0/Crane map structure
    // The column number of the map data column (__raw__) and buffers to hold
    // the final map and intermediate pairs
    int32 map_col_index;
    ssize_t pair_buf_len;
    char* pair_buf;

    bool is_flextable;

    std::vector<std::string> formatStrings;

    bool should_trim;
    bool omit_empty_keys;
    bool treat_empty_val_as_null;
    bool reject_on_duplicate;
    bool reject_on_materialized_type_error;
    bool reject_on_empty_key;

    // Auxiliary buffer to normalize keys and store null-terminated values
    BufferWithAllocator<char> aux_buf;

    // column headers
    typedef std::vector<std::string> DataColLookupMap;
    DataColLookupMap header_names;

    vbool enforcelength;
    /// END data members


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

    virtual void deinitialize(ServerInterface &srvInterface, SizedColumnTypes &returnType);
};


class FDelimitedParserFactory : public ParserFactory {
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
 * Field extractor from strings in simple delimited format.
 */
class FDelimitedExtractor : public ScalarFunction {
public:
    FDelimitedExtractor(char delimiter = '|',
                        bool shouldTrim = true,
                        std::string headerNamesParam = "",
                        bool treatEmptyValAsNull = true,
                        vbool enforceLength = vbool_null);

/// Delete any char*'s constructucted during initialize()
    ~FDelimitedExtractor();


private:
    /// BEGIN data members
    /*
     * Start-position of the current column, within the current row, relative to getDataPtr().
     * We read in each row one row at a time,
     */
    size_t currentColPosition;
    /*
     * Size of the current column, within the current row, relative to getDataPtr().
     * We read in each row one row at a time,
     */
    size_t currentColSize;

    /// Configurable parsing parameters
    /// Set by the constructor
    char delimiter;
    std::string header_names_param;

    // 7.0/Crane map structure
    char* map_data_buf;
    ssize_t map_data_buf_len;
    char* pair_buf;

    bool should_trim;
    bool treat_empty_val_as_null;
    vbool enforcelength;

    // column headers
    typedef std::vector<std::string> DataColLookupMap;
    DataColLookupMap header_names;
    /// END data members


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
    bool fetchNextColumn(const VString& inputRecord);
    bool fetchNextColumn(std::string inputRecord);
    bool fetchNextColumn(const char* pos, size_t len);

    /**
     * Advance to the next column
     */
    void advanceCol();

    void rejectRecord(const std::string &reason);

public:
    virtual void setup(ServerInterface &srvInterface, const SizedColumnTypes &argTypes);

    virtual void processBlock(ServerInterface &srvInterface,
                               BlockReader &arg_reader,
                               BlockWriter &result_writer);

private:
    /*
     * Simple heuristic for determining how much space is needed for a given VMap when using the FDelimitedExtractor.
     *
     * XXX TODO XXX ¿Better heuristic for this? Depends on the length of the fields as compared to their column headers. ¿Re-try iff too small? XXX TODO XXX
     */
static inline size_t estimateNeededSpaceForMap(size_t inputDelimSize, std::string headerNames="");
};



class FDelimitedExtractorFactory : public ScalarFunctionFactory {
public:
    FDelimitedExtractorFactory();

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


/// END define DELIMITED_H_
#endif

