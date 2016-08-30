/**
 * Copyright (c) 2005 - 2015, Hewlett-Packard Development Co., L.P.
 */
/**
 * Flex Table IDX parser, for IDOL IDX files and extracts
 */

#ifndef FLEXTABLE_IDX_H_
#define FLEXTABLE_IDX_H_

#include "VMap.h"

#include "Vertica.h"
#include "ContinuousUDParser.h"
#include "StringParsers.h"

namespace flextable
{
/**
 * Parser for Flex Tables, with data files in a simple key0=value0,key1=value1,...,keyN=valueN format.
 */
class FlexTableIDXParser : public ContinuousUDParser {
public:
    FlexTableIDXParser(bool storeContent);

/// Delete any char*'s constructucted during initialize()
    ~FlexTableIDXParser();

private:
    // Keep a copy of the information about each column.
    // Note that Vertica doesn't let us safely keep a reference to
    // the internal copy of this data structure that it shows us.
    // But keeping a copy is fine.
    SizedColumnTypes colInfo;

    // Offset of start and end of #DRE tag.
    ssize_t dre_start, dre_end;
    ssize_t key_start, key_end;
    ssize_t val_start, val_end;
    ssize_t entry_end;

#if DUNNO
    // An instance of the class containing the methods that we're
    // using to parse strings to the various relevant data types
    std::vector<std::string> formatStrings;
#endif
    StringParsers sp;

    ColLkup real_col_lookup;

    // 7.0/Crane map structure
    // The column number of the map data column (__raw__)
    int32 map_col_index;
    char* map_data_buf;
    ssize_t map_data_buf_len;
    char* pair_buf;
    VMapPairWriter *map_writer;

    bool is_flextable;

    // TODO: We would like to be able to suppress and rename fields within the parse
    bool storeContent;

    /**
     * Make sure that the next #DRE token is in memory
     *   getDataPtr() will lead it
     * Returns false if we are at EOF
     */
    bool fetchNextDRETag();

    /**
     * Make sure (via reserve()) that the full upcoming #DRE lineset is in memory.
     * Assumes that getDataPtr() points at the start of the upcoming row.
     * (This is guaranteed by run(), prior to calling fetchNextRow().)
     *
     * Returns true if we stopped due to a record terminator;
     * false if we stopped due to EOF.
     */
    bool fetchThisDRE(bool quoted);

    /**
     * Skip over current #DRE tag, to the next one.  Discards the value
     *   (useful for DRECONTENT, say)
     * Returns false if we stopped due to EOF
     */
    bool skipThisDRE(bool quoted);

    /**
     * Within the DRE, set the key value pair pointers
     */
    bool parseThisDRE(bool fieldname, bool quoted, bool multiline);

    /**
     * Go onto the next DRE
     */
    void doneWithDRE();

    /**
     * Write a field into whatever structures are appropriate
     *  Parses
     *  Puts into map (if flex table) and into real columns
     */
    bool writeOutField(size_t ks, size_t ke, size_t vs, size_t ve);

    /**
     * There are few criteria for DRE rejection.  Very few.
     *   But still, a facility seems sensible.
     */
    void rejectRecord(const std::string &reason);

    bool isProcessingRecord;
    void startNewRecord();
    void endRecord();

public:
    virtual void run();

    virtual void initialize(ServerInterface &srvInterface, SizedColumnTypes &returnType);
};


class FIDXParserFactory : public ParserFactory {
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


/// End header guard
#endif

