/**
 * Copyright (c) 2005 - 2015 Hewlett Packard Enterprise Development LP
 *
 * Creation Date: July 31, 2014
 */
/**
 * Flex Table Regex parser
 */

#include "FlexTable.h"
#include "Regex.h"

// 'pattern' parameter length
#define FREGEX_PATTERN_PARAM_LEN 65000

namespace flextable
{

/// BEGIN class FlexTableRegexParser
FlexTableRegexParser::FlexTableRegexParser(std::string pattern, 
                                           std::string recordTerminatorPattern, 
                                           std::string loglinecolumn,
                                           bool useJit)
    : currentRecordSize(0), pattern(pattern), recordTerminator(recordTerminatorPattern),
      loglinecolumn(loglinecolumn), loglinecol_index(-1),
      maxColumns(0), ovec(NULL), options(useJit ? PCRE_STUDY_JIT_COMPILE : 0), 
      pattern_regex(NULL), pattern_extra(NULL), rt_regex(NULL), rt_extra(NULL),
      map_col_index(-1), map_data_buf(NULL), map_data_buf_len(-1),
      pair_buf(NULL), is_flextable(false), srvInterface(NULL), aux_buf()
{
}

FlexTableRegexParser::~FlexTableRegexParser() {
    if (pattern_regex) pcre_free(pattern_regex);
    if (pattern_extra) pcre_free(pattern_extra);
    if (rt_regex) pcre_free(rt_regex);
    if (rt_extra) pcre_free(rt_extra);
    delete[] ovec;
    ovec = NULL;
}

bool FlexTableRegexParser::fetchNextRow() {
    // Amount of data we have to work with
    size_t reserved;

    // Amount of data that we've requested to work with.
    // Equal to `reserved` after calling reserve(), except in case of end-of-file.
    size_t reservationRequest = BASE_RESERVE_SIZE;

    // Pointer into the middle of our current data buffer.
    // Must always be betweeen getDataPtr() and getDataPtr() + reserved.
    char* ptr;

    // Our current position within the stream.
    // Kept around so that we can update ptr correctly after reserve()ing more data.
    size_t position = 0;

    //
    char rt = recordTerminator[0];

    do {
        // Get some (more) data
        reserved = cr.reserve(reservationRequest);

        // Position counter.  Not allowed to pass getDataPtr() + reserved.
        ptr = (char*)cr.getDataPtr() + position;

        // Keep reading until we hit EOF.
        // If we find the record terminator, we'll return out of the loop.
        // Very tight loop; very performance-sensitive.
        while ((position < reserved) && *ptr != rt) {
            ++ptr;
            ++position;
        }

        if ((position < reserved) && *ptr == rt) {
            currentRecordSize = position;
            return true;
        }

        reservationRequest *= 2;  // Request twice as much data next time
    } while (!cr.noMoreData());  // Stop if we run out of data;
                         // correctly handles files that aren't newline terminated
                         // and when we reach eof but haven't seeked there yet

    currentRecordSize = position;
    return false;
}

bool FlexTableRegexParser::handle_field(size_t colNum, char* start, size_t len, bool hasPadding)
{
    // Empty colums are null.
    if (len==0) {
        writer->setNull(colNum);
        return true;
    } else {
        NullTerminatedString str(start, len, false, hasPadding);
        return parseStringToType(str.ptr(), str.size(), colNum, colInfo.getColumnType(colNum), writer, sp);
    }
}

void FlexTableRegexParser::rejectRecord(const std::string &reason) {
    RejectedRecord rr(reason, (char*)cr.getDataPtr(), currentRecordSize, recordTerminator);
    crej.reject(rr);
}

void FlexTableRegexParser::run() {
    try {
    while (fetchNextRow()) {
        bool rejected = false;

        // Special case: ignore trailing newlines (record terminators) at
        // the end of files
        if (cr.isEof() && currentRecordSize == 0) {
            break;
        }

        char* record = (char*)cr.getDataPtr();
        int count = pcre_exec( pattern_regex, pattern_extra, record, currentRecordSize, 0, 0, 
                               ovec, (maxColumns+1)*3);
        if (count == -1) {
            std::string s(record,currentRecordSize);
            //log("No match: [%s]",s.c_str());
            cr.seek(currentRecordSize + 1);
            continue; // no match!
        }
        if (count < 0) {
            vt_report_error(1,"Error during match: [%d]",count);
        }

        if (is_flextable) {
            // Flex Table parsing
            VMapPairWriter map_writer(pair_buf, map_data_buf_len);

            // Parse each column into the map
            //log("Count: [%d]",count);
            for (int i=1; i <= maxColumns; i++) {
                char* base = record + ovec[i*2];
                size_t bound = ovec[i*2+1] - ovec[i*2];
                //log("i [%d] len [%d] start [%d] end [%d]",i,currentRecordSize,ovec[i*2],ovec[i*2+1]);
                if (bound <= 0) continue;
                map_writer.append(*srvInterface, captureToColMap[i].c_str(), captureToColMap[i].length(),
                                  false, base, bound);
            }

            VMapPairReader map_reader(map_writer);

            // Make sure our output column is clean (especially in the case of previously-rejected rows)
            for (size_t realColNum = 0; realColNum < writer->getNumCols(); realColNum++) {
                writer->setNull(realColNum);
            }

            // Capture dump-column data
            if (map_col_index != -1) {
                size_t total_offset=0;
                VMapBlockWriter::convert_vmap(getServerInterface(), map_reader,
                                         map_data_buf, map_data_buf_len, total_offset);
                VString& map_data_col = writer->getStringRef(map_col_index);
                map_data_col.copy(map_data_buf, total_offset);
            }

            // Now, capture any interesting columns
            const std::vector<VMapPair> &vpairs = map_reader.get_pairs();
            std::set<int> usedCols;
            for (size_t virtualColNum = 0; virtualColNum < vpairs.size(); virtualColNum++) {
                const size_t keyLen = vpairs[virtualColNum].key_length();
                aux_buf.allocate(keyLen + 1);
                normalize_key(vpairs[virtualColNum].key_str(), keyLen, aux_buf.loc);
                ImmutableStringPtr keyString(aux_buf.loc, keyLen);
                ColLkup::iterator it = real_col_lookup.find(keyString);

                if (it != real_col_lookup.end()) {
                    // Copy value into the auxiliary buffer so we can append
                    // a null terminator to it
                    const size_t valueLen = vpairs[virtualColNum].value_length();
                    aux_buf.allocate(valueLen + 1);
                    memcpy(aux_buf.loc, vpairs[virtualColNum].value_str(), valueLen);

                    handle_field(it->second, aux_buf.loc, valueLen, true);
                    usedCols.insert(it->second);
                }
            }

        } else {
            // Regular structured parsing.
            int numcols = colInfo.getColumnCount();
            for (int i=0; i < numcols; i++) {
                std::map<int,int>::iterator it = colIndexToCaptureMap.find(i);
                if (it != colIndexToCaptureMap.end()) {
                    int index = it->second;
                    char* base = record + ovec[index*2];
                    int bound = ovec[index*2+1] - ovec[index*2];
                    //log("i [%d] index [%d] len [%d] start [%d] end [%d]",i,index,currentRecordSize,ovec[i*2],ovec[i*2+1]);
                    if (bound <= 0) {
                        writer->setNull(i);
                    } else {
                        handle_field(i,base,bound,ovec[index*2+1] < ((int)currentRecordSize));
                    }
                } else {
                    writer->setNull(i);
                }
            }

        }

        if (loglinecol_index >= 0)
        {
            handle_field(loglinecol_index,record,currentRecordSize,false);
        }

        // Seek past the current record.
        // currentRecordSize points to the end of the record not counting the
        // record terminator.  But we want to seek over the record terminator too.
        cr.seek(currentRecordSize + 1);

        if (!rejected) {
            writer->next();
            recordsAcceptedInBatch++;
        }

    }
    } catch (std::exception &e) {
        log("std exception! [%s]",e.what());
        vt_report_error(1,"std::exception: [%s]",e.what());
    }
}

void FlexTableRegexParser::initialize(ServerInterface &srvInterface, SizedColumnTypes &returnType) {
    colInfo = returnType;
    aux_buf.initialize(&srvInterface, BASE_RESERVE_SIZE);
    this->srvInterface = &srvInterface;

    for (uint32 col = 0; col < colInfo.getColumnCount(); col++) {
        const std::string &str = colInfo.getColumnName(col);
        aux_buf.allocate(str.size() + 1);
        normalize_key(str.c_str(), str.size(), aux_buf.loc);
        real_col_lookup[ImmutableStringPtr(aux_buf.loc, str.size())] = col;
        aux_buf.loc += str.size() + 1;
    }

    // Find relevant columsn ( Flex Table __raw__ column, loglinecolumn)
    bool foundLogLineColumn = loglinecolumn.empty();
    for (uint32 col = 0; col < colInfo.getColumnCount(); col++) {
        if (map_col_index < 0 && (0 == strcmp(colInfo.getColumnName(col).c_str(), RAW_COLUMN_NAME)) && colInfo.getColumnType(col).isStringType()) {
            map_col_index = col;
            map_data_buf_len = colInfo.getColumnType(col).getStringLength();
            map_data_buf = (char*)srvInterface.allocator->alloc(map_data_buf_len);
        }
        if (!foundLogLineColumn && colInfo.getColumnName(col) == loglinecolumn) {
            foundLogLineColumn = true;
            loglinecol_index = col;
        }
    }
    if (!foundLogLineColumn) {
        vt_report_error(1, "Unable to find column for log line: [%s]",loglinecolumn.c_str());
    }

    if (map_data_buf_len > 0) {
        pair_buf = (char*)srvInterface.allocator->alloc(map_data_buf_len);
        is_flextable = true;
    }

    const char* error;
    int erroffset;
    pattern_regex = pcre_compile(pattern.c_str(), options, &error, &erroffset, NULL);
    if (!pattern_regex)
      vt_report_error(1, "Unable to compile pattern '[%s]': [%s]  [%d]",pattern.c_str(),error,erroffset);
    if (pattern_regex) {
        pattern_extra = pcre_study(pattern_regex, 0, &error);
        if (error != NULL)
          vt_report_error(1, "Unable to study pattern '[%s]': [%s]",pattern.c_str(),error);
    }
    int rc;
    // look up number of columns captured
    if ((rc = pcre_fullinfo(pattern_regex,pattern_extra,PCRE_INFO_CAPTURECOUNT,&maxColumns)) != 0)
      vt_report_error(1, "Unable to get capturecount");
    ovec = new int[3*(maxColumns+1)];

    int namecount;
    if ((rc = pcre_fullinfo(pattern_regex, pattern_extra, PCRE_INFO_NAMECOUNT, &namecount)) != 0)
      vt_report_error(1, "Unable to get namecount");

    if (maxColumns != namecount)
      vt_report_error(1, "Some capture patterns do not have names '[%s]' (patterns: [%d] names: [%d])",pattern.c_str(),maxColumns,namecount);

    int entrysize;
    if ((rc = pcre_fullinfo(pattern_regex, pattern_extra, PCRE_INFO_NAMEENTRYSIZE, &entrysize)) != 0)
      vt_report_error(1, "Unable to get pattern nameentrysize");

    unsigned char* nametable;
    if ((rc = pcre_fullinfo(pattern_regex, pattern_extra, PCRE_INFO_NAMETABLE, &nametable)) != 0)
      vt_report_error(1, "Unable to get pattern nametable");

    for (int i=0; i < namecount; i++) {
        unsigned char* entry = nametable + i * entrysize;
        char* key = (char*)(entry+2);
        int index = (entry[0] << 8) + entry[1];
        //log("Capture [%d] -> [%s]",index,key);
        if (is_flextable) {
            captureToColMap[index] = std::string(key);
        } else {
            const size_t keysize = strlen(key);
            aux_buf.allocate(keysize + 1);
            normalize_key(key, keysize, aux_buf.loc);
            ColLkup::iterator it = real_col_lookup.find(ImmutableStringPtr(aux_buf.loc, keysize));
            if (it == real_col_lookup.end())
                vt_report_error(1,"Capture [%s] does not match any column in target",key);
            colIndexToCaptureMap[it->second] = index;
        }
    }
}
/// END class FlexTableRegexParser


/// BEGIN class FRegexParserFactory
void FRegexParserFactory::plan(ServerInterface &srvInterface,
        PerColumnParamReader &perColumnParamReader,
        PlanContext &planCtxt) {
    /* Check parameters */
//        ParamReader args(srvInterface.getParamReader());   . . .

    /* Populate planData */
    // Nothing to do here
}

UDParser* FRegexParserFactory::prepare(ServerInterface &srvInterface,
        PerColumnParamReader &perColumnParamReader,
        PlanContext &planCtxt,
        const SizedColumnTypes &returnType)
{
    ParamReader args(srvInterface.getParamReader());

    // Defaults.
    std::string pattern(""), record_terminator("\n"), loglinecolumn("");
    bool use_jit(true);

    // Args.
    if (args.containsParameter("pattern"))
        pattern = args.getStringRef("pattern").str();
    if (args.containsParameter("record_terminator"))
        record_terminator = args.getStringRef("record_terminator").str();
    if (args.containsParameter("loglinecolumn"))
        loglinecolumn = args.getStringRef("loglinecolumn").str();
    if (args.containsParameter("use_jit"))
        use_jit = args.getBoolRef("use_jit");

    // Validate.
    if (record_terminator.size()!=1) {
        vt_report_error(1, "Invalid record_terminator [%s]: single character required",
                        record_terminator.c_str());
    }

    UDParser *p =  vt_createFuncObj(srvInterface.allocator,
                                    FlexTableRegexParser,
                                    pattern,
                                    record_terminator,
                                    loglinecolumn,
                                    use_jit);
    return p;
}

void FRegexParserFactory::getParserReturnType(ServerInterface &srvInterface,
        PerColumnParamReader &perColumnParamReader,
        PlanContext &planCtxt,
        const SizedColumnTypes &argTypes,
        SizedColumnTypes &returnType)
{
    returnType = argTypes;
}

void FRegexParserFactory::getParameterType(ServerInterface &srvInterface,
                              SizedColumnTypes &parameterTypes) {
    parameterTypes.addVarchar(FREGEX_PATTERN_PARAM_LEN, "pattern");
    parameterTypes.addVarchar(1, "record_terminator");
    parameterTypes.addVarchar(256, "loglinecolumn");
    parameterTypes.addBool("use_jit");
}
/// END class FRegexParserFactory



// Register the parser factory
RegisterFactory(FRegexParserFactory);


/// BEGIN class RegexExtractor
RegexExtractor::RegexExtractor(std::string pattern, 
                                     bool useJit)
    : pattern(pattern),
      maxColumns(0), ovec(NULL), options(useJit ? PCRE_STUDY_JIT_COMPILE : 0), 
      pattern_regex(NULL), pattern_extra(NULL), rt_regex(NULL), rt_extra(NULL),
      map_data_buf(NULL), map_data_buf_len(-1),
      pair_buf(NULL)
{
}

RegexExtractor::~RegexExtractor() {
    if (pattern_regex) pcre_free(pattern_regex);
    if (pattern_extra) pcre_free(pattern_extra);
    if (rt_regex) pcre_free(rt_regex);
    if (rt_extra) pcre_free(rt_extra);
    delete[] ovec;
    ovec = NULL;
}

void RegexExtractor::processBlock(ServerInterface &srvInterface,
                                  BlockReader &arg_reader,
                                  BlockWriter &result_writer)
{
    uint64 currRow=0;
    do {
        currRow++;
        const VString& logline = arg_reader.getStringRef(0);
        VString& outvmap = result_writer.getStringRef();
        if (logline.isNull()) {
            outvmap.setNull();
            result_writer.next();
            continue;
        }
        try {
            const char* record = logline.data();
            int count = pcre_exec( pattern_regex, pattern_extra, record, logline.length(), 0, 0, 
                                   ovec, (maxColumns+1)*3);
            if (count == -1) { // failed match
                outvmap.setNull();
            } else {
                if (count < 0) {
                    vt_report_error(1,"Error during match: [%d]",count);
                }

                // compute VMap
                VMapPairWriter map_writer(pair_buf, map_data_buf_len);

                // Parse each column into the map
                //log("Count: [%d]",count);
                for (int i=1; i <= maxColumns; i++) {
                    const char* base = record + ovec[i*2];
                    size_t bound = ovec[i*2+1] - ovec[i*2];
                    //log("i [%d] len [%d] start [%d] end [%d]",i,logline.length(),ovec[i*2],ovec[i*2+1]);
                    if (bound <= 0) continue;
                    map_writer.append(srvInterface, captureToColMap[i].c_str(), captureToColMap[i].length(),
                                      false, base, bound);
                }

                VMapPairReader map_reader(map_writer);

                // Convert to rest-format
                size_t total_offset=0;
                VMapBlockWriter::convert_vmap(srvInterface, map_reader,
                                             map_data_buf, map_data_buf_len, total_offset);
                outvmap.copy(map_data_buf, total_offset);
            }
        } catch(std::exception& e) {
            outvmap.setNull();
            LogDebugUDBasic("Exception while processing partition row: [%llu] on map regex extractor: [%s]", currRow, e.what());
        } catch(...) {
            outvmap.setNull();
            LogDebugUDBasic("Unknown exception while processing partition row: [%llu] on map regex extractor.", currRow);
        }

        result_writer.next();
    } while (arg_reader.next());
}

void RegexExtractor::setup(ServerInterface &srvInterface, const SizedColumnTypes &argTypes)
{
    // setup VMap buffers
    map_data_buf_len = estimateNeededSpaceForMap(argTypes.getColumnType(0).getStringLength());
    map_data_buf = (char*)srvInterface.allocator->alloc(map_data_buf_len);
    pair_buf = (char*)srvInterface.allocator->alloc(map_data_buf_len);

    // ini PCRE regex
    const char* error;
    int erroffset;
    pattern_regex = pcre_compile(pattern.c_str(), options, &error, &erroffset, NULL);
    if (!pattern_regex)
      vt_report_error(1, "Unable to compile pattern '[%s]': [%s]  [%d]",pattern.c_str(),error,erroffset);
    if (pattern_regex) {
        pattern_extra = pcre_study(pattern_regex, 0, &error);
        if (error != NULL)
          vt_report_error(1, "Unable to study pattern '[%s]': [%s]",pattern.c_str(),error);
    }
    int rc;
    // look up number of columns captured
    if ((rc = pcre_fullinfo(pattern_regex,pattern_extra,PCRE_INFO_CAPTURECOUNT,&maxColumns)) != 0)
      vt_report_error(1, "Unable to get capturecount");
    ovec = new int[3*(maxColumns+1)];

    int namecount;
    if ((rc = pcre_fullinfo(pattern_regex, pattern_extra, PCRE_INFO_NAMECOUNT, &namecount)) != 0)
      vt_report_error(1, "Unable to get namecount");

    if (maxColumns != namecount)
      vt_report_error(1, "Some capture patterns do not have names '[%s]' (patterns: [%d] names: [%d])",pattern.c_str(),maxColumns,namecount);

    int entrysize;
    if ((rc = pcre_fullinfo(pattern_regex, pattern_extra, PCRE_INFO_NAMEENTRYSIZE, &entrysize)) != 0)
      vt_report_error(1, "Unable to get pattern nameentrysize");

    unsigned char* nametable;
    if ((rc = pcre_fullinfo(pattern_regex, pattern_extra, PCRE_INFO_NAMETABLE, &nametable)) != 0)
      vt_report_error(1, "Unable to get pattern nametable");

    for (int i=0; i < namecount; i++) {
        unsigned char* entry = nametable + i * entrysize;
        char* key = (char*)(entry+2);
        int index = (entry[0] << 8) + entry[1];
        //log("Capture [%d] -> [%s]",index,key);
        captureToColMap[index] = std::string(key);
    }
}


inline size_t RegexExtractor::estimateNeededSpaceForMap(size_t inputRegexSize) {
    return 2.5 * ((double)inputRegexSize); // 2 is not enough for vertica.log cases
}
/// END class RegexExtractor


/// BEGIN class RegexExtractorFactory

RegexExtractorFactory::RegexExtractorFactory() {
    vol = IMMUTABLE;
}

void RegexExtractorFactory::getPrototype(ServerInterface &srvInterface,
                                         ColumnTypes &argTypes,
                                         ColumnTypes &returnType)
{
    argTypes.addLongVarchar();
    returnType.addLongVarbinary();
}

void RegexExtractorFactory::getReturnType(ServerInterface &srvInterface,
                                          const SizedColumnTypes &input_types,
                                          SizedColumnTypes &output_types)
{
    output_types.addLongVarbinary(VALUE_FOOTPRINT, MAP_VALUE_OUTPUT_NAME);
}

ScalarFunction * RegexExtractorFactory::createScalarFunction(ServerInterface &srvInterface)
{ 
    ParamReader args(srvInterface.getParamReader());

    // Defaults.
    std::string pattern("");
    bool use_jit(true);

    // Args.
    if (args.containsParameter("pattern"))
        pattern = args.getStringRef("pattern").str();
    if (args.containsParameter("use_jit"))
        use_jit = args.getBoolRef("use_jit");
    return vt_createFuncObj(srvInterface.allocator, RegexExtractor, pattern, use_jit);
}

void RegexExtractorFactory::getParameterType(ServerInterface &srvInterface,
                                             SizedColumnTypes &parameterTypes) {
    parameterTypes.addVarchar(FREGEX_PATTERN_PARAM_LEN, "pattern");
    parameterTypes.addBool("use_jit");
}
/// END class RegexExtractorFactory

RegisterFactory(RegexExtractorFactory);

} /// END namespace flextable


