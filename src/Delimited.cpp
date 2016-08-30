/**
 * Copyright (c) 2005 - 2015 Hewlett Packard Enterprise Development LP
 *
 * Creation Date: December 10, 2013
 */
/**
 * Flex Table Delimited parser
 */

#include "FlexTable.h"
#include "Delimited.h"




namespace flextable
{


/// BEGIN class FlexTableDelimitedParser
FlexTableDelimitedParser::FlexTableDelimitedParser(
                            char delimiter,
                            char recordTerminator,
                            bool headerRow,
                            std::vector<std::string> formatStrings,
                            bool shouldTrim,
                            bool omitEmptyKeys,
                            bool rejectOnDuplicate,
                            bool rejectOnMaterializedTypeError,
                            bool rejectOnEmptyKey,
                            bool treatEmptyValAsNull,
		       				vbool enforceLength)
    : currentRecordSize(0), delimiter(delimiter), recordTerminator(recordTerminator),
      headerRow(headerRow), map_col_index(-1), pair_buf_len(-1), pair_buf(NULL),
      is_flextable(false), formatStrings(formatStrings), should_trim(shouldTrim), omit_empty_keys(omitEmptyKeys),
      treat_empty_val_as_null(treatEmptyValAsNull),
      reject_on_duplicate(rejectOnDuplicate), reject_on_materialized_type_error(rejectOnMaterializedTypeError),
      reject_on_empty_key(rejectOnEmptyKey), aux_buf(), header_names(),enforcelength(enforceLength) {}

FlexTableDelimitedParser::~FlexTableDelimitedParser() {}

bool FlexTableDelimitedParser::fetchNextRow() {
    // Amount of data we have to work with
    size_t reserved;

    // Amount of data that we've requested to work with.
    // Equal to `reserved` after calling reserve(), except in case of end-of-file.
    size_t reservationRequest = BASE_RESERVE_SIZE;

    // Pointer into the middle of our current data buffer.
    // Must always be betweeen getDataPtr() and getDataPtr() + reserved.
    char* ptr;

    // Our current pos??ition within the stream.
    // Kept around so that we can update ptr correctly after reserve()ing more data.
    size_t position = 0;

    do {
        // Get some (more) data
        reserved = cr.reserve(reservationRequest);

        // Position counter.  Not allowed to pass getDataPtr() + reserved.
        ptr = (char*)cr.getDataPtr() + position;

        // Keep reading until we hit EOF.
        // If we find the record terminator, we'll return out of the loop.
        // Very tight loop; very performance-sensitive.
        while (position < reserved && *ptr != recordTerminator) {
            ++ptr;
            ++position;
        }

        if ((position < reserved) && *ptr == recordTerminator) {
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


bool FlexTableDelimitedParser::FlexTableDelimitedParser::fetchNextColumn() {
    // fetchNextRow() has guaranteed that we can read until the next
    // delimiter or the record terminator, whichever comes first.
    // So this can be a very tight loop:
    // Just scan forward until we hit one of the two.
    char* pos = (char*)cr.getDataPtr() + currentColPosition;
    currentColSize = 0;

    while ((currentColSize + currentColPosition) < currentRecordSize &&
           *pos != delimiter && *pos != recordTerminator) {
        ++pos;
        ++currentColSize;
    }
    return (((currentColSize + currentColPosition) < currentRecordSize) && *pos == delimiter);
}

bool FlexTableDelimitedParser::handle_field(size_t colNum, char* start, size_t len, bool hasPadding) {
	const Vertica::VerticaType &destColType = colInfo.getColumnType(colNum);
    ssize_t maxDataLen = len;
    if (destColType.isStringType()) {
	    ssize_t destColLength = (ssize_t)destColType.getStringLength();
	    if(enforcelength && (destColLength >= 0 && len > (size_t)destColLength)){
            return false;
        }
        maxDataLen = std::min(destColLength, maxDataLen);
    }
    NullTerminatedString str(start, maxDataLen, false, hasPadding);
    try {
        return parseStringToType(str.ptr(), str.size(), colNum, colInfo.getColumnType(colNum), writer, sp);
    } catch(std::exception& e) {
          log("Exception while processing partition: [%s] while type converting Delimited materialized column #[%lu], skipping this materialized column for this row.", e.what(), colNum);
    } catch (...) {
        log("Unknown exception while type converting Delimited materialized column #[%lu], skipping this materialized column for for this row.", colNum);
    }

    // on error, set the column to null and return false.
    writer->setNull(colNum);
    return false;
}

void FlexTableDelimitedParser::initCols() {
    currentColPosition = 0;
    currentColSize = 0;
}

void FlexTableDelimitedParser::advanceCol() {
    currentColPosition += currentColSize + 1;
}

void FlexTableDelimitedParser::rejectRecord(const std::string &reason) {
    RejectedRecord rr(reason, (char*)cr.getDataPtr(), currentRecordSize,
                      std::string(1, recordTerminator));
    crej.reject(rr);
}

void FlexTableDelimitedParser::run() {
    bool processedHeader = !headerRow;

    uint64 rowNum = 0;
    std::vector<bool> emptyCols(0, false);

    const size_t colInfoColumnCount = colInfo.getColumnCount();
    // Did we successfully parse real columns?
    bool* parsedRealCols = vt_allocArray(getServerInterface().allocator,
                                         bool, colInfoColumnCount);

    // Index mapping between header names (the keys) and real columns in colInfo
    // There may not necessarily be the same number of header names and real columns    
    // An index '-1' means the column values do not materialize to any real column
    std::vector<int> headerNamesToRealCols;

    while (fetchNextRow()) {
        rowNum++;
        bool rejected = false;

        // Special case: ignore trailing newlines (record terminators) at
        // the end of files
        if (cr.isEof() && currentRecordSize == 0) {
            break;
        }

        initCols();

        // Process header
        if (headerRow && (!processedHeader)) {
            uint32_t colNum = 0;
            uint32_t sourceColNum = 0;
            bool done = false;
            std::set<std::string, VMapBlockWriter::StringInsensitiveCompare> caseInsensitiveSet =
                    std::set<std::string, VMapBlockWriter::StringInsensitiveCompare>();
            while (!done) {
                done = !fetchNextColumn();
                std::string headCol = std::string((char*)cr.getDataPtr() + currentColPosition, currentColSize);
                header_names.push_back((should_trim) ? trim(headCol) : headCol);
#if defined (DEBUG)
                Assert((colNum + 1) == header_names.size());
#endif

                if (omit_empty_keys) emptyCols.push_back(false);
                if ((reject_on_empty_key || omit_empty_keys) &&
                        (header_names[colNum].length() <= 0 ||
                         ((!reject_on_duplicate) && trim(header_names[colNum]).length() == 0))) {
                    if (reject_on_empty_key) {
                        rejected = true;
                        std::stringstream rejectMessage;
                        rejectMessage << "Processed a header row with an empty key in column: [" << colNum << "] with reject_on_empty_key specified; rejecting.";
                        LogDebugUDWarn("%s", rejectMessage.str().c_str());
                        rejectRecord(rejectMessage.str());
                        break;
                    }

                    // omit_empty_keys
                    LogDebugUDWarn("Processed a header row with an empty key in column: [%d] with omit_empty_keys specified; skipping column.", sourceColNum);
                    emptyCols[sourceColNum] = true;
                    header_names.pop_back();
                    advanceCol();
                    sourceColNum++;
                    continue;
                }
                if (reject_on_duplicate) {
                    std::pair<std::set<std::string>::iterator, bool> insertResult =
                            caseInsensitiveSet.insert(header_names[colNum]);
                    if (!insertResult.second) {
                        rejected = true;
                        std::stringstream rejectMessage;
                        rejectMessage << "Processed a header row with duplicate keys with reject_on_duplicate specified; rejecting.";
                        LogDebugUDWarn("%s", rejectMessage.str().c_str());
                        rejectRecord(rejectMessage.str());
                        break;
                    }
                }
                advanceCol();
                sourceColNum++;
                colNum++;
            }
            cr.seek(currentRecordSize + 1);
            processedHeader = true;
            if (rejected) {
                break;
            }

            // Get index mapping of header names (the keys) to real columns
            for (size_t i = 0; i < header_names.size(); ++i) {
                // Check if the normalized header name matches with any real column
                const std::string &headerName = header_names[i];
                const size_t headerLen = headerName.length();
                aux_buf.allocate(headerLen + 1);
                normalize_key(headerName.c_str(), headerLen, aux_buf.loc);
                ImmutableStringPtr keyString(aux_buf.loc, headerLen);
                ColLkup::iterator it = real_col_lookup.find(keyString);
                int colIdx = -1;
                if (it != real_col_lookup.end()) {
                    colIdx = it->second;
                }
                headerNamesToRealCols.push_back(colIdx);
            }

            LogDebugUDBasic("Processed a header row with %zu columns.", header_names.size());
            continue;
        }

        memset(parsedRealCols, 0, colInfoColumnCount);

        // FlexTable parsing
        VMapPairWriter map_writer(pair_buf, pair_buf_len);

        // Parse each column into the map
        uint32_t colNum = 0;
        uint32_t sourceColNum = 0;
        bool done = false;
        while (!done) {
            done = !fetchNextColumn();

            if (omit_empty_keys && emptyCols[sourceColNum]) {
                sourceColNum++;
                advanceCol();
                continue;
            }

            if (header_names.size() <= colNum) {
                // make up a column name
                std::ostringstream oss;
                oss << "ucol" << colNum;
                header_names.push_back(oss.str());
#if defined (DEBUG)
                Assert((colNum + 1) == header_names.size());
#endif
            }
            const std::string &headerName = header_names[colNum];

            char* base = (char*)cr.getDataPtr() + currentColPosition;
            size_t bound = currentColSize;
            if (should_trim) {
                trim_buf_ends(&base, &bound);
            }

            bool isNull = (bound == 0 && treat_empty_val_as_null);
            if (is_flextable && !map_writer.append(getServerInterface(), headerName.c_str(), headerName.length(), isNull, base, bound)) {
                rejected = true;
                std::stringstream rejectMessage;
                rejectMessage << "Error:  Row [" << rowNum << "] failed to append value for column: [" << std::string(headerName.c_str(), headerName.length()) << "]";
                rejectMessage << " with value: [" << std::string(base, bound) << "].";
                LogDebugUDBasic("%s", rejectMessage.str().c_str());
                rejectRecord(rejectMessage.str());
                break;
            }

            // Materialize column if necessary
            if (colNum < headerNamesToRealCols.size() &&
                headerNamesToRealCols[colNum] >= 0) {
                const int &realColIdx = headerNamesToRealCols[colNum];
                if (isNull) {
                    writer->setNull(realColIdx);
                    parsedRealCols[realColIdx] = true;
                } else {
                    // Copy value into the auxiliary buffer so we can append
                    // a null terminator to it
                    aux_buf.allocate(bound + 1);
                    memcpy(aux_buf.loc, base, bound);

                    if (handle_field(realColIdx, aux_buf.loc, bound, true)) {
                        parsedRealCols[realColIdx] = true;
                    } else if (reject_on_materialized_type_error) {
                        rejected = true;
                        std::stringstream rejectMessage;
                        rejectMessage << "Row [" << rowNum << "] rejected due to materialized type error on column: [" << headerName.c_str() << "]";
                        rejectMessage << " with value: [" << std::string(base, bound) << "].";
                        LogDebugUDBasic("%s", rejectMessage.str().c_str());
                        rejectRecord(rejectMessage.str());
                        break;
                    }
                }
            }

            advanceCol();
            colNum++;
            sourceColNum++;
        } // while (!done)
        if (rejected) {
            cr.seek(currentRecordSize + 1);
            continue;
        }

        // Capture dump-column data
        if (map_col_index != -1) {
            VMapPairReader map_reader(map_writer);
            size_t total_offset = 0;
            VString& map_data_col = writer->getStringRef(map_col_index);
            if (VMapBlockWriter::convert_vmap(getServerInterface(), map_reader,
                                              map_data_col.data(), pair_buf_len, total_offset)) {
                // Output VMap was safely written. Update string length
                EE::setLenSV(map_data_col.sv, total_offset);
            } else {
                // Target VMap output buffer is too small to store the result. Reject this record
                rejected = true;
                std::stringstream rejectMessage;
                rejectMessage << "Error:  Row [" << rowNum << "] failed to write VMap value for column: [" << writer->getTypeMetaData().getColumnName(map_col_index) << "]";
                log("%s", rejectMessage.str().c_str());
                rejectRecord(rejectMessage.str());
            }
        }

        if (rejected) {
            cr.seek(currentRecordSize + 1);
            continue;
        }

        // Make sure our output column is clean (especially in the case of previously-rejected rows)
        for (size_t realColNum = 0; realColNum < writer->getNumCols(); realColNum++) {
            if (!(parsedRealCols[realColNum] || (0 == strcmp(colInfo.getColumnName(realColNum).c_str(), RAW_COLUMN_NAME))))
                writer->setNull(realColNum);
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

}

void FlexTableDelimitedParser::initialize(ServerInterface &srvInterface, SizedColumnTypes &returnType) {
    colInfo = returnType;
    aux_buf.initialize(&srvInterface, BASE_RESERVE_SIZE);

    if (formatStrings.size() != returnType.getColumnCount()) {
        formatStrings.resize(returnType.getColumnCount(), "");
    }
    //sp.setFormats(formatStrings);

    for (uint32 colNum = 0; colNum < colInfo.getColumnCount(); colNum++) {
        const std::string &str = colInfo.getColumnName(colNum);
        aux_buf.allocate(str.size() + 1);
        normalize_key(str.c_str(), str.size(), aux_buf.loc);
        real_col_lookup[ImmutableStringPtr(aux_buf.loc, str.size())] = colNum;
        aux_buf.loc += str.size() + 1;
    }

    // Find the flextable __raw__ column
    for (uint32 colNum = 0; colNum < colInfo.getColumnCount(); colNum++) {
        if ((0 == strcmp(colInfo.getColumnName(colNum).c_str(), RAW_COLUMN_NAME)) && colInfo.getColumnType(colNum).isStringType()) {
            map_col_index = colNum;
            pair_buf_len = colInfo.getColumnType(colNum).getStringLength();
            break;
        }
    }

    if (pair_buf_len > 0) {
        pair_buf = (char*)srvInterface.allocator->alloc(pair_buf_len);
        is_flextable = true;
    }
}

void FlexTableDelimitedParser::deinitialize(ServerInterface &srvInterface, SizedColumnTypes &returnType) { }
/// END class FlexTableDelimitedParser


/// BEGIN class FDelimitedParserFactory
void FDelimitedParserFactory::plan(ServerInterface &srvInterface,
        PerColumnParamReader &perColumnParamReader,
        PlanContext &planCtxt) { }

UDParser* FDelimitedParserFactory::prepare(ServerInterface &srvInterface,
        PerColumnParamReader &perColumnParamReader,
        PlanContext &planCtxt,
        const SizedColumnTypes &returnType)
{
    // Defaults
    std::string delimiter("|"), record_terminator("\n");
    std::vector<std::string> formatStrings;
    bool headerRow(true);
    bool omitEmptyKeys(false);
    bool shouldTrim(true);
    bool rejectOnDuplicate(false);
    bool rejectOnMaterializedTypeError(false);
    bool rejectOnEmptyKey(false);
    bool treatEmptyValAsNull(true);

    // Args
    ParamReader args(srvInterface.getParamReader());
    if (args.containsParameter("delimiter"))
        delimiter = args.getStringRef("delimiter").str();
    if (args.containsParameter("record_terminator"))
        record_terminator = args.getStringRef("record_terminator").str();
    if (args.containsParameter("header"))
        headerRow = args.getBoolRef("header");
    if (args.containsParameter("trim"))
        shouldTrim = args.getBoolRef("trim");
    if (args.containsParameter("omit_empty_keys"))
        omitEmptyKeys = args.getBoolRef("omit_empty_keys");
    if (args.containsParameter("treat_empty_val_as_null"))
        treatEmptyValAsNull = args.getBoolRef("treat_empty_val_as_null");
    if (args.containsParameter("reject_on_duplicate"))
        rejectOnDuplicate = args.getBoolRef("reject_on_duplicate");
    if (args.containsParameter("reject_on_materialized_type_error"))
        rejectOnMaterializedTypeError = args.getBoolRef("reject_on_materialized_type_error");
    if (args.containsParameter("reject_on_empty_key"))
        rejectOnEmptyKey = args.getBoolRef("reject_on_empty_key");

    // Validate
    if (delimiter.size()!=1) {
        vt_report_error(0, "Invalid delimiter [%s]: single character required",
                        delimiter.c_str());
    }
    if (record_terminator.size()!=1) {
        vt_report_error(1, "Invalid record_terminator [%s]: single character required",
                        record_terminator.c_str());
    }

    vbool enforcelength = vbool_null;
    if (args.containsParameter("enforce_length")) {
        enforcelength = args.getBoolRef("enforce_length");
    }
    if (enforcelength == vbool_null) {
        enforcelength = vbool_false;
    }

    // Extract the "format" argument.
    // Default to the global setting, but let any per-column settings override for that column.
    if (args.containsParameter("format"))
        formatStrings.resize(returnType.getColumnCount(), args.getStringRef("format").str());
    else
        formatStrings.resize(returnType.getColumnCount(), "");

    for (size_t i = 0; i < returnType.getColumnCount(); i++) {
        const std::string &cname(returnType.getColumnName(i));
        if (perColumnParamReader.containsColumn(cname)) {
            ParamReader &colArgs = perColumnParamReader.getColumnParamReader(cname);
            if (colArgs.containsParameter("format")) {
                formatStrings[i] = colArgs.getStringRef("format").str();
            }
        }
    }

    return vt_createFuncObj(srvInterface.allocator,
                            FlexTableDelimitedParser,
                            delimiter[0],
                            record_terminator[0],
                            headerRow,
                            formatStrings,
                            shouldTrim,
                            omitEmptyKeys,
                            rejectOnDuplicate,
                            rejectOnMaterializedTypeError,
                            rejectOnEmptyKey,
                            treatEmptyValAsNull,
                            enforcelength);
}

void FDelimitedParserFactory::getParserReturnType(ServerInterface &srvInterface,
        PerColumnParamReader &perColumnParamReader,
        PlanContext &planCtxt,
        const SizedColumnTypes &argTypes,
        SizedColumnTypes &returnType)
{
    returnType = argTypes;
}

void FDelimitedParserFactory::getParameterType(ServerInterface &srvInterface,
                              SizedColumnTypes &parameterTypes) {
    parameterTypes.addVarchar(1, "delimiter");
    parameterTypes.addVarchar(1, "record_terminator");
    parameterTypes.addBool("header");
    parameterTypes.addVarchar(256, "format");
    parameterTypes.addBool("trim");
    parameterTypes.addBool("omit_empty_keys");
    parameterTypes.addBool("reject_on_duplicate");
    parameterTypes.addBool("reject_on_materialized_type_error");
    parameterTypes.addBool("reject_on_empty_key");
    parameterTypes.addBool("treat_empty_val_as_null");
    parameterTypes.addBool("enforce_length");
}
/// END class FDelimitedParserFactory



// Register the parser factory
RegisterFactory(FDelimitedParserFactory);



/// BEGIN class FDelimitedExtractor
FDelimitedExtractor::FDelimitedExtractor(
                            char delimiter,
                            bool shouldTrim,
                            std::string headerNamesParam,
                            bool treatEmptyValAsNull,
			     			vbool enforceLength)
    : currentColPosition(0), currentColSize(0), delimiter(delimiter), header_names_param(headerNamesParam),
      map_data_buf(NULL), map_data_buf_len(-1), pair_buf(NULL),
      should_trim(shouldTrim), treat_empty_val_as_null(treatEmptyValAsNull),enforcelength(enforceLength) {}

FDelimitedExtractor::~FDelimitedExtractor() {}

bool FDelimitedExtractor::fetchNextColumn(const VString& inputRecord) {
    return fetchNextColumn(inputRecord.data() + currentColPosition, inputRecord.length());
}

bool FDelimitedExtractor::fetchNextColumn(std::string inputRecord) {
    return fetchNextColumn(((const char*)(inputRecord.c_str() + currentColPosition)), inputRecord.length());
}

bool FDelimitedExtractor::fetchNextColumn(const char* pos, size_t len) {
    // read until the next delimiter or the record end, whichever comes first.
    // So this can be a very tight loop:
    // Just scan forward until we hit one of the two.
    currentColSize = 0;

    while ((currentColSize + currentColPosition) < len && *pos != delimiter) {
        ++pos;
        ++currentColSize;
    }
    return (((currentColSize + currentColPosition) < len) && *pos == delimiter);
}

void FDelimitedExtractor::advanceCol() {
    currentColPosition += currentColSize + 1;
}

void FDelimitedExtractor::rejectRecord(const std::string &reason) {
    LogDebugUDBasic("%s", reason.c_str());
}

void FDelimitedExtractor::processBlock(ServerInterface &srvInterface,
                                  BlockReader &argReader,
                                  BlockWriter &resultWriter) {
    uint64 rowNum = 0;
    do {
        if ((rowNum == 0) && (header_names_param.length() > 0)) {
            currentColPosition = 0;
            currentColSize = 0;
            uint32_t colNum = 0;
            uint32_t sourceColNum = 0;
            bool done = false;
            while (!done) {
                done = !fetchNextColumn(header_names_param);
                std::string headCol = std::string(header_names_param.data() + currentColPosition, currentColSize);
                header_names.push_back((should_trim) ? trim(headCol) : headCol);
#if defined (DEBUG)
                Assert((colNum + 1) == header_names.size());
#endif
                advanceCol();
                sourceColNum++;
                colNum++;
            }
            LogDebugUDBasic("Processed a header row parameter with %zu columns.", header_names.size());
        }

        rowNum++;
        currentColPosition = 0;
        currentColSize = 0;
        bool rejected = false;
        const VString& inputRecord = argReader.getStringRef(0);
        VString& outvmap = resultWriter.getStringRef();
        try {
            if (inputRecord.isNull() || inputRecord.length() == 0) {
                outvmap.setNull();
            } else {

                const char* record = inputRecord.data();
                VMapPairWriter map_writer(pair_buf, map_data_buf_len);

                // Parse each column into the map
                uint32_t colNum = 0;
                bool done = false;
                while (!done) {
                    done = !fetchNextColumn(inputRecord);

                    if (header_names.size() <= colNum) {
                        // make up a column name
                        std::ostringstream oss;
                        oss << "ucol" << colNum;
                        header_names.push_back(oss.str());
#if defined (DEBUG)
                        Assert((colNum + 1) == header_names.size());
#endif
                    }
                    std::string headerName = header_names[colNum];

                    const char* currColData = record + currentColPosition;
                    size_t bound = currentColSize;
                    if (should_trim) {
                        trim_buf_ends(&currColData, &bound);
                    }
                    bool isNull = (bound == 0 && treat_empty_val_as_null);
                    if (map_writer.append(srvInterface, headerName.c_str(), headerName.length(), isNull, currColData, bound)) {
                        advanceCol();
                        colNum++;
                    } else {
                        rejected = true;
                        std::stringstream rejectMessage;
                        rejectMessage << "Error:  Row [" << rowNum << "] failed to append value for column: [" << std::string(headerName.c_str(), headerName.length()) << "]";
                        log("%s", rejectMessage.str().c_str());
                        rejectMessage << " with value: [" << std::string(currColData, bound) << "].";
                        rejectRecord(rejectMessage.str());
                        break;
                    }
                }
                if (rejected) {
                    outvmap.setNull();
                } else {
                    // Capture dump-column data
                    size_t total_offset = 0;
                    VMapPairReader map_reader(map_writer);
                    VMapBlockWriter::convert_vmap(srvInterface, map_reader,
                                             map_data_buf, map_data_buf_len, total_offset);
                    outvmap.copy(map_data_buf, total_offset);
                }
            }

        } catch(std::exception& e) {
            outvmap.setNull();
            LogDebugUDBasic("Exception while processing partition row: [%llu] on map delimited extractor: [%s]", rowNum, e.what());
        } catch(...) {
            outvmap.setNull();
            LogDebugUDBasic("Unknown exception while processing partition row: [%llu] on map delimited extractor.", rowNum);
        }

        resultWriter.next();

    } while (argReader.next());
}

void FDelimitedExtractor::setup(ServerInterface &srvInterface, const SizedColumnTypes &argTypes) {
    // setup VMap buffers
    map_data_buf_len = estimateNeededSpaceForMap(argTypes.getColumnType(0).getStringLength(), header_names_param);
    map_data_buf = (char*)srvInterface.allocator->alloc(map_data_buf_len);
    pair_buf = (char*)srvInterface.allocator->alloc(map_data_buf_len);
}


/// Smaller inputs need more buffer space, as compared to their total size (luckily, small buffers are cheaper)
inline size_t FDelimitedExtractor::estimateNeededSpaceForMap(size_t inputDelimSize, std::string headerNamesParam) {
    return 10.0 * ((double)inputDelimSize) +
              6 * ((double)inputDelimSize) * ((inputDelimSize <= 10) ? 1 : 0) +
              6 * ((double)inputDelimSize) * ((inputDelimSize <= 4)  ? 1 : 0) +
             20 * ((double)inputDelimSize) * ((inputDelimSize == 1)  ? 1 : 0) +
           (1.5 * headerNamesParam.length());
           // XXX FUTURE XXX Estimate col size better via counting delimiters and adding known size for auto-generated column names (2.5 is fine for normal-sized values, but chokes on: MapDelimitedExtractor('A|B|C|D|E') and MapDelimitedExtractor('Aaaaa|Bbbbb|Ccccc|Ddddd|Eeeee'); 8.0 still chokes on: MapDelimitedExtractor('A|B|C|D|E'), missing by only 3 Bytes); 13 chokes on: MapDelimitedExtractor('A|B')
}
/// END class FDelimitedExtractor


/// BEGIN class FDelimitedExtractorFactory
FDelimitedExtractorFactory::FDelimitedExtractorFactory() {
    vol = IMMUTABLE;
}

void FDelimitedExtractorFactory::getPrototype(ServerInterface &srvInterface,
                                              ColumnTypes &argTypes,
                                              ColumnTypes &returnType)
{
    argTypes.addLongVarchar();
    returnType.addLongVarbinary();
}

void FDelimitedExtractorFactory::getReturnType(ServerInterface &srvInterface,
                                          const SizedColumnTypes &input_types,
                                          SizedColumnTypes &output_types)
{
    output_types.addLongVarbinary(VALUE_FOOTPRINT, MAP_VALUE_OUTPUT_NAME);
}

ScalarFunction* FDelimitedExtractorFactory::createScalarFunction(ServerInterface &srvInterface)
{
    // Defaults
    std::string delimiter("|");
    bool shouldTrim(true);
    std::string headerNamesParam("");
    bool treatEmptyValAsNull(true);

    // Args
    ParamReader args(srvInterface.getParamReader());
    if (args.containsParameter("delimiter"))
        delimiter = args.getStringRef("delimiter").str();
    if (args.containsParameter("trim"))
        shouldTrim = args.getBoolRef("trim");
    if (args.containsParameter("header_names")) {
        const VString& headerRow = args.getStringRef("header_names");
        headerNamesParam = headerRow.str();
    }
    if (args.containsParameter("treat_empty_val_as_null"))
        treatEmptyValAsNull = args.getBoolRef("treat_empty_val_as_null");

    // Validate
    if (delimiter.size()!=1) {
        vt_report_error(0, "Invalid delimiter [%s]: single character required",
                        delimiter.c_str());
    }

    vbool enforceLength = vbool_null;
    if (args.containsParameter("enforce_length")) {
        enforceLength = args.getBoolRef("enforce_length");
    }
    if (enforceLength == vbool_null) {
        enforceLength = vbool_false;
    }

    return vt_createFuncObj(srvInterface.allocator,
                            FDelimitedExtractor,
                            delimiter[0],
                            shouldTrim,
                            headerNamesParam,
                            treatEmptyValAsNull, 
                            enforceLength);
}

void FDelimitedExtractorFactory::getParameterType(ServerInterface &srvInterface,
                              SizedColumnTypes &parameterTypes) {
    parameterTypes.addVarchar(1, "delimiter");
    parameterTypes.addBool("trim");
    parameterTypes.addVarchar(MAP_FOOTPRINT, "header_names");
    parameterTypes.addBool("treat_empty_val_as_null");
    parameterTypes.addBool("enforce_length");
}
/// END class FDelimitedExtractorFactory



// Register the extractor factory
RegisterFactory(FDelimitedExtractorFactory);


} /// END namespace flextable


