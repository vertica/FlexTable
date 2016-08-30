/**
 * Copyright (c) 2005 - 2015 Hewlett Packard Enterprise Development LP
 *
 * Creation Date: XXX TODO XXX Fill in when releasing this code
 */
/**
 * Flex Table DC parser
 */

#include "FlexTable.h"
#include "DC.h"




namespace flextable
{


/// BEGIN class FlexTableDCParser
FlexTableDCParser::FlexTableDCParser(std::vector<std::string> formatStrings)
    : currentRecordSize(0), recordTerminator("\n.\n"),
      map_col_index(-1), map_data_buf(NULL), map_data_buf_len(-1),
      pair_buf(NULL), is_flextable(false),
      formatStrings(formatStrings)
{}

FlexTableDCParser::~FlexTableDCParser() {
}

bool FlexTableDCParser::fetchNextRow() {
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

    // progress through multi-character record-terminator
    size_t matched = 0, end = recordTerminator.size();

    do {
        // Get some (more) data
        reserved = cr.reserve(reservationRequest);

        // Position counter.  Not allowed to pass getDataPtr() + reserved.
        ptr = (char*)cr.getDataPtr() + position;

        // Keep reading until we hit EOF.
        // If we find the record terminator, we'll return out of the loop.
        // Very tight loop; very performance-sensitive.
        while (matched < end && position < reserved) {
            if (*ptr == recordTerminator[matched]) matched++; 
            else matched = 0;
            ++ptr;
            ++position;
        }

        if (matched == end) {
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

bool FlexTableDCParser::FlexTableDCParser::fetchNextColumn() {
    // fetchNextRow() has guaranteed that we can read until the next
    // delimiter or the record terminator, whichever comes first.
    // So this can be a very tight loop:
    // Just scan forward until we hit one of the two.
    char* pos = (char*)cr.getDataPtr() + currentColPosition;
    currentColSize = 0;

    while ((currentColSize + currentColPosition) < currentRecordSize && *pos != '\n') {
        ++pos;
        ++currentColSize;
    }
    return ((currentColSize + currentColPosition) < currentRecordSize && *pos == '\n') &&
        !(currentColSize == 1 && *(pos-1) == '.');  // . on a line by itself is the recordterm
}

bool FlexTableDCParser::handle_field(size_t colNum, char* start, size_t len, bool hasPadding)
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

void FlexTableDCParser::initCols() {
    currentColPosition = 0;
    currentColSize = 0;
}

void FlexTableDCParser::advanceCol() {
    currentColPosition += currentColSize + 1;
}

void FlexTableDCParser::rejectRecord(const std::string &reason) {
    RejectedRecord rr(reason, (char*)cr.getDataPtr(), currentRecordSize,
                      "\n.\n");
    crej.reject(rr);
}

void FlexTableDCParser::run() {
    while (fetchNextRow()) {
        bool rejected = false;

        // Special case: ignore trailing newlines (record terminators) at
        // the end of files
        if (cr.isEof() && currentRecordSize == 0) {
            break;
        }

        initCols();

        //log("DC Record: [%s]",std::string((char*)cr.getDataPtr(),currentRecordSize).c_str());

        if (is_flextable) {
            // Flex Table parsing
            VMapPairWriter map_writer(pair_buf, map_data_buf_len);

            // Parse each column into the map
            bool done = false;
            while (!done) {
                done = !fetchNextColumn();
                char* key = (char*)cr.getDataPtr() + currentColPosition;
                char* value = strchr(key,':'); // find the :
                if (value != NULL && key != value && value < key + currentColSize) {
                    size_t keybound = value-key;
                    value++;       // move past the : character
                    size_t valbound = currentColSize-keybound-1;
                    map_writer.append(getServerInterface(), key,keybound, false, value, valbound);
                }
                advanceCol();
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
                ImmutableStringPtr keyString(vpairs[virtualColNum].key_str(), vpairs[virtualColNum].key_length());
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

            // Make sure our output column is clean (especially in the case of previously-rejected rows)
            for (size_t realColNum = 0; realColNum < writer->getNumCols(); realColNum++) {
                writer->setNull(realColNum);
            }

            // Parse each key=value pair
            while (fetchNextColumn()) {
                // Do something with that column's data.
                // Typically involves writing it to our StreamWriter,
                // in which case we have to know the input column number.
                char* key = (char*)cr.getDataPtr() + currentColPosition;
                char* value = strchr(key,':'); // find the =
                if (value != NULL && key != value && value < key + currentColSize) {
                    size_t keybound = value-key;
                    value++;       // move to the next character
                    size_t valbound = currentColSize-keybound-1;
                    ImmutableStringPtr keyString(key, keybound);
                    ColLkup::iterator it = real_col_lookup.find(keyString);
                    if (it != real_col_lookup.end()) {
                        if (!handle_field(it->second, value, valbound, true)) {
                            std::stringstream ss;
                            ss<<"Parse error in column " << std::string(key,keybound);
                            rejectRecord(ss.str());
                            rejected = true;
                            break;  // Don't bother parsing this row.
                        }
                    }
                }

                advanceCol();
            }
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

void FlexTableDCParser::initialize(ServerInterface &srvInterface, SizedColumnTypes &returnType) {
    colInfo = returnType;
    aux_buf.initialize(&srvInterface, BASE_RESERVE_SIZE);

    if (formatStrings.size() != returnType.getColumnCount()) {
        formatStrings.resize(returnType.getColumnCount(), "");
    }
    sp.setFormats(formatStrings);

    for (uint32 col = 0; col < colInfo.getColumnCount(); col++) {
        const std::string &str = colInfo.getColumnName(col);
        aux_buf.allocate(str.size() + 1);
        normalize_key(str.c_str(), str.size(), aux_buf.loc);
        real_col_lookup[ImmutableStringPtr(aux_buf.loc, str.size())] = col;
    }

    // Find the Flex Table __raw__ column
    for (uint32 col = 0; col < colInfo.getColumnCount(); col++) {
        if ((0 == strcmp(colInfo.getColumnName(col).c_str(), RAW_COLUMN_NAME)) && colInfo.getColumnType(col).isStringType()) {
            map_col_index = col;
            map_data_buf_len = colInfo.getColumnType(col).getStringLength();
            map_data_buf = (char*)srvInterface.allocator->alloc(map_data_buf_len);
            break;
        }
    }

    if (map_data_buf_len > 0) {
        pair_buf = (char*)srvInterface.allocator->alloc(map_data_buf_len);
        is_flextable = true;
    }
}
/// END class FlexTableDCParser


/// BEGIN class FDCParserFactory
void FDCParserFactory::plan(ServerInterface &srvInterface,
        PerColumnParamReader &perColumnParamReader,
        PlanContext &planCtxt) {
    /* Check parameters */
    // Nothing to do here
}

UDParser* FDCParserFactory::prepare(ServerInterface &srvInterface,
        PerColumnParamReader &perColumnParamReader,
        PlanContext &planCtxt,
        const SizedColumnTypes &returnType)
{
    ParamReader args(srvInterface.getParamReader());

    std::vector<std::string> formatStrings;

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
                log("Got format: [%s] : [%s]",cname.c_str(),formatStrings[i].c_str());
            }
        }
    }

    return vt_createFuncObj(srvInterface.allocator,
                            FlexTableDCParser,
                            formatStrings
        );
}

void FDCParserFactory::getParserReturnType(ServerInterface &srvInterface,
        PerColumnParamReader &perColumnParamReader,
        PlanContext &planCtxt,
        const SizedColumnTypes &argTypes,
        SizedColumnTypes &returnType)
{
    returnType = argTypes;
}

void FDCParserFactory::getParameterType(ServerInterface &srvInterface,
                              SizedColumnTypes &parameterTypes) {
}
/// END class FDCParserFactory



// Register the parser factory
RegisterFactory(FDCParserFactory);


} /// END namespace flextable


