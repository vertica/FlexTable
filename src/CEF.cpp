/**
 * Copyright (c) 2005 - 2015 Hewlett Packard Enterprise Development LP
 *
 * Creation Date: March 3, 2014
 */
/**
 * Flex Table Cef parser
 */

#include "FlexTable.h"
#include "CEF.h"

#include <string.h>


namespace flextable
{

/// BEGIN class FlexTableCefParser
// Note: start the counter at -1 because it increments before being used
FlexTableCefParser::FlexTableCefParser(
                            char delimiter,
                            char recordTerminator,
                            std::vector<std::string> formatStrings,
                            bool shouldTrim,
                            bool rejectOnUnescapedDelimiter)
    : currentRecordSize(0), delimiter(delimiter), recordTerminator(recordTerminator),
      map_col_index(-1), map_data_buf(NULL), map_data_buf_len(-1),
      pair_buf(NULL), tmp_escape_buf(NULL), tmp_escape_buf_len(0),
      is_flextable(false), has_escape_character(false),
      formatStrings(formatStrings), should_trim(shouldTrim), counter(-1),
      reject_on_unescaped_delimiter(rejectOnUnescapedDelimiter),
      rejected(false) {}

FlexTableCefParser::~FlexTableCefParser() {
}

ImmutableStringPtr FlexTableCefParser::prefixColNames[] = {
    ImmutableStringPtr(PREFIX_COL1, sizeof(PREFIX_COL1) - 1),
    ImmutableStringPtr(PREFIX_COL2, sizeof(PREFIX_COL2) - 1),
    ImmutableStringPtr(PREFIX_COL3, sizeof(PREFIX_COL3) - 1),
    ImmutableStringPtr(PREFIX_COL4, sizeof(PREFIX_COL4) - 1),
    ImmutableStringPtr(PREFIX_COL5, sizeof(PREFIX_COL5) - 1),
    ImmutableStringPtr(PREFIX_COL6, sizeof(PREFIX_COL6) - 1),
    ImmutableStringPtr(PREFIX_COL7, sizeof(PREFIX_COL7) - 1)
};

bool FlexTableCefParser::fetchNextRow() {
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
        while ((position < reserved) && *ptr != recordTerminator) {
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

bool FlexTableCefParser::fetchNextColumn(char _delimiter) {
    // fetchNextRow() has guaranteed that we can read until the next
    // delimiter or the record terminator, whichever comes first.
    // So this can be a very tight loop:
    // Just scan forward until we hit one of the two.
    char* pos = (char*)cr.getDataPtr() + currentColPosition;
    currentColSize = 0;
    has_escape_character = false; // Reset at the beginning of each column

    while ((currentColSize + currentColPosition) < currentRecordSize &&
           *pos != _delimiter && *pos != recordTerminator) {
        ++pos;
        ++currentColSize;
        // handle spaces in the prefix and the key value pairs
        if (*pos == _delimiter) {
            // in the prefix, the pipes can be escaped
            char *escape = pos - 1;
            if (*pos == delimiter) {
                // check for a sequence of delimiters
                delimiterLookAhead(pos);
            } else if (*escape == ESCAPE_CHARACTER) {
                // skip escaped pipes in the prefix
                ++pos;
                ++currentColSize;
            }
        }
        // Backslash detected in the column
        if (*pos == ESCAPE_CHARACTER) {
            has_escape_character = true;
        }
    }
    return ((currentColSize + currentColPosition) < currentRecordSize && *pos == _delimiter);
}

bool FlexTableCefParser::fetchNextPrefixColumn(char _delimiter) {
    // Parse a certain number of prefix fields
    counter++;
    if (counter < PREFIX_FIELDS) {
        return fetchNextColumn(_delimiter);
    } else {
        return false;
    }
}

// Finds the last unescaped delimiter before the next unescaped equals sign.
// pos walks the string until the next unescaped delimiter, then searches
// for an unescaped equals between pos and the last delimiter it found
void FlexTableCefParser::delimiterLookAhead(char* &pos) {
    size_t incr = 0; // used to increment currentColSize
    char *lastDelim = pos; // used to keep a record of the last space
    // bool checkForEscapes = false;
    while (true) {
        pos++;
        incr++;

        // Backslash detected in the column
        if (*pos == ESCAPE_CHARACTER) {
            has_escape_character = true;
        }

        // When next delimiter found, enter if statement
        if (*pos == delimiter || *pos == recordTerminator) {
            // Create a string which is between the two delimiters
            // Check for the equals sign in that string
            char *equalsPtr = (char*) memchr(lastDelim, '=', pos - lastDelim);

            // If the equals exists and it is not escaped,
            // we have found the end of the current value
            if (equalsPtr != NULL && *(equalsPtr - 1) != ESCAPE_CHARACTER) {
                pos = lastDelim;
                break;
            } else {
                // checkForEscapes = true;
                if (reject_on_unescaped_delimiter) {
                    std::stringstream ss;
                    ss << "Error: Unescaped delimiter detected at position " <<
                        currentColPosition + currentColSize + 1;
                    rejectRecord(ss.str());
                    rejected = true;
                    break;
                }
                lastDelim = pos; // move lastDelim to the next space
                currentColSize += incr;
                incr = 0;
                if (*pos == recordTerminator) {
                    // Edge case: if the delimiter is the last character before the
                    // record terminator, it will be included in the table unless we
                    // decrement the column size by one
                    if (*(pos - 1) == delimiter) {
                        currentColSize--;
                    }
                    break;
                }
            }
        }
    }
    // when the position is on a new line, this pos value will not
    // match the pos in fetchNextColumn(), otherwise they are the same
}

// Scan for escape characters and remove them
void FlexTableCefParser::removeBackslashes(char* &value, size_t &valbound) {
    ServerInterface &srvInterface = getServerInterface();
    if (tmp_escape_buf_len <= valbound) {
        tmp_escape_buf = (char*)srvInterface.allocator->alloc(valbound);
        tmp_escape_buf_len = valbound;
    }

    size_t i;
    size_t escCtr = 0; // Amount to subtract from valbound after the loop
    size_t lastEsc = 0; // Position of previous escape character
    size_t numBytes = 0; // Bytes to write to buffer
    char* dst = tmp_escape_buf; // Destination is our temporary buffer
    char* src = value; // Source is the data in the current column

    /* Iterate through the value string character by character.  If a character
     * is the escape character, add characters up to the current esc.  For instance,
     * if the escape characters are at positions 4 and 8 in value, we want to write
     * characters 0-3, 5-7, and 9-12 from value into the tmp buf.
     */
    for (i=0; i<valbound; i++) {
        char* pos = value + i;
        if (*pos == ESCAPE_CHARACTER && *(pos + 1) != 'n' && *(pos + 1) != 'r') {
            escCtr++;
            dst += numBytes; // Move the temp buf pointer by adding bytes written
            if (escCtr == 1) {
                numBytes = i;
            } else {
                src = value + lastEsc + 1; // Data one position after the last esc
                numBytes = i - (lastEsc + 1); // In between last esc and current
            }
            memcpy(dst, src, numBytes);
            lastEsc = i;

            // If two escs in a row, skip the next esc
            if (*(pos + 1) == ESCAPE_CHARACTER) {
                i++;
            }
        }
    }

    /* When an escape is found in the next column by delimiterLookAhead, this
     * function will be called even if this column does not have any escapes.
     * So check if we found any escapes and if not, don't change anything
     */
    if (escCtr) {
        src = value + lastEsc + 1;
        dst += numBytes;
        numBytes = i - (lastEsc + 1);
        memcpy(dst, src, numBytes);
        value = tmp_escape_buf;
        valbound -= escCtr; // Account for the backslashes which have been removed
    }
}

bool FlexTableCefParser::handleField(size_t colNum, char* start, size_t len, bool hasPadding) {
    // Empty colums are null.
    if (len==0) {
        writer->setNull(colNum);
        return true;
    } else {
        NullTerminatedString str(start, len, false, hasPadding);
        return parseStringToType(str.ptr(), str.size(), colNum, colInfo.getColumnType(colNum), writer, sp);
    }
}

void FlexTableCefParser::initCols() {
    currentColPosition = 0;
    currentColSize = 0;
}

void FlexTableCefParser::advanceCol() {
    currentColPosition += currentColSize + 1;
}

bool FlexTableCefParser::rejectMessage() {
    std::stringstream ss;
    if (counter < PREFIX_FIELDS) {
        ss << "Error: Non standard CEF prefix - too few prefix fields";
        rejectRecord(ss.str());
        return true;
    }
    return false;
}

void FlexTableCefParser::rejectRecord(const std::string &reason) {
    RejectedRecord rr(reason, (char*)cr.getDataPtr(), currentRecordSize,
                      std::string(1, recordTerminator));
    crej.reject(rr);
}

void FlexTableCefParser::run() {
    while (fetchNextRow()) {
        rejected = false;
        counter = -1; // counter increments before being used, so -1 instead of 0

        initCols();
        // Special case: ignore trailing newlines (record terminators) at
        // the end of files
        if (cr.isEof() && currentRecordSize == 0) {
            break;
        }

        // check for a prefix by looking for "CEF:" in the first 4 characters
        NullTerminatedString checkFirst4Chars((char*)cr.getDataPtr(), VERSION_NUMBER_POSITION);
        hasPrefix = (strcmp(PREFIX_INDICATOR, (const char*)(checkFirst4Chars.ptr())) == 0);

        if (is_flextable) {
            // Flex Table parsing
            VMapPairWriter map_writer(pair_buf, map_data_buf_len);

            if (hasPrefix) {
                currentColPosition = VERSION_NUMBER_POSITION; // only read # from CEF:#

                while (fetchNextPrefixColumn(PREFIX_DELIMITER)) {
                    // write the predetermined key and the parsed value to the Vmap pair
                    char* value = (char*)cr.getDataPtr() + currentColPosition;
                    size_t valbound = currentColSize;
                    if (has_escape_character) {
                        removeBackslashes(value, valbound);
                    }
                    map_writer.append(getServerInterface(), prefixColNames[counter].ptr,
                            prefixColNames[counter].len, false,
                            value, valbound);
                    advanceCol();
                }
                rejected = rejectMessage();
            }

            // Parse each column into the map
            bool done = true;
            while (done) {
                done = fetchNextColumn(delimiter);
                char* key = (char*)cr.getDataPtr() + currentColPosition;
                // find the '=' within the bytes of the current record
                char* value = (char*) memchr(key, '=', currentRecordSize - currentColPosition);
                if (value && value < (key + currentColSize)) {
                    size_t keybound = value-key;
                    value++;       // move to the next character
                    size_t valbound = currentColSize-keybound-1;
                    if (should_trim) {
                        trim_buf_ends(&key, &keybound);
                        trim_buf_ends(&value, &valbound);
                    }

                    aux_buf.allocate(keybound + 1);
                    normalize_key(key, keybound, aux_buf.loc);

                    if (has_escape_character) {
                        removeBackslashes(value, valbound);
                    }
                    map_writer.append(getServerInterface(), aux_buf.loc, keybound, false,
                                      value, valbound);
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

                    handleField(it->second, aux_buf.loc, valueLen, true);
                    usedCols.insert(it->second);
                }
            }

        } else {
            // Regular structured parsing.

            // Make sure our output column is clean (especially in the case of previously-rejected rows)
            for (size_t realColNum = 0; realColNum < writer->getNumCols(); realColNum++) {
                writer->setNull(realColNum);
            }

            if (hasPrefix) {
                currentColPosition = VERSION_NUMBER_POSITION; // only read # from CEF:#
                while (fetchNextPrefixColumn(PREFIX_DELIMITER)) {
                    // match a key from the file to column name specified by the user
                    ImmutableStringPtr keyString((char *)prefixColNames[counter].ptr,
                        prefixColNames[counter].len);
                    ColLkup::iterator it = real_col_lookup.find(keyString);
                    // add the value to the table
                    if (it != real_col_lookup.end()) {
                        char* value = (char*)cr.getDataPtr() + currentColPosition;
                        size_t valbound = currentColSize;
                        if (has_escape_character) {
                            removeBackslashes(value, valbound);
                        }
                        if (!handleField(it->second, value, valbound)) {
                            std::stringstream ss;
                            ss<<"Parse error in column " << std::string(
                                    prefixColNames[counter].ptr,
                                    prefixColNames[counter].len);
                            rejectRecord(ss.str());
                            rejected = true;
                            break;  // Don't bother parsing this row.
                        }
                    }
                    advanceCol();
                }
                rejected = rejectMessage();
            }

            // Parse each key=value pair
            bool done = true;
            while (done) {
                done = fetchNextColumn(delimiter);
                // Do something with that column's data.
                // Typically involves writing it to our StreamWriter,
                // in which case we have to know the input column number.
                char* key = (char*)cr.getDataPtr() + currentColPosition;
                // find the '=' within the bytes of the current record
                char* value = (char*) memchr(key, '=', currentRecordSize - currentColPosition);
                if (value && value < key + currentColSize) {
                    size_t keybound = value-key;
                    value++;       // move to the next character
                    size_t valbound = currentColSize-keybound-1;
                    if (should_trim) {
                        trim_buf_ends(&key, &keybound);
                        trim_buf_ends(&value, &valbound);
                    }

                    aux_buf.allocate(keybound + 1);
                    normalize_key(key, keybound, aux_buf.loc);
                    if (has_escape_character) {
                        removeBackslashes(value, valbound);
                    }
                    ImmutableStringPtr keyString(aux_buf.loc, keybound);
                    ColLkup::iterator it = real_col_lookup.find(keyString);
                    if (it != real_col_lookup.end()) {
                        // Copy value into the auxiliary buffer so we can append
                        // a null terminator to it
                        aux_buf.allocate(valbound + 1);
                        memcpy(aux_buf.loc, value, valbound);
                        
                        if (!handleField(it->second, aux_buf.loc, valbound, true)) {
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

void FlexTableCefParser::initialize(ServerInterface &srvInterface, SizedColumnTypes &returnType) {
    colInfo = returnType;
    aux_buf.initialize(&srvInterface, BASE_RESERVE_SIZE);

    if (formatStrings.size() != returnType.getColumnCount()) {
        formatStrings.resize(returnType.getColumnCount(), "");
    }

    for (uint32 col = 0; col < colInfo.getColumnCount(); col++) {
        const std::string &str = colInfo.getColumnName(col);
        aux_buf.allocate(str.size() + 1);
        normalize_key(str.c_str(), str.size(), aux_buf.loc);
        real_col_lookup[ImmutableStringPtr(aux_buf.loc, str.size())] = col;
        aux_buf.loc += str.size() + 1;
    }

    // Find the flextable __raw__ column
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
/// END class FlexTableCefParser


/// BEGIN class FCefParserFactory
void FCefParserFactory::plan(ServerInterface &srvInterface,
        PerColumnParamReader &perColumnParamReader,
        PlanContext &planCtxt) {
    /* Check parameters */
//        ParamReader args(srvInterface.getParamReader());   . . .

    /* Populate planData */
    // Nothing to do here
}

UDParser* FCefParserFactory::prepare(ServerInterface &srvInterface,
        PerColumnParamReader &perColumnParamReader,
        PlanContext &planCtxt,
        const SizedColumnTypes &returnType)
{
    ParamReader args(srvInterface.getParamReader());

    // Defaults.
    std::string delimiter(" "), record_terminator("\n");
    std::vector<std::string> formatStrings;
    bool shouldTrim(true);
    bool rejectOnUnescapedDelimiter(false);

    // Args.
    if (args.containsParameter("delimiter"))
        delimiter = args.getStringRef("delimiter").str();
    if (args.containsParameter("record_terminator"))
        record_terminator = args.getStringRef("record_terminator").str();
    if (args.containsParameter("trim"))
        shouldTrim = args.getBoolRef("trim");
    if (args.containsParameter("reject_on_unescaped_delimiter"))
        rejectOnUnescapedDelimiter = args.getBoolRef("reject_on_unescaped_delimiter");

    // Validate.
    if (delimiter.size()!=1) {
        vt_report_error(0, "Invalid delimiter [%s]: single character required",
                        delimiter.c_str());
    }
    if (record_terminator.size()!=1) {
        vt_report_error(1, "Invalid record_terminator [%s]: single character required",
                        record_terminator.c_str());
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
                            FlexTableCefParser,
                            delimiter[0],
                            record_terminator[0],
                            formatStrings,
                            shouldTrim,
                            rejectOnUnescapedDelimiter
        );
}

void FCefParserFactory::getParserReturnType(ServerInterface &srvInterface,
        PerColumnParamReader &perColumnParamReader,
        PlanContext &planCtxt,
        const SizedColumnTypes &argTypes,
        SizedColumnTypes &returnType)
{
    returnType = argTypes;
}

void FCefParserFactory::getParameterType(ServerInterface &srvInterface,
                              SizedColumnTypes &parameterTypes) {
    parameterTypes.addVarchar(1, "delimiter");
    parameterTypes.addVarchar(1, "record_terminator");
    parameterTypes.addVarchar(256, "format");
    parameterTypes.addBool("trim");
    parameterTypes.addBool("reject_on_unescaped_delimiter");
}
/// END class FCefParserFactory



// Register the parser factory
RegisterFactory(FCefParserFactory);


} /// END namespace flextable

