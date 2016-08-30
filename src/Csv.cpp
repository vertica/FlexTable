/**
 * Copyright (c) 2005 - 2015 Hewlett Packard Enterprise Development LP
 *
 * Creation Date: December  2014
 */
/**
 * Flex Table Csv parser
 *
 */

#include "FlexTable.h"
#include "Csv.h"

namespace flextable {
/// BEGIN class FlexTableCsvParser
FlexTableCsvParser::FlexTableCsvParser(char delimiter, char recordTerminator,
                                       bool headerRow, std::vector<std::string> formatStrings,
                                       bool shouldTrim, bool omitEmptyKeys,
                                       bool rejectOnDuplicate, bool rejectOnMaterializedTypeError,
                                       bool rejectOnEmptyKey, char enclosed_by, char escape,
                                       std::string standardType, std::string NULLST,
                                       bool containsNULLparam) :
    currentRecordSize(0), delimiter(delimiter),
    recordTerminator(recordTerminator), enclosed_by(enclosed_by),
    escape(escape), standardType(standardType), NULLST(NULLST),
    headerRow(headerRow), containsNULLparam(containsNULLparam),
    map_col_index(-1), map_data_buf(NULL),
    map_data_buf_len(-1), pair_buf(NULL), is_flextable(false),
    formatStrings(formatStrings), should_trim(shouldTrim),
    omit_empty_keys(omitEmptyKeys), reject_on_duplicate(rejectOnDuplicate),
    reject_on_materialized_type_error(rejectOnMaterializedTypeError),
    reject_on_empty_key(rejectOnEmptyKey), aux_buf(),
    header_names() {
    state = OUTQUOTE;
}

FlexTableCsvParser::~FlexTableCsvParser() {
}

/*
 * Method to detect rows and fields of the row, based on RFC4180 standard
 * We have two states - inside quotes (enclosed_by char) and outside quotes
 * When fields are detected the vector fieldsPosition will
 * store the positions (start and end) of each field
 * Another vector will store the positions of the characters that are escape (double-double quotes)
 *  to then remove the escape character
 * remove of the escape character will be at VMap construction state (run method)
 */
bool FlexTableCsvParser::fetchNextRowRFC4180() {
    // Amount of data we have to work with
    size_t reserved;
    // Amount of data that we've requested to work with.
    // Equal to `reserved` after calling reserve(), except in case of end-of-file.
    size_t reservationRequest = BASE_RESERVE_SIZE;

    // Pointer into the middle of our current data buffer.
    // Must always be between getDataPtr() and getDataPtr() + reserved.
    char* ptr;

    // Our current position within the stream.
    // Kept around so that we can update ptr correctly after reserve()ing more data.

    size_t position = 0;

    fieldsPosition.clear();
    ignoreEscapedCharPosition.clear();
    fieldsStartingAndFinishWithQuotes.clear();
    int posField = 0; //position inside a field
    bool lastWasQuote = false;
    bool lastWasDelim = false;
    bool lastColWasQuoted = false;
    startedWithQuote = false;
    finishWithQuote = false;
    size_t fieldsDetected = 0;
    size_t lastQuotePos = 0;
    size_t lastDelimPos = 0;
    size_t startQuote = 0;
    size_t finishQuote = 0;

    do {
        // Get some (more) data
        reserved = cr.reserve(reservationRequest);

        // Position counter.  Not allowed to pass getDataPtr() + reserved.
        ptr = (char*) cr.getDataPtr() + position;
        // Keep reading until we hit EOF.
        // If we find the record terminator (not in quoted text), we'll return out of the loop.
        while (position < reserved) {
            switch (state) {

            case INQUOTE:
                //check quoting
                //actual is enclosed_by and last one was also enclosed_by
                //if (*ptr == enclosed_by && lastWasQuote) {
                //	state = OUTQUOTE;
                //	lastWasQuote = true;
                //	finishWithQuote = true;
                //store that we need to "remove" previous
                //	storePreviousCharToEscape(fieldsDetected, posField);
                //	lastQuotePos = position;
                //} else
                if (*ptr == enclosed_by) {
                    //actual is enclosed_by but last one was not enclosed_by
                    //set outquote state
                    lastWasQuote = true;
                    finishWithQuote = true;
                    state = OUTQUOTE;
                    lastQuotePos = position;
                    finishQuote = position;
                } else {
                    //actual is not enclosed_by, still INQUOTE state
                    lastWasQuote = false;
                    finishWithQuote = false;
                }
                ++ptr;
                ++position;
                posField++;
                break;

            case OUTQUOTE:
                //check quoting
                if (*ptr == enclosed_by && lastWasQuote) {
                    //double escaped enclosed_by character
                    state = INQUOTE;
                    //store that we need to "remove" previous
                    storePreviousCharToEscape(fieldsDetected, posField);
                    lastWasQuote = true;
                    lastQuotePos = position;
                } else if (*ptr == enclosed_by) {
                    //actual is quote and last one was not quote
                    state = INQUOTE;
                    lastWasQuote = true;
                    //if (lastWasDelim) {
                    startedWithQuote = true;
                    //}
                    lastQuotePos = position;
                    startQuote = position;
                    posField = 0;
                } else {
                    //actual is other char different from enclosed_by
                    lastWasQuote = false;
                }
                if (*ptr == recordTerminator) {
                    //we are outquote and found record_terminator -> end of row
                    currentRecordSize = position;
                    posField = 0;
                    if (startedWithQuote && finishWithQuote) {
                        //actual field (last one) started and finished with quotes
                        //store field position ignoring quotes
                        //if (fieldsPosition.size() == 0) {
                        //	fieldsPosition.push_back(1);
                        //} else if (lastColWasQuoted) {
                        //skip characters enclosed and delimiter
                        //	fieldsPosition.push_back(
                        //			fieldsPosition.back() + lastQuotedSkipSize);
                        //} else {
                        //last was not quoted
                        //	fieldsPosition.push_back(
                        //			fieldsPosition.back()
                        //					+ lastNotQuotedSkipSize);
                        //}
                        fieldsPosition.push_back(startQuote + 1);
                        fieldsPosition.push_back(finishQuote - 1);
                        lastColWasQuoted = true;
                        //add this field as it started and finished with quotes, this is to
                        //ignore quotes index when constructing the final field on run() method
                        fieldsStartingAndFinishWithQuotes.push_back(
                            fieldsDetected);
                        startQuote = 0;
                        finishQuote = 0;
                    } else {
                        //actual field is not enclosed_by, we dont have to consider
                        //quotes
                        if (fieldsPosition.size() == 0) {
                            fieldsPosition.push_back(0);
                        } else if (lastColWasQuoted) {
                            //fieldsPosition.push_back(
                            //		fieldsPosition.back()
                            //				+ lastQuotedActualNotQuotedSkipSize);
                            fieldsPosition.push_back(lastDelimPos + 1);
                        } else {
                            //fieldsPosition.push_back(
                            //		fieldsPosition.back()
                            //				+ lastNotQuotedActualNotQuotedSkipSize);
                            fieldsPosition.push_back(lastDelimPos + 1);
                        }
                        fieldsPosition.push_back(position - 1);
                        lastColWasQuoted = false;
                    }
                    fieldsDetected++;
                    //END
                    return true;
                }

                //check actual is delimiter and as we are outquote
                //a field is detected now (it is not the last one)
                if (*ptr == delimiter) {
                    posField = -1; ///-1 as we are always adding 1, so at the end it will be 0
                    if (startedWithQuote && finishWithQuote) {
                        //if (fieldsPosition.size() == 0) {
                        //	fieldsPosition.push_back(1);
                        //} else if (lastColWasQuoted) {
                        //	fieldsPosition.push_back(
                        //			fieldsPosition.back() + lastQuotedSkipSize);
                        //} else {
                        //	fieldsPosition.push_back(
                        //			fieldsPosition.back()
                        //					+ lastNotQuotedSkipSize);
                        //}
                        //fieldsPosition.push_back(lastQuotePos - 1);
                        fieldsPosition.push_back(startQuote + 1);
                        fieldsPosition.push_back(finishQuote - 1);
                        lastColWasQuoted = true;
                        fieldsStartingAndFinishWithQuotes.push_back(
                            fieldsDetected);
                        startQuote = 0;
                        finishQuote = 0;
                    } else {
                        if (fieldsPosition.size() == 0) {
                            fieldsPosition.push_back(0);
                        } else if (lastColWasQuoted) {
                            //fieldsPosition.push_back(
                            //		fieldsPosition.back()
                            //				+ lastQuotedActualNotQuotedSkipSize);
                            fieldsPosition.push_back(lastDelimPos + 1);
                        } else {
                            //fieldsPosition.push_back(
                            //		fieldsPosition.back()
                            //				+ lastNotQuotedActualNotQuotedSkipSize);
                            fieldsPosition.push_back(lastDelimPos + 1);
                        }
                        fieldsPosition.push_back(position - 1);
                        lastColWasQuoted = false;
                    }
                    startedWithQuote = false;
                    finishWithQuote = false;
                    lastWasDelim = true;
                    fieldsDetected++;
                    lastDelimPos = position;
                } else {
                    //actual not a delimiter, update var
                    lastWasDelim = false;
                }

                if (position == 0) {
                    //if is the first char and is enclosed_by
                    //update var
                    if (*ptr == enclosed_by) {
                        startedWithQuote = true;
                        lastQuotePos = position;
                    }
                }

                ++ptr;
                ++position;
                posField++;
                break;
            default:
                //could state be different from INQUOTE or OUTQUOTE?
                if (position == 0) {
                    fieldsPosition.push_back(position);
                }
                ++ptr;
                ++position;
                posField++;
                break;

            }
        }

        reservationRequest *= 2; // Request twice as much data next time
    } while (!cr.noMoreData()); // Stop if we run out of data;
    // correctly handles files that aren't newline terminated
    // and when we reach eof but haven't seeked there yet

    currentRecordSize = position;
    if (!fieldsPosition.empty()) {
        if (startedWithQuote && finishWithQuote) {

            fieldsPosition.push_back(startQuote + 1);
            fieldsPosition.push_back(finishQuote - 1);
            lastColWasQuoted = true;
            //add this field as it started and finished with quotes, this is to
            //ignore quotes index when constructing the final field on run() method
            fieldsStartingAndFinishWithQuotes.push_back(fieldsDetected);
            startQuote = 0;
            finishQuote = 0;
        } else {
            //actual field is not enclosed_by, we dont have to consider
            //quotes
            if (fieldsPosition.size() == 0) {
                fieldsPosition.push_back(0);
            } else if (lastColWasQuoted) {
                fieldsPosition.push_back(lastDelimPos + 1);
            } else {
                fieldsPosition.push_back(lastDelimPos + 1);
            }
            fieldsPosition.push_back(position - 1);
            lastColWasQuoted = false;
        }
    }
    return false;
}

/*
 * Method to detect rows and fields of the row based on the Traditional CSV Format
 * This is, escape is used. It can handle fields like: Field 1 with comma \,,Field 2 with \"embedded quote\",Field 3
 * THis differs from RFC4180 in that we can have escape sequences like '\,' inside a field
 */
bool FlexTableCsvParser::fetchNextRowTraditional() {
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

    fieldsPosition.clear();
    ignoreEscapedCharPosition.clear();
    fieldsStartingAndFinishWithQuotes.clear();
    int posField = 0; //position inside a field
    bool lastWasQuote = false;
    //we have escape
    bool lastWasEscape = false;
    bool lastWasDelim = false;
    bool lastColWasQuoted = false;
    startedWithQuote = false;
    finishWithQuote = false;
    size_t fieldsDetected = 0;
    size_t lastQuotePos = 0;
    size_t lastDelimPos = 0;
    size_t startQuote = 0;
    size_t finishQuote = 0;

    do {
        // Get some (more) data
        reserved = cr.reserve(reservationRequest);

        // Position counter.  Not allowed to pass getDataPtr() + reserved.
        ptr = (char*) cr.getDataPtr() + position;
        // Keep reading until we hit EOF.
        // If we find the record terminator (not in quoted text), we'll return out of the loop.
        while (position < reserved) {
            switch (state) {

            case INQUOTE:
                //check quoting
                //actual is enclosed_by and last one was also enclosed_by
                //if (*ptr == enclosed_by && lastWasQuote) {
                //	state = OUTQUOTE;
                //	lastWasQuote = false;
                //	finishWithQuote = true;
                //store that we need to "remove" previous
                //	storePreviousCharToEscape(fieldsDetected, posField);
                //	lastQuotePos = position;
                //} else
                if (*ptr == enclosed_by && !lastWasEscape) {
                    //change state if actual is encosed_by char but previous is not escape
                    lastWasQuote = true;
                    state = OUTQUOTE;
                    finishWithQuote = true;
                    lastQuotePos = position;
                    finishQuote = position;
                } else {
                    //actual is not enclosed_by, still INQUOTE state
                    lastWasQuote = false;
                    finishWithQuote = false;
                }
                //check escape
                if (*ptr == escape && !lastWasEscape) {
                    //set lastWasEscape, next one possible escape
                    lastWasEscape = true;
                } else if (lastWasEscape) {
                    //check for escape as previous was escape char
                    lastWasEscape = false;
                    checkEscape(ptr, fieldsDetected, posField);
                }
                ++ptr;
                ++position;
                posField++;
                break;

            case OUTQUOTE:
                //check quoting
                if (*ptr == enclosed_by && lastWasQuote) {
                    //double escaped enclosed_by character
                    state = INQUOTE;
                    //store that we need to "remove" previous
                    storePreviousCharToEscape(fieldsDetected, posField);
                    lastWasQuote = false;
                    lastQuotePos = position;
                } else if (*ptr == enclosed_by && !lastWasEscape) {
                    //actual is quote and last one was not quote
                    state = INQUOTE;
                    lastWasQuote = true;
                    //if (lastWasDelim) {
                    startedWithQuote = true;
                    //}
                    lastQuotePos = position;
                    startQuote = position;
                    posField = 0;
                } else {
                    //actual is other char different from enclosed_by
                    lastWasQuote = false;
                }

                if (*ptr == recordTerminator) {
                    //we are outquote and found record_terminator -> end of row
                    currentRecordSize = position;
                    posField = 0;
                    if (startedWithQuote && finishWithQuote) {
                        //actual field (last one) started and finished with quotes
                        //store field position ignoring quotes
                        //if (fieldsPosition.size() == 0) {
                        //	fieldsPosition.push_back(1);
                        //} else if (lastColWasQuoted) {
                        //skip characters enclosed and delimiter
                        //	fieldsPosition.push_back(
                        //			fieldsPosition.back() + lastQuotedSkipSize);
                        //} else {
                        //last was not quoted
                        //	fieldsPosition.push_back(
                        //			fieldsPosition.back()
                        //					+ lastNotQuotedSkipSize);
                        //}
                        fieldsPosition.push_back(startQuote + 1);
                        fieldsPosition.push_back(finishQuote - 1);
                        lastColWasQuoted = true;
                        //add this field as it started and finished with quotes, this is to
                        //ignore quotes index when constructing the final field on run() method
                        fieldsStartingAndFinishWithQuotes.push_back(
                            fieldsDetected);
                        startQuote = 0;
                        finishQuote = 0;
                    } else {
                        //actual field is not enclosed_by, we dont have to consider
                        //quotes
                        if (fieldsPosition.size() == 0) {
                            fieldsPosition.push_back(0);
                        } else if (lastColWasQuoted) {
                            //fieldsPosition.push_back(
                            //		fieldsPosition.back()
                            //				+ lastQuotedActualNotQuotedSkipSize);
                            fieldsPosition.push_back(lastDelimPos + 1);
                        } else {
                            //fieldsPosition.push_back(
                            //		fieldsPosition.back()
                            //				+ lastNotQuotedActualNotQuotedSkipSize);
                            fieldsPosition.push_back(lastDelimPos + 1);
                        }
                        fieldsPosition.push_back(position - 1);
                        lastColWasQuoted = false;
                    }
                    fieldsDetected++;
                    //END
                    return true;
                }
                //check actual is delimiter and as we are outquote
                //a field is detected now (is not the last one)
                if (*ptr == delimiter && !lastWasEscape) {
                    posField = -1; ///-1 as we are always adding 1 so at the end it will be 0
                    if (startedWithQuote && finishWithQuote) {
                        //if (fieldsPosition.size() == 0) {
                        //	fieldsPosition.push_back(1);
                        //} else if (lastColWasQuoted) {
                        //	fieldsPosition.push_back(
                        //			fieldsPosition.back() + lastQuotedSkipSize);
                        //} else {
                        //	fieldsPosition.push_back(
                        //			fieldsPosition.back()
                        //					+ lastNotQuotedSkipSize);
                        //}
                        //fieldsPosition.push_back(lastQuotePos - 1);
                        fieldsPosition.push_back(startQuote + 1);
                        fieldsPosition.push_back(finishQuote - 1);
                        lastColWasQuoted = true;
                        fieldsStartingAndFinishWithQuotes.push_back(
                            fieldsDetected);
                        startQuote = 0;
                        finishQuote = 0;
                    } else {
                        if (fieldsPosition.size() == 0) {
                            fieldsPosition.push_back(0);
                        } else if (lastColWasQuoted) {
                            //fieldsPosition.push_back(
                            //		fieldsPosition.back()
                            //				+ lastQuotedActualNotQuotedSkipSize);
                            fieldsPosition.push_back(lastDelimPos + 1);
                        } else {
                            //fieldsPosition.push_back(
                            //		fieldsPosition.back()
                            //				+ lastNotQuotedActualNotQuotedSkipSize);
                            fieldsPosition.push_back(lastDelimPos + 1);
                        }
                        fieldsPosition.push_back(position - 1);
                        lastColWasQuoted = false;
                    }
                    startedWithQuote = false;
                    finishWithQuote = false;
                    lastWasDelim = true;
                    fieldsDetected++;
                    lastDelimPos = position;
                } else {
                    //actual not a delimiter, update var
                    lastWasDelim = false;
                }

                if (position == 0) {
                    //if is the first char and is enclosed_by
                    if (*ptr == enclosed_by) {
                        startedWithQuote = true;
                        lastQuotePos = position;
                    }
                }

                //check escape
                if (*ptr == escape && !lastWasEscape) {
                    //next one possible escape sequence
                    lastWasEscape = true;
                } else if (lastWasEscape) {
                    //last was escape, check actual for escaping
                    lastWasEscape = false;
                    checkEscape(ptr, fieldsDetected, posField);
                }

                ++ptr;
                ++position;
                posField++;
                break;
            default:
                //could state be different from INQUOTE or OUTQUOTE?
                if (position == 0) {
                    fieldsPosition.push_back(position);
                }
                ++ptr;
                ++position;
                posField++;
                break;

            }
        }

        reservationRequest *= 2; // Request twice as much data next time
    } while (!cr.noMoreData()); // Stop if we run out of data;
    // correctly handles files that aren't newline terminated
    // and when we reach eof but haven't seeked there yet

    currentRecordSize = position;
    if (!fieldsPosition.empty()) {
        if (startedWithQuote && finishWithQuote) {

            fieldsPosition.push_back(startQuote + 1);
            fieldsPosition.push_back(finishQuote - 1);
            lastColWasQuoted = true;
            //add this field as it started and finished with quotes, this is to
            //ignore quotes index when constructing the final field on run() method
            fieldsStartingAndFinishWithQuotes.push_back(fieldsDetected);
            startQuote = 0;
            finishQuote = 0;
        } else {
            //actual field is not enclosed_by, we dont have to consider
            //quotes
            if (fieldsPosition.size() == 0) {
                fieldsPosition.push_back(0);
            } else if (lastColWasQuoted) {
                fieldsPosition.push_back(lastDelimPos + 1);
            } else {
                fieldsPosition.push_back(lastDelimPos + 1);
            }
            fieldsPosition.push_back(position - 1);
            lastColWasQuoted = false;
        }
    }
    return false;
}

/*
 * This method stores for each field the positions that we need to remove as they are
 * escape (commonly \)
 */
void FlexTableCsvParser::storePreviousCharToEscape(size_t fieldsDetected,
                                                   size_t position) {
    //if the vector does not have the index 'fieldsDetected' resize
    if (ignoreEscapedCharPosition.size() < (fieldsDetected + 1)) {
        ignoreEscapedCharPosition.resize(fieldsDetected + 1);
    }

    ignoreEscapedCharPosition.at(fieldsDetected).push_back(position);
}

/*checks for escaping sequences considering that previous
 * character was escape, if a escape sequence is found
 * as two character, store that we will remove previous escape
 */
int FlexTableCsvParser::checkEscape(char * ptr, size_t fieldsDetected,
                                    size_t position) {
    if (*ptr == escape || *ptr == enclosed_by || *ptr == delimiter) {
        // for '\\' , '\"' and '\,' in traditional or double escaped quotes in rfc4180""
        storePreviousCharToEscape(fieldsDetected, position - 1);
    }
    //else if (*ptr == 'n') {
    //*(ptr) = '\n';
    //	storePreviousCharToEscape(fieldsDetected, position);
    //	return 1;
    //} else if (*ptr == 'a') {
    //*(ptr) = '\a';
    //	storePreviousCharToEscape(fieldsDetected, position);
    //} else if (*ptr == 'b') {
    //*(ptr) = '\b';
    //	storePreviousCharToEscape(fieldsDetected, position);
    //} else if (*ptr == 'f') {
    //*(ptr) = '\f';
    //	storePreviousCharToEscape(fieldsDetected, position);
    //} else if (*ptr == 'r') {
    //*(ptr) = '\r';
    //	storePreviousCharToEscape(fieldsDetected, position);
    //} else if (*ptr == 't') {
    //*(ptr) = '\t';
    //	storePreviousCharToEscape(fieldsDetected, position);
    //} else if (*ptr == 'v') {
    //*(ptr) = '\v';
    //	storePreviousCharToEscape(fieldsDetected, position);
    //}
    return 0;

}

bool FlexTableCsvParser::handle_field(size_t colNum, char* start, size_t len,
                                      bool hasPadding) {
    // Empty colums are null.
    if (len == 0) {
        writer->setNull(colNum);
        return true;
    } else {
        NullTerminatedString str(start, len, false, hasPadding);
        try {
            return parseStringToType(str.ptr(), str.size(), colNum,
                                     colInfo.getColumnType(colNum), writer, sp);
        } catch (std::exception& e) {
            log(
                "Exception while processing partition: [%s] while type converting Csv materialized column #[%lu], skipping this materialized column for this row.",
                e.what(), colNum);
        } catch (...) {
            log(
                "Unknown exception while type converting Csv materialized column #[%lu], skipping this materialized column for for this row.",
                colNum);
        }
    }
        
    writer->setNull(colNum);
    return false;
}

void FlexTableCsvParser::rejectRecord(const std::string &reason) {
    RejectedRecord rr(reason, (char*) cr.getDataPtr(), currentRecordSize,
                      std::string(1, recordTerminator));
    crej.reject(rr);
}

void FlexTableCsvParser::run() {
    bool processedHeader = !headerRow;

    uint64 rowNum = 0;
    std::vector<bool> emptyCols(0, false);

    //read rows and detect fields
    while (!cr.isEof()) {
        if (standardType.compare("traditional") == 0) {
            fetchNextRowTraditional();
        } else {
            fetchNextRowRFC4180();
        }
        rowNum++;
        bool rejected = false;

        if (currentRecordSize == 0 || fieldsPosition.size() == 0) {
            if (currentRecordSize > 1) {
                char* base = (char*) cr.getDataPtr();
                size_t bound = currentRecordSize;
                std::string ss(base, bound);
                std::stringstream rejectMessage;
                rejectMessage << "Error:  Row [" << rowNum
                              << "] failed, no fields detected";
                rejectMessage << " with value: [" << ss << "].";
                LogDebugUDBasic("%s", rejectMessage.str().c_str());
                rejectRecord(rejectMessage.str());

                cr.seek(currentRecordSize);

            } else
                cr.seek(1);
            continue;
        }

        // Process header
        if (headerRow && (!processedHeader)) {
            uint32_t colNum = 0;
            uint32_t sourceColNum = 0;
            std::set<std::string, VMapBlockWriter::StringInsensitiveCompare>
                caseInsensitiveSet = std::set<std::string,
                VMapBlockWriter::StringInsensitiveCompare>();
            //create header strings from the positions of fields that are stored in fieldsPosition
            for (size_t i = 0; i < fieldsPosition.size(); i += 2) {
                std::string headCol;
                headCol = std::string(
                    (char*) cr.getDataPtr() + fieldsPosition.at(i),
                    fieldsPosition.at(i + 1) - fieldsPosition.at(i) + 1);

                //headers can be escaped too
                //remove escaped characters that were detected as a 2 character sequence e.g. '\\' remove first '\'
                headCol = removeEscapedChars(headCol, (i / 2));

                header_names.push_back((should_trim) ? trim(headCol) : headCol);
#if defined (DEBUG)
                Assert((colNum + 1) == header_names.size());
#endif

                if (omit_empty_keys)
                    emptyCols.push_back(false);
                if ((reject_on_empty_key || omit_empty_keys)
                    && (header_names[colNum].length() <= 0
                        || ((!reject_on_duplicate) && trim(
                                header_names[colNum]).length() == 0))) {
                    if (reject_on_empty_key) {
                        rejected = true;
                        std::stringstream rejectMessage;
                        rejectMessage
                            << "Processed a header row with an empty key in column: ["
                            << colNum
                            << "] with reject_on_empty_key specified; rejecting.";
                        LogDebugUDWarn("%s", rejectMessage.str().c_str());
                        rejectRecord(rejectMessage.str());
                        break;
                    }

                    // omit_empty_keys
                    LogDebugUDWarn(
                        "Processed a header row with an empty key in column: [%d] with omit_empty_keys specified; skipping column.",
                        sourceColNum);
                    emptyCols[sourceColNum] = true;
                    header_names.pop_back();
                    sourceColNum++;
                    continue;
                }
                if (reject_on_duplicate) {
                    std::pair<std::set<std::string>::iterator, bool>
                        insertResult = caseInsensitiveSet.insert(
                            header_names[colNum]);
                    if (!insertResult.second) {
                        rejected = true;
                        std::stringstream rejectMessage;
                        rejectMessage
                            << "Processed a header row with duplicate keys with reject_on_duplicate specified; rejecting.";
                        LogDebugUDWarn("%s", rejectMessage.str().c_str());
                        rejectRecord(rejectMessage.str());
                        break;
                    }
                }
                sourceColNum++;
                colNum++;
            }
            cr.seek(currentRecordSize + 1);
            processedHeader = true;
            if (rejected) {
                break;
            }

            LogDebugUDBasic("Processed a header row with %zu columns.",
                            header_names.size());
            continue;
        }

        parsedRealCols.clear();
        parsedRealCols.resize(colInfo.getColumnCount());
        if (is_flextable) {
            VMapPairWriter map_writer(pair_buf, map_data_buf_len);

            // Parse each column into the map
            uint32_t colNum = 0;
            uint32_t sourceColNum = 0;
            for (size_t i = 0; i < fieldsPosition.size(); i += 2) {
                if (omit_empty_keys && emptyCols[sourceColNum]) {
                    //actual was detected as empty
                    sourceColNum++;
                    continue;
                }

                if (header_names.size() <= colNum) {
                    // make up a column name for 'extra' detected fields
                    std::ostringstream oss;
                    oss << "ucol" << colNum;
                    header_names.push_back(oss.str());
#if defined (DEBUG)
                    Assert((colNum + 1) == header_names.size());
#endif
                }
                std::string &headerName = header_names[colNum];

                //start position of field and size of field
                char* base = (char*) cr.getDataPtr() + fieldsPosition.at(i);
                size_t bound = fieldsPosition.at(i + 1) - fieldsPosition.at(i)
                    + 1;

                std::string ss(base, bound);
                //remove escaped characters that were detected as a 2 character sequence e.g. '\\' remove first '\'
                //change escaped version here instead of the iput buffer
                ss = removeEscapedChars(ss, (i / 2));
                if (should_trim) {
                    ss = trim(ss);
                }
                                
                if (containsNULLparam) {
                    if (0 == strcmp(ss.c_str(), NULLST.c_str())) {
                        ss = "";
                    }
                }

                base = (char*) ss.c_str();
                bound = ss.size();
                if (map_writer.append(getServerInterface(), headerName.c_str(),
                                      headerName.length(), false, ss.c_str(), bound)) {
                    //we added the field to map
                    colNum++;
                    sourceColNum++;
                } else {
                    //field could not be added - reject
                    rejected = true;
                    std::stringstream rejectMessage;
                    rejectMessage << "Error:  Row [" << rowNum
                                  << "] failed to append value for column: ["
                                  << std::string(headerName.c_str(),
                                                 headerName.length()) << "]";
                    rejectMessage << " with value: [" << std::string(base, bound) << "].";
                    LogDebugUDBasic("%s", rejectMessage.str().c_str());
                    rejectRecord(rejectMessage.str());
                    break;
                }
            }
            if (rejected) {
                cr.seek(currentRecordSize + 1);
                continue;
            }

            VMapPairReader map_reader(map_writer);


            // Capture dump-column data
            if (map_col_index != -1) {
                size_t total_offset = 0;
                VMapBlockWriter::convert_vmap(getServerInterface(), map_reader,
                                              map_data_buf, map_data_buf_len, total_offset);
                VString& map_data_col = writer->getStringRef(map_col_index);
                map_data_col.copy(map_data_buf, total_offset);
            }

            // Process any materialized columns
            const std::vector<VMapPair> &vpairs = map_reader.get_pairs();
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

                    if (handle_field(it->second, aux_buf.loc, valueLen, true)) {
                        parsedRealCols[it->second] = true;
                    } else if (reject_on_materialized_type_error) {
                        rejected = true;
                        std::stringstream rejectMessage;
                        rejectMessage << "Row [" << rowNum
                                      << "] rejected due to materialized type error on column: ["
                                      << std::string(vpairs[virtualColNum].key_str(),
                                                     vpairs[virtualColNum].key_length())
                                      << "]";
                        rejectMessage << " with value: ["
                                      << std::string(aux_buf.loc, valueLen)
                                      << "].";
                        LogDebugUDBasic("%s", rejectMessage.str().c_str());
                        rejectRecord(rejectMessage.str());
                        break;
                    }
                }
            }
            if (rejected) {
                cr.seek(currentRecordSize + 1);
                continue;
            }

        } else {
            // Regular structured parsing.
            // Parse each column; hope there are the right number of columns
            uint32_t colFields = 0;
            size_t numFields = fieldsPosition.size() / 2;

            if (numFields != colInfo.getColumnCount()) {
                rejected = true;
                std::stringstream ss3;
                ss3 << "Wrong number of columns! COPYing "
                    << colInfo.getColumnCount()
                    << " columns from a source file containing: "
                    << ((numFields < colInfo.getColumnCount() - 1) ? "FEWER"
                        : "MORE") << " columns."; // Convert 0-indexing to 1-indexing
                rejectRecord(ss3.str());
            } else {
                for (uint32_t colNum = 0; colNum < colInfo.getColumnCount(); colNum++) {
                    char* base = (char*) cr.getDataPtr() + fieldsPosition.at(
                        colFields);
                    size_t bound;
                    bound = fieldsPosition.at(colFields + 1)
                        - fieldsPosition.at(colFields) + 1;
                    std::string stField(base, bound);
                    //remove escaped characters that were detected as a 2 character sequence e.g. '\\' remove first '\'
                    stField = removeEscapedChars(stField, colNum);

                    if (should_trim) {
                        stField = trim(stField);
                    }
                    base = (char*) stField.c_str();
                    bound = stField.size();
                    if (!handle_field(colNum, base, bound, !cr.isEof())) {
                        rejected = true;
                        std::stringstream ss;
                        ss << "Parse error in column " << colNum + 1; // Convert 0-indexing to 1-indexing
                        rejectRecord(ss.str());
                        break; // Don't bother parsing this row.
                    }

                    colFields += 2;
                }
            }

        }

        if(is_flextable){
            // Make sure our output column is clean (especially in the case of previously-rejected rows)
            for (size_t realColNum = 0; realColNum < writer->getNumCols(); realColNum++) {
                if (!(parsedRealCols[realColNum] || (0 == strcmp(colInfo.getColumnName(realColNum).c_str(), RAW_COLUMN_NAME)))){
                    writer->setNull(realColNum);
                }
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

std::string FlexTableCsvParser::removeEscapedChars(std::string field,
                                                   size_t indexField) {
    //if the field has escape characters to remove
    if (ignoreEscapedCharPosition.size() > indexField) {
        if (field.find_first_not_of(enclosed_by) == std::string::npos) {
            //we only have a field with quotes
            field.erase(0, (field.size() / 2));
        } else {
            for (size_t j = 0; j < ignoreEscapedCharPosition.at(indexField).size(); j++) {
                if (std::find(fieldsStartingAndFinishWithQuotes.begin(),
                              fieldsStartingAndFinishWithQuotes.end(), indexField)
                    != fieldsStartingAndFinishWithQuotes.end()) {
                    //its a field that is enclosed_by

                    if (ignoreEscapedCharPosition.at(indexField).at(j) <= 0) {
                        field.erase(0, 1);
                    } else {
                        if( field.size() > ignoreEscapedCharPosition.at(indexField).at(j)
                            - j - 1 )
                            field.erase(
                                ignoreEscapedCharPosition.at(indexField).at(j)- j - 1, 1);
                    }
                } else {
                    //not enclosed_by
                    if( field.size() > ignoreEscapedCharPosition.at(indexField).at(j) - j)
                        field.erase(
                            ignoreEscapedCharPosition.at(indexField).at(j) - j, 1);
                }
            }
        }
    }
    return field;
}

void FlexTableCsvParser::initialize(ServerInterface &srvInterface,
                                    SizedColumnTypes &returnType) {
    colInfo = returnType;
    aux_buf.initialize(&srvInterface, BASE_RESERVE_SIZE);

    if (formatStrings.size() != returnType.getColumnCount()) {
        formatStrings.resize(returnType.getColumnCount(), "");
    }

    for (uint32 colNum = 0; colNum < colInfo.getColumnCount(); colNum++) {
        const std::string &str = colInfo.getColumnName(colNum);
        aux_buf.allocate(str.size() + 1);
        normalize_key(str.c_str(), str.size(), aux_buf.loc);
        real_col_lookup[ImmutableStringPtr(aux_buf.loc, str.size())] = colNum;
        aux_buf.loc += str.size() + 1;
    }

    // Find the flextable __raw__ column
    for (uint32 colNum = 0; colNum < colInfo.getColumnCount(); colNum++) {
        if ((0
             == strcmp(colInfo.getColumnName(colNum).c_str(),
                       RAW_COLUMN_NAME))
            && colInfo.getColumnType(colNum).isStringType()) {
            map_col_index = colNum;
            map_data_buf_len = colInfo.getColumnType(colNum).getStringLength();
            break;
        }
    }

    if (map_data_buf_len > 0) {
        //it is a flex table
        map_data_buf = (char*) srvInterface.allocator->alloc(map_data_buf_len);
        pair_buf = (char*) srvInterface.allocator->alloc(map_data_buf_len);
        is_flextable = true;
    }
}

void FlexTableCsvParser::deinitialize(ServerInterface &srvInterface,
                                      SizedColumnTypes &returnType) {
}

// Gets allocator.

VTAllocator * FlexTableCsvParser::getAllocator() {
    return getServerInterface().allocator;
}

/// END class FlexTableCsvParser


/// BEGIN class FCsvParserFactory

void FCsvParserFactory::plan(ServerInterface &srvInterface,
                             PerColumnParamReader &perColumnParamReader, PlanContext &planCtxt) {
}

UDParser* FCsvParserFactory::prepare(ServerInterface &srvInterface,
                                     PerColumnParamReader &perColumnParamReader,
                                     PlanContext &planCtxt,
                                     const SizedColumnTypes &returnType) {
    // Defaults
    std::string delimiter(","), record_terminator("\n"), enclosed_by("\""),
        escape("\\");
    std::string standardType("rfc4180");
    std::vector<std::string> formatStrings;
    bool headerRow(true);
    bool omitEmptyKeys(false);
    bool shouldTrim(true);
    bool rejectOnDuplicate(false);
    bool rejectOnMaterializedTypeError(false);
    bool rejectOnEmptyKey(false);
    std::string NULLST("NULL");
    bool containsNULLparam(false);

    // Args
    ParamReader args(srvInterface.getParamReader());

    if (args.containsParameter("type")) {
        if (args.getStringRef("type").isNull()) {
            vt_report_error(2, "Invalid parameter type NULL");
        }
        standardType = args.getStringRef("type").str();
        //check for empty parameter
        if (standardType.empty()) {
            standardType = "rfc4180";
        } else {
            //not empty, trim parameter
            standardType = trim(standardType);
        }
        //check empty after trim
        if (standardType.empty()) {
            standardType = "rfc4180";
        } else {
            //lowercase parameter
            std::transform(standardType.begin(), standardType.end(),
                           standardType.begin(), (int(*)(int)) tolower);
        }

        if (standardType.compare("traditional") == 0) {
            if (args.containsParameter("delimiter")) {
                if (args.getStringRef("delimiter").isNull()) {
                    vt_report_error(2, "Invalid parameter delimiter NULL");
                }
                std::string delim = args.getStringRef("delimiter").str();
                if (!delim.empty()) {
                    delimiter = delim;
                }
                //if it is empty we will preserve the default one
            }
            if (args.containsParameter("record_terminator")) {
                if (args.getStringRef("record_terminator").isNull()) {
                    vt_report_error(2,
                                    "Invalid parameter record_terminator NULL");
                }
                std::string rec_term =
                    args.getStringRef("record_terminator").str();
                if (!rec_term.empty()) {
                    record_terminator = rec_term;
                }
                if (record_terminator.size() == 2) {
                    if (record_terminator.at(0) == '\\'
                        && record_terminator.at(1) == 'n') {
                        record_terminator = '\n';
                    }
                }
            }
            if (args.containsParameter("enclosed_by")) {
                if (args.getStringRef("enclosed_by").isNull()) {
                    vt_report_error(2, "Invalid parameter enclosed_by NULL");
                }
                std::string enc = args.getStringRef("enclosed_by").str();
                if (!enc.empty()) {
                    enclosed_by = enc;
                }
            }
            if (args.containsParameter("escape")) {
                if (args.getStringRef("escape").isNull()) {
                    vt_report_error(2, "Invalid parameter escape NULL");
                }
                std::string esc = args.getStringRef("escape").str();
                if (!esc.empty()) {
                    escape = esc;
                }
            }
        } else if (standardType.compare("rfc4180") != 0) {
            //not traditional or rfc4180
            vt_report_error(0,
                            "Invalid parameter type [%s]: 'rfc4180' or 'traditional'",
                            standardType.c_str());
        }
    }

    if (args.containsParameter("header")) {
        if (args.getBoolRef("header") == vbool_null) {
            vt_report_error(2, "Invalid parameter header NULL");
        }
        headerRow = args.getBoolRef("header");
    }
    if (args.containsParameter("trim")) {
        if (args.getBoolRef("trim") == vbool_null) {
            vt_report_error(2, "Invalid parameter trim NULL");
        }
        shouldTrim = args.getBoolRef("trim");
    }
    if (args.containsParameter("omit_empty_keys")) {
        if (args.getBoolRef("omit_empty_keys") == vbool_null) {
            vt_report_error(2, "Invalid parameter omit_empty_keys NULL");
        }
        omitEmptyKeys = args.getBoolRef("omit_empty_keys");
    }
    if (args.containsParameter("reject_on_duplicate")) {
        if (args.getBoolRef("reject_on_duplicate") == vbool_null) {
            vt_report_error(2, "Invalid parameter reject_on_duplicate NULL");
        }
        rejectOnDuplicate = args.getBoolRef("reject_on_duplicate");
    }
    if (args.containsParameter("reject_on_materialized_type_error")) {
        if (args.getBoolRef("reject_on_materialized_type_error") == vbool_null) {
            vt_report_error(2,
                            "Invalid parameter reject_on_materialized_type_error NULL");
        }
        rejectOnMaterializedTypeError = args.getBoolRef(
            "reject_on_materialized_type_error");
    }
    if (args.containsParameter("reject_on_empty_key")) {
        if (args.getBoolRef("reject_on_empty_key") == vbool_null) {
            vt_report_error(2, "Invalid parameter reject_on_empty_key NULL");
        }
        rejectOnEmptyKey = args.getBoolRef("reject_on_empty_key");
    }

    if (args.containsParameter("NULL")) {
        containsNULLparam = true;
        NULLST = args.getStringRef("NULL").str();
    }
    // Validate size
    if (delimiter.size() != 1) {
        vt_report_error(0, "Invalid delimiter [%s]: single character required",
                        delimiter.c_str());
    }
    if (record_terminator.size() != 1) {
        vt_report_error(0,
                        "Invalid record_terminator [%s]: single character required",
                        record_terminator.c_str());
    }
    if (enclosed_by.size() != 1) {
        vt_report_error(0,
                        "Invalid enclosed_by [%s]: single character required",
                        enclosed_by.c_str());
    }
    if (escape.size() != 1) {
        vt_report_error(0, "Invalid escape [%s]: single character required",
                        escape.c_str());
    }

    //Validate Escape sequences greater than dec 127
    if (int(delimiter[0]) < 0 || int(delimiter[0]) > 127) {
        vt_report_error(0, "Invalid delimiter [%s]: ", delimiter.c_str());
    }
    if (int(record_terminator[0]) < 0 || int(record_terminator[0]) > 127) {
        vt_report_error(0, "Invalid record_terminator [%s]: ",
                        record_terminator.c_str());
    }
    if (int(enclosed_by[0]) < 0 || int(enclosed_by[0]) > 127) {
        vt_report_error(0, "Invalid enclosed_by [%s]: ", enclosed_by.c_str());
    }
    if (int(escape[0]) < 0 || int(escape[0]) > 127) {
        vt_report_error(0, "Invalid escape [%s]: ", escape.c_str());
    }

    //Validate: parameters enclosed_by, record_terminator, escape and/or delimtier cant be the same
    if (delimiter.compare(record_terminator) == 0 || delimiter.compare(
            enclosed_by) == 0 || delimiter.compare(escape) == 0
        || record_terminator.compare(enclosed_by) == 0
        || record_terminator.compare(escape) == 0 || enclosed_by.compare(
            escape) == 0) {
        vt_report_error(
            1,
            "Invalid delimiter|enclosed_by|record_terminator|escape parameter, must be different from each other");
    }

    // Extract the "format" argument.
    // Default to the global setting, but let any per-column settings override for that column.
    if (args.containsParameter("format"))
        formatStrings.resize(returnType.getColumnCount(),
                             args.getStringRef("format").str());
    else
        formatStrings.resize(returnType.getColumnCount(), "");

    for (size_t i = 0; i < returnType.getColumnCount(); i++) {
        const std::string &cname(returnType.getColumnName(i));
        if (perColumnParamReader.containsColumn(cname)) {
            ParamReader &colArgs = perColumnParamReader.getColumnParamReader(
                cname);
            if (colArgs.containsParameter("format")) {
                formatStrings[i] = colArgs.getStringRef("format").str();
            }
        }
    }

    return vt_createFuncObj(srvInterface.allocator, FlexTableCsvParser,
                            delimiter[0], record_terminator[0], headerRow, formatStrings,
                            shouldTrim, omitEmptyKeys, rejectOnDuplicate,
                            rejectOnMaterializedTypeError, rejectOnEmptyKey, enclosed_by[0],
                            escape[0], standardType, NULLST, containsNULLparam);
}

void FCsvParserFactory::getParserReturnType(ServerInterface &srvInterface,
                                            PerColumnParamReader &perColumnParamReader,
                                            PlanContext &planCtxt,
                                            const SizedColumnTypes &argTypes,
                                            SizedColumnTypes &returnType) {
    returnType = argTypes;
}

void FCsvParserFactory::getParameterType(ServerInterface &srvInterface,
                                         SizedColumnTypes &parameterTypes) {
    parameterTypes.addVarchar(2, "delimiter");
    parameterTypes.addVarchar(256, "record_terminator");
    parameterTypes.addBool("header");
    parameterTypes.addVarchar(256, "format");
    parameterTypes.addBool("trim");
    parameterTypes.addBool("omit_empty_keys");
    parameterTypes.addBool("reject_on_duplicate");
    parameterTypes.addBool("reject_on_materialized_type_error");
    parameterTypes.addBool("reject_on_empty_key");
    parameterTypes.addVarchar(2, "enclosed_by");
    parameterTypes.addVarchar(2, "escape");
    parameterTypes.addVarchar(256, "type");
    parameterTypes.addVarchar(256, "NULL");

}
/// END class FCsvParserFactory

// Register the parser factory
RegisterFactory( FCsvParserFactory);
} /// END namespace flextable
