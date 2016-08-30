/**
 * Copyright (c) 2005 - 2015 Hewlett Packard Enterprise Development LP
 *
 * Creation Date: December 10, 2013
 */
/**
 * Description: Top-level UDl plugin for json parser.
 *              Also uses JSONParseStack and JSONParseHandlers
 */


#include <iterator>

#include "JSONParser.h"
#include "DelimitedRecordChunker.h"

// 'start_point' parameter length
#define FJSON_STARTPOINT_PARAM_LEN 65000

namespace flextable
{


///BEGIN class JSONParser
JSONParser::JSONParser(vbool flattenMaps,
                       vbool flattenArrays,
                       vbool omitEmptyKeys,
                       std::string startPoint,
                       int spo,
                       vbool rejectOnDuplicate,
                       vbool rejectOnMaterializedTypeError,
                       vbool rejectOnEmptyKey,
                       vbool alphanumKeys,
                       std::string sep,
		       vbool enforceLength,
                       bool skipRecordAfterYajlError, char recordTerminator)
    : colInfo(), sp(), real_col_lookup(), map_col_index(-1), map_data_buf(NULL), map_data_buf_len(0), tuple_buf(NULL), aux_buf(), parse_context(NULL),
      flatten_maps(flattenMaps), flatten_arrays(flattenArrays), omit_empty_keys(omitEmptyKeys),
      start_point(startPoint), start_point_occurrence(spo), reject_on_duplicate(rejectOnDuplicate),
      reject_on_materialized_type_error(rejectOnMaterializedTypeError),
      reject_on_empty_key(rejectOnEmptyKey), suppress_nonalphanumeric_key_chars(alphanumKeys),
      separator(sep),enforcelength(enforceLength), skipRecordAfterYajlError(skipRecordAfterYajlError), recordTerminator(recordTerminator)
{ }

/// Delete any char*'s constructucted during setup()
JSONParser::~JSONParser()
{ }

void JSONParser::cleanup() {
    if (parse_context) {
        if (parse_context->yajl_hand) {
            yajl_free(parse_context->yajl_hand);
        }
        parse_context->yajl_hand == NULL;
        delete parse_context;
    }
    parse_context = NULL;
}


/// BEGIN Map Emitters
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
bool JSONParser::handle_field(size_t colNum, char* start, size_t len, bool hasPadding) {
    // Empty colums are null.
    if (len == 0) {
        writer->setNull(colNum);
        return true;
    } else {
        const Vertica::VerticaType &destColType = colInfo.getColumnType(colNum);
        ssize_t maxDataLen = len;
        if (destColType.isStringType()) {
	    ssize_t destColLength = (ssize_t)destColType.getStringLength();
	    if(enforcelength && (destColLength >= 0 && len > (size_t)destColLength)){
                reject_enforce_length = true;
                return false;
            }
            maxDataLen = std::min(destColLength, maxDataLen);
        }

        NullTerminatedString str(start, maxDataLen, false, hasPadding);
        try {
            return parseStringToType(str.ptr(), str.size(), colNum, colInfo.getColumnType(colNum), writer, sp);
        } catch(std::exception& e) {
            log("Exception while processing partition: [%s] while type converting JSON materialized column #[%lu] in row [%u], skipping this materialized column for this row.", e.what(), colNum, parse_context->num_records);
        } catch (...) {
            log("Unknown exception while type converting JSON materialized column #[%lu] in row [%u], skipping this materialized column for for this row.", colNum, parse_context->num_records);
        }
    }

    writer->setNull(colNum);
    return false;
}

/*
* Generates map data and emits a row in the target table containing it including potentially applying to real columns.
*/
int JSONParser::handle_record(void* _this, VMapPairWriter &map_writer) {
    return static_cast<JSONParser*>(_this)->handle_record_impl(map_writer);
}
int JSONParser::handle_record_impl(VMapPairWriter &map_writer) {
    if (parse_context->reject_row) {
        log("Reached end of record to be rejected in row [%u], not emitting.", parse_context->num_records);
        return false;
    } else if ((!parse_context->start_point.empty()) && parse_context->start_point_depth == 0) {
        vt_report_error(0, "Corrupt JSON file: start_point: ['%s'] not found with any nested objects.", parse_context->start_point.c_str());
        return false;
    } else {
        try {
            VMapPairReader map_reader(map_writer);

            // Validate desired case sensitivity/duplicate name behavior
            std::vector<VMapPair> &vpairs = map_reader.get_pairs();
            if (parse_context->reject_on_duplicate) {
                std::set<std::string, VMapBlockWriter::StringInsensitiveCompare> caseInsensitiveSet =
                        std::set<std::string, VMapBlockWriter::StringInsensitiveCompare>();
                for (size_t virtualColNum = 0; virtualColNum < vpairs.size(); virtualColNum++) {
                    std::pair<std::set<std::string>::iterator, bool> insertResult =
                            caseInsensitiveSet.insert(std::string(vpairs[virtualColNum].key_str(),
                                                      vpairs[virtualColNum].key_length()));

                    if (!insertResult.second) {
                        parse_context->reject_row = true;
                        std::stringstream rejectMessage;
                        rejectMessage << "Processed a row [" << (parse_context->num_records) << "] with duplicate keys with reject_on_duplicate specified; rejecting.";
                        if (parse_context->reject_reason.empty()) {
                            parse_context->reject_reason = rejectMessage.str();
                        }
                        LogDebugUDBasic("%s", rejectMessage.str().c_str());
                        return false;
                    }
                }
            }

            // Capture dump-column data
            if (map_col_index != -1) {
                size_t total_offset = construct_map(*parse_context->srvInterface, map_reader);
                if (total_offset == 0) {
                    parse_context->reject_row = true;
                    std::stringstream rejectMessage;
                    rejectMessage << "No data written for row [" << (parse_context->num_records) << "].";
                    if (parse_context->reject_reason.empty()) {
                        parse_context->reject_reason = rejectMessage.str();
                    }
                    log("%s", rejectMessage.str().c_str());
                    return false;
                } else {
                    VString& map_data_col = writer->getStringRef(map_col_index);
                    map_data_col.copy(map_data_buf, total_offset);
                }
            }

            // Process any materialized columns
            std::vector<bool> parsedRealCols(colInfo.getColumnCount()); // XXX TODO XXX Switch to boost::dynamic_bitset<> when available to link
            for (size_t virtualColNum = 0; virtualColNum < vpairs.size(); virtualColNum++) {
                try {
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
                            parse_context->reject_row = true;
                            normalize_key(vpairs[virtualColNum].key_str(), keyLen, aux_buf.loc);
                            std::stringstream rejectMessage;
			    if(reject_enforce_length){
				const Vertica::VerticaType &destColType = colInfo.getColumnType(it->second);
				int destColLength = destColType.getStringLength();
				rejectMessage << "Row [" << (parse_context->num_records) << "] rejected due to enforce_length error. Value length [" << valueLen << "] exceeds [" << std::string(aux_buf.loc, keyLen) << "] column size ["<< destColLength <<"]";
                                parse_context->reject_reason = rejectMessage.str();
                                log("%s", rejectMessage.str().c_str());
                                reject_enforce_length = false;
			    } else {
				parse_context->type_error = true;
			        rejectMessage << "Row [" << (parse_context->num_records) << "] rejected due to materialized type error on column: [" << std::string(aux_buf.loc, keyLen) << "]";
                                parse_context->reject_reason = rejectMessage.str();
                                log("%s", rejectMessage.str().c_str());
			    }
                            LogDebugUDBasic("%s", rejectMessage.str().c_str());
                            return false;
                        }
                    }
                } catch(std::exception& e) {
                    log("Exception while processing partition: [%s] while emitting JSON materialized virtual column #[%lu] in row [%u], skipping this materialized column for this row.", e.what(), virtualColNum, parse_context->num_records);
                } catch(...) {
                    log("Unknown exception while emitting JSON materialized virtual column #[%lu] in row [%u], skipping this materialized column for for this row.", virtualColNum, parse_context->num_records);
                }
            }

            // Make sure our output column is clean (especially in the case of previously-rejected rows)
            for (size_t realColNum = 0; realColNum < writer->getNumCols(); realColNum++) {
                if (!(parsedRealCols[realColNum] || (0 == strcmp(colInfo.getColumnName(realColNum).c_str(), RAW_COLUMN_NAME))))
                    writer->setNull(realColNum);
            }

            // Only move to the next entry on success
            writer->next();
            recordsAcceptedInBatch++;

        } catch(std::exception& e) {
            log("Exception while processing partition: [%s] while emitting JSON row [%u], skipping further data for this row.", e.what(), parse_context->num_records);
            return false;
        } catch(...) {
            log("Unknown exception while emitting JSON row [%u], skipping futher data for this row.", parse_context->num_records);
            return false;
        }
    }

    return !parse_context->reject_row;
}

/*
* Generates map data and sets its location and length.
*/
int JSONParser::handle_nested_map(void* _this, VMapPairWriter &map_writer, const char*& results_buf_loc, size_t & results_buf_size) {
    return static_cast<JSONParser*>(_this)->handle_nested_map_impl(map_writer, results_buf_loc, results_buf_size);
}
int JSONParser::handle_nested_map_impl(VMapPairWriter &map_writer, const char*& results_buf_loc, size_t & results_buf_size) {
    if (!parse_context->reject_row) {
        try {
            VMapPairReader map_reader(map_writer);
            // Capture dump-column data
            results_buf_size = construct_map(*parse_context->srvInterface, map_reader);
            results_buf_loc = map_data_buf;
        } catch(std::exception& e) {
            vt_report_error(0, "Exception while processing partition: [%s] while emitting JSON Flex Table sub-map in row [%u], skipping field.", e.what(), parse_context->num_records);
        } catch(...) {
            vt_report_error(0, "Unknown exception while emitting JSON Flex Table sub-map in row [%u], skipping field.", parse_context->num_records);
        }
    }

    return true;
}

/*
 * Resets the parsing context for when the start_point is found to ignore an undesired JSON wrapper.
 */
int JSONParser::handle_start_point(void* _this, ServerInterface &srvInterface) {
    return static_cast<JSONParser*>(_this)->handle_start_point_impl(srvInterface);
}

int JSONParser::handle_start_point_impl(ServerInterface &srvInterface) {
    return reset_parse_context(srvInterface);
}

/*
* Generates the Flex Table map type char* block.
*/
size_t JSONParser::construct_map(ServerInterface &srvInterface, VMapPairReader map_reader) {
    size_t total_offset = 0;

    // Capture dump-column data
    VMapBlockWriter::convert_vmap(srvInterface, map_reader,
                                 map_data_buf, map_data_buf_len, total_offset);

    return total_offset;
}
/// END Map Emitters

/// BEGIN Data Processors
/// BEGIN UDParser Overrides
StreamState JSONParser::process(ServerInterface &srvInterface, DataBuffer &input, InputState input_state) {
    // Will call back into us if/as data is available
    LogDebugUDBasic("{JSONParser.process} received: [%zu] new JSON Bytes to process with starting input_state: [%i].", input.size, input_state);
    if (input_state == Vertica::END_OF_FILE && input.size == 0) {
        return DONE;
    }

    yajl_status status = yajl_status_ok;
    input.offset = add_data((const unsigned char*)input.buf, input.size, srvInterface, status);
    LogDebugUDBasic("{JSONParser.process} parsed: [%zu] new JSON Bytes reported as: [%zu] out of [%zu] with ending input_state: [%i] and yajl_status: [%s].", yajl_get_bytes_consumed(parse_context->yajl_hand), input.offset, input.size, input_state, yajl_status_to_string(status));
    if (status == yajl_status_client_canceled ||
            ((input_state == Vertica::END_OF_FILE &&
             input.size == yajl_get_bytes_consumed(parse_context->yajl_hand)) &&
                 (parse_context->reject_row ||
                 parse_context->parse_stack.size() != 1))) {

        bool rejectionVsCancel = (status != yajl_status_client_canceled ||
                                 (status == yajl_status_client_canceled &&
                                      (!parse_context->reject_row)));

        // Finished a start_point parse
        if (rejectionVsCancel && (parse_context->start_point_depth== parse_context->curr_depth)) {
            return DONE;
        }

        if (status == yajl_status_client_canceled && parse_context->reject_row) {
            // A new YAJL handle needs to be constructed to continue parsing past the rejected
            //  row as YAJL does not support clearing previous errors and continuing to parse
            //  (even yajl_status_client_canceled)
            if (parse_context && parse_context->yajl_hand) {
                yajl_free(parse_context->yajl_hand);
            }
            JSONParseHandlers::RegisterCallbacks(parse_context);
        }

        parse_context->reject_row = false;
        parse_context->type_error = false;
        parse_context->reject_reason = "";

        if ((!rejectionVsCancel) || (input_state == Vertica::END_OF_FILE &&
                     input.size == yajl_get_bytes_consumed(parse_context->yajl_hand))) {
            LogDebugUDBasic("{JSONParser.process} row to be rejected now: [%i].", parse_context->num_records - 1);
            return REJECT;
        }
    }

    return ((input_state == END_OF_FILE) ? DONE : INPUT_NEEDED);
}

void JSONParser::setup(ServerInterface &srvInterface, SizedColumnTypes &returnType) {
    colInfo = returnType;
    aux_buf.initialize(&srvInterface, BASE_RESERVE_SIZE);

    // Need to re-create the parse context for each file being processed when loading
    //  multiple files from the same node as part of a multi-file COPY statement.
    delete parse_context;
    parse_context = NULL;
    parse_context = new JSONParseContext(handle_record, handle_nested_map, handle_start_point, this,
                                         flatten_maps, flatten_arrays, omit_empty_keys, start_point, start_point_occurrence, reject_on_duplicate,
                                         reject_on_materialized_type_error, reject_on_empty_key,
                                         !skipRecordAfterYajlError,
                                         suppress_nonalphanumeric_key_chars, separator);
    JSONParseHandlers::RegisterCallbacks(parse_context);

    reject_enforce_length = false;

    try {
        for (uint32 colNum = 0; colNum < colInfo.getColumnCount(); colNum++) {
            const std::string &str = colInfo.getColumnName(colNum);
            aux_buf.allocate(str.size() + 1);
            normalize_key(str.c_str(), str.size(), aux_buf.loc);
            real_col_lookup[ImmutableStringPtr(aux_buf.loc, str.size())] = colNum;
            aux_buf.loc += str.size() + 1;
        }

        for (uint32 colNum = 0; colNum < colInfo.getColumnCount(); colNum++) {
            if ((0 == strcmp(colInfo.getColumnName(colNum).c_str(), RAW_COLUMN_NAME)) && colInfo.getColumnType(colNum).isStringType()) {
                map_col_index = colNum;
                map_data_buf_len = colInfo.getColumnType(colNum).getStringLength();
                parse_context->set_push_to_raw_column(true);
                break;
            }
        }

        if (map_data_buf_len <= 0) {
            map_data_buf_len = MAP_FOOTPRINT;
        }
        map_data_buf = (char*)srvInterface.allocator->alloc(map_data_buf_len);
        parse_context->set_buf((char*)srvInterface.allocator->alloc(map_data_buf_len), map_data_buf_len, (char*)srvInterface.allocator->alloc(map_data_buf_len), map_data_buf_len);
    } catch(std::exception& e) {
        vt_report_error(0, "Exception setting up parsing while processing partition: [%s].", e.what());
    } catch(...) {
        vt_report_error(0, "Unknown exception setting up parsing.");
    }
}

int JSONParser::reset_parse_context(ServerInterface &srvInterface) {
    if (parse_context) {
        parse_context->reset_context();
    } else {
        parse_context = (new JSONParseContext(handle_record, handle_nested_map, handle_start_point, this,
                                              flatten_maps, flatten_arrays, omit_empty_keys, start_point, start_point_occurrence, reject_on_duplicate,
                                              reject_on_materialized_type_error, reject_on_empty_key, 
                                              !skipRecordAfterYajlError,
                                              suppress_nonalphanumeric_key_chars, separator));
        JSONParseHandlers::RegisterCallbacks(parse_context);

        try {
            if (map_data_buf_len <= 0) {
                map_data_buf_len = MAP_FOOTPRINT;
                map_data_buf = (char*)srvInterface.allocator->alloc(map_data_buf_len);
                parse_context->set_buf((char*)srvInterface.allocator->alloc(map_data_buf_len), map_data_buf_len, (char*)srvInterface.allocator->alloc(map_data_buf_len), map_data_buf_len);
            }
        } catch(std::exception& e) {
            vt_report_error(0, "Exception resetting parse context buffers while processing partition: [%s].", e.what());
        } catch(...) {
            vt_report_error(0, "Unknown exception resetting parse context buffers.");
        }
    }

    return true;
}

void JSONParser::destroy(ServerInterface &srvInterface, SizedColumnTypes &returnType) {
    cleanup();
}
/// END UDParser Overrides

size_t JSONParser::skipCurrentRecord(const unsigned char *&data, size_t &len) {
    size_t skippedBytes = 0;
    if (len && *data != recordTerminator) {
        // Skip remaining bytes until we get to the next record
        size_t currLen = len;
        do {
            data++;
            currLen--;
        } while (currLen && *data != recordTerminator);

        // Update skipped bytes and buffer length
        skippedBytes += (len - currLen);
        len = currLen;
    }
    if (len) {
        // Move data to the potential next record
        data++;
        len--;
        skippedBytes++;
    }
    return skippedBytes;
}

/*
* Entry point for COPY commands when new data is ready to be parsed.
*/
uint JSONParser::add_data(const unsigned char* data, size_t len, ServerInterface &srvInterface, yajl_status &status) {

    size_t bytesConsumedTotal = 0; // total bytes consumed
    try {
        parse_context->srvInterface = &srvInterface;

        const size_t totalLen = len;
        do {
            bool skipRecord = false;
            status = yajl_parse(parse_context->yajl_hand, data, len);
            const size_t bytesConsumed = yajl_get_bytes_consumed(parse_context->yajl_hand);
            bytesConsumedTotal += bytesConsumed;

            // Check for cancel from reaching the end of the chosen start_point's children
            if (status == yajl_status_client_canceled &&
                parse_context->start_point_depth > 0 &&
                parse_context->start_point_depth == parse_context->curr_depth) {
                return totalLen; // Finished parse under start_point; ignore the rest

            } else if (status == yajl_status_client_canceled) {
                YajlFreeErrorWrapper yfe(parse_context->yajl_hand, yajl_get_error(parse_context->yajl_hand, 1, data, len));
                LogDebugUDWarn("Non-YAJL parse cancel by the JSON parser on record # [%i] with status: [%s] and Bytes consumed: [%zu]: [%s]", parse_context->num_records - 1, yajl_status_to_string(status), yajl_get_bytes_consumed(parse_context->yajl_hand), yfe.getStr());

            } else if (status != yajl_status_ok) {
                YajlFreeErrorWrapper yfe(parse_context->yajl_hand, yajl_get_error(parse_context->yajl_hand, 1, data, len));
                if (!skipRecordAfterYajlError) {
                    vt_report_error(0, "Unrecoverable YAJL Error on record # [%i] with status: [%s] and [%lu] Bytes consumed: [%s]", parse_context->num_records - 1, yajl_status_to_string(status), yajl_get_bytes_consumed(parse_context->yajl_hand), yfe.getStr());
                } else {
                    // Skip this JSON record
                    skipRecord = true;

                    std::stringstream rejectMessage;
                    rejectMessage << "Malformed record found [" << yfe.getStr()
                                  << "] with record_terminator specified; rejecting";
                    LogDebugUDBasic("%s", rejectMessage.str().c_str());
                }
            }

            data += bytesConsumed;
            len -= bytesConsumed;

            if (skipRecord) {
                // Continue parsing from the next JSON record
                bytesConsumedTotal += skipCurrentRecord(data, len);

                // There doesn't seem to be a way to reset the internal state 
                // of the Yajl parse handle and keep parsing
                // Release current parse handle now, and allocate a new one
                if (parse_context->yajl_hand) {
                    yajl_free(parse_context->yajl_hand);
                    parse_context->yajl_hand = NULL;
                }
                // Allocate a new parse handle to resume parsing
                JSONParseHandlers::RegisterCallbacks(parse_context);
                parse_context->reset_context();
            }
        } while (skipRecordAfterYajlError && len);
    } catch(std::exception& e) {
        vt_report_error(0, "Unrecoverable parse exception while processing partition: [%s] while parsing JSON data for row [%i] with [%u] Bytes consumed.", e.what(), parse_context->num_records - 1, yajl_get_bytes_consumed(parse_context->yajl_hand));
    } catch(...) {
        vt_report_error(0, "Unknown unrecoverable parse exception while parsing JSON data for row [%u] with [%i] Bytes consumed.", parse_context->num_records - 1, yajl_get_bytes_consumed(parse_context->yajl_hand));
    }

    return bytesConsumedTotal;
}
/// END Data Processors

/// END class JSONParser



/// BEGIN class FJSONParserFactory
void FJSONParserFactory::plan(ServerInterface &srvInterface,
        PerColumnParamReader &perColumnParamReader,
        PlanContext &planCtxt) {
    /* Check parameters */
// XXX TODO XXX Validate parameters here, for a bit more cleanliness
//    ParamReader args(srvInterface.getParamReader());


    /* Populate planData */
    // Nothing to do here
}

UDParser* FJSONParserFactory::prepare(ServerInterface &srvInterface,
        PerColumnParamReader &perColumnParamReader,
        PlanContext &planCtxt,
        const SizedColumnTypes &returnType)
{
    // Args
    ParamReader args(srvInterface.getParamReader());
    vbool flatten_maps = vbool_null;
    if (args.containsParameter("flatten_maps")) {
        flatten_maps = args.getBoolRef("flatten_maps");
    }
    if (flatten_maps == vbool_null) {
        flatten_maps = vbool_true;
    }
    vbool flatten_arrays = vbool_null;
    if (args.containsParameter("flatten_arrays")) {
        flatten_arrays = args.getBoolRef("flatten_arrays");
    }
    if (flatten_arrays == vbool_null) {
        flatten_arrays = vbool_false;
    }
    vbool omit_empty_keys = vbool_null;
    if (args.containsParameter("omit_empty_keys")) {
        omit_empty_keys = args.getBoolRef("omit_empty_keys");
    }
    if (omit_empty_keys == vbool_null) {
        omit_empty_keys = vbool_false;
    }

    std::string start_point;
    if (args.containsParameter("start_point")) {
        start_point = args.getStringRef("start_point").str();
    }

    int start_point_occurrence = 1;
    if (args.containsParameter("start_point_occurrence")) {
        start_point_occurrence = args.getIntRef("start_point_occurrence");
    }

    vbool reject_on_duplicate = vbool_null;
    if (args.containsParameter("reject_on_duplicate")) {
        reject_on_duplicate = args.getBoolRef("reject_on_duplicate");
    }
    if (reject_on_duplicate == vbool_null) {
        reject_on_duplicate = vbool_false;
    }
    vbool reject_on_materialized_type_error = vbool_null;
    if (args.containsParameter("reject_on_materialized_type_error")) {
        reject_on_materialized_type_error = args.getBoolRef("reject_on_materialized_type_error");
    }
    if (reject_on_materialized_type_error == vbool_null) {
        reject_on_materialized_type_error = vbool_false;
    }
    vbool reject_on_empty_key = vbool_null;
    if (args.containsParameter("reject_on_empty_key")) {
        reject_on_empty_key = args.getBoolRef("reject_on_empty_key");
    }
    if (reject_on_empty_key == vbool_null) {
        reject_on_empty_key = vbool_false;
    }
    vbool suppress_nonalphanumeric_key_chars = vbool_null;
    if (args.containsParameter("suppress_nonalphanumeric_key_chars")) {
        suppress_nonalphanumeric_key_chars = args.getBoolRef("suppress_nonalphanumeric_key_chars");
    }
    if (suppress_nonalphanumeric_key_chars == vbool_null) {
        suppress_nonalphanumeric_key_chars = vbool_false;
    }

    std::string separator = ".";
    if (args.containsParameter("key_separator")) {
        separator = args.getStringRef("key_separator").str();
        if (separator.length() != 1) separator = ".";
    }
    
    vbool enforcelength = vbool_null;
    if (args.containsParameter("enforce_length")) {
        enforcelength = args.getBoolRef("enforce_length");
    }
    if (enforcelength == vbool_null) {
        enforcelength = vbool_false;
    }

    bool skipRecordAfterYajlError = false;
    char recordTerminator = 0;
    if (args.containsParameter("record_terminator")) {
        skipRecordAfterYajlError = true;
        recordTerminator = args.getStringRef("record_terminator").str()[0];
    }
 
    LogDebugUDBasic("{FJSONParserFactory.prepare} Passed flatten_maps: [%s], flatten_arrays: [%s], omit_empty_keys: [%s], start_point: [%s: %d], reject_on_duplicate [%s], reject_on_materialized_type_error: [%s], reject_on_empty_key: [%s], enforcelength: [%s], record_terminator: [%s] ", toString((VBool) flatten_maps).c_str(), toString((VBool) flatten_arrays).c_str(), toString((VBool) omit_empty_keys).c_str(), start_point.c_str(), start_point_occurrence, toString((VBool) reject_on_duplicate).c_str(), toString((VBool) reject_on_materialized_type_error).c_str(), toString((VBool) reject_on_empty_key).c_str(), toString((VBool) enforcelength).c_str(), (skipRecordAfterYajlError) ? "Yes" : "No");

    return vt_createFuncObj(srvInterface.allocator, JSONParser,
                            flatten_maps, flatten_arrays, omit_empty_keys,
                            start_point, start_point_occurrence, reject_on_duplicate,
                            reject_on_materialized_type_error, reject_on_empty_key,
                            suppress_nonalphanumeric_key_chars, separator, enforcelength,
                            skipRecordAfterYajlError, recordTerminator);
}

UDChunker* FJSONParserFactory::prepareChunker(ServerInterface &srvInterface,
                                              PerColumnParamReader &perColumnParamReader,
                                              PlanContext &planCtxt,
                                              const SizedColumnTypes &returnType) {
    ParamReader params(srvInterface.getParamReader());
    bool skipRecordAfterYajlError = false;
    char recordTerminator = 0;
    if (params.containsParameter("record_terminator")) {
        skipRecordAfterYajlError = true;
        recordTerminator = params.getStringRef("record_terminator").str()[0];
    }

    if (skipRecordAfterYajlError) {
        // User has set a record terminator
        // Good. We can find record boundaries
        return vt_createFuncObject<DelimitedRecordChunker>(srvInterface.allocator,
                                                           recordTerminator);
    } else {
        // Too bad. We don't know how to find record boundaries
        return NULL;
    }
}

void FJSONParserFactory::getParserReturnType(ServerInterface &srvInterface,
        PerColumnParamReader &perColumnParamReader,
        PlanContext &planCtxt,
        const SizedColumnTypes &argTypes,
        SizedColumnTypes &returnType)
{
    returnType = argTypes;
}

void FJSONParserFactory::getParameterType(ServerInterface &srvInterface,
                                          SizedColumnTypes &parameterTypes) {
    parameterTypes.addBool("flatten_maps");
    parameterTypes.addBool("flatten_arrays");
    parameterTypes.addVarchar(FJSON_STARTPOINT_PARAM_LEN, "start_point");
    parameterTypes.addInt("start_point_occurrence");
    parameterTypes.addBool("omit_empty_keys");
    parameterTypes.addBool("reject_on_duplicate");
    parameterTypes.addBool("reject_on_materialized_type_error");
    parameterTypes.addBool("reject_on_empty_key");
    parameterTypes.addBool("suppress_nonalphanumeric_key_chars");
    parameterTypes.addVarchar(1, "key_separator");
    parameterTypes.addBool("enforce_length");

    // Parameter to skip bad JSON records
    SizedColumnTypes::Properties props(true /*visible*/, false /*required*/,
                                       false /*canBeNull*/, "Single-character record terminator. When set, the parser skips erroneous JSON records and continues from the next record");
    parameterTypes.addVarchar(1, "record_terminator", props);
}
/// END class FJSONParserFactory



// Register the parser factory
RegisterFactory(FJSONParserFactory);




/// BEGIN class JSONExtractor
JSONExtractor::JSONExtractor(vbool flattenMaps,
                             vbool flattenArrays,
                             vbool omitEmptyKeys,
                             std::string startPoint,
                             vbool rejectOnDuplicate,
                             vbool rejectOnEmptyKey,
                             std::string rejectAction,
                             std::string errorAction,
			     vbool enforceLength)
    : map_data_buf(NULL), map_data_buf_len(0), tuple_buf(NULL),
      parse_context(NULL),
      flatten_maps(flattenMaps), flatten_arrays(flattenArrays), omit_empty_keys(omitEmptyKeys),
      start_point(startPoint), reject_on_duplicate(rejectOnDuplicate),
      reject_on_empty_key(rejectOnEmptyKey),
      reject_action(parse_failure_action(rejectAction)), error_action(parse_failure_action(errorAction)),
      enforcelength(enforceLength), warnings_buf(NULL), warnings_buf_len(0) {
}

/// Faster to linearly search in such a small enum set than incur the overhead of generating a map lookup
FailAction JSONExtractor::parse_failure_action(std::string failAction) {
    for (int currFailActionNum = 0; currFailActionNum < NUM_FAILACTIONS; currFailActionNum++) {
        if (strcasecmp(failAction.c_str(), FailAction_STRINGS[currFailActionNum]) == 0) {
            return ((FailAction)currFailActionNum);
        }
    }

    // XXX TODO XXX Add this back once VER-32893 is resolved and vt_report_error() here doesn't crash Vertica any more
//    // Passed an invalid failure action
//    std::stringstream availableFailureActions;
//    availableFailureActions << FailAction_STRINGS[0];
//    for (int currFailActionNum = 1; currFailActionNum < NUM_FAILACTIONS; currFailActionNum++) {
//        availableFailureActions << ", " << FailAction_STRINGS[currFailActionNum];
//    }
//    vt_report_error(0, "Invalid failure action: [%s]. The valid failure actions are: [%s].", failAction.c_str(), GetAvailableFailActionsString().c_str());
    log("Invalid failure action: [%s]. The valid failure actions are: [%s].", failAction.c_str(), GetAvailableFailActionsString().c_str());    // XXX TODO XXX And remove this when the vt_report_error() is working
    return ((FailAction)-1);  // To remove the warning; this will always get skipped due to vt_report_error()'s Exception throw
}

JSONExtractor::~JSONExtractor() {
    cleanup();
}

void JSONExtractor::cleanup() {
    if (warnings_buf != NULL) {
        delete[] warnings_buf;
    }
    warnings_buf = NULL;

    if (parse_context) {
        if (parse_context->yajl_hand) {
            yajl_free(parse_context->yajl_hand);
        }
        parse_context->yajl_hand == NULL;
        delete parse_context;
    }
    parse_context = NULL;
}


/// BEGIN Map Emitters
/*
* Generates map data and emits a row in the target table containing it including potentially applying to real columns.
*/
int JSONExtractor::handle_record(void* _this, VMapPairWriter &map_writer) {
    return static_cast<JSONExtractor*>(_this)->handle_record_impl(map_writer);
}
int JSONExtractor::handle_record_impl(VMapPairWriter &map_writer) {
    if (parse_context->reject_row) {
        return false;
    } else if ((!parse_context->start_point.empty()) && parse_context->start_point_depth == 0) {
        PerformFailOp(error_action, "Corrupt JSON file: start_point: ['%s'] not found with any nested objects.", parse_context->start_point.c_str());
        return false;
    } else if (parse_context->num_rows_emitted > 0) {
        log("Tried to emit multiple maps into the same row [%u], not emitting.", parse_context->num_records);
        parse_context->reject_row = true;
        return false;
    } else {
        try {
            VMapPairReader map_reader(map_writer);

            // Validate desired case sensitivity/duplicate name behavior
            std::vector<VMapPair> &vpairs = map_reader.get_pairs();
            if (parse_context->reject_on_duplicate) {
                std::set<std::string, VMapBlockWriter::StringInsensitiveCompare> caseInsensitiveSet =
                        std::set<std::string, VMapBlockWriter::StringInsensitiveCompare>();
                for (size_t virtualColNum = 0; virtualColNum < vpairs.size(); virtualColNum++) {
                    std::pair<std::set<std::string>::iterator, bool> insertResult =
                            caseInsensitiveSet.insert(std::string(vpairs[virtualColNum].key_str(),
                                                      vpairs[virtualColNum].key_length()));

                    if (!insertResult.second) {
                        parse_context->reject_row = true;
                        std::stringstream rejectMessage;
                        rejectMessage << "Processed a row [" << (parse_context->num_records) << "] with duplicate keys with reject_on_duplicate specified; rejecting.";
                        if (parse_context->reject_reason.empty()) {
                            parse_context->reject_reason = rejectMessage.str();
                        }
                        LogDebugUDBasic("%s", rejectMessage.str().c_str());
                        return false;
                    }
                }
            }

            // Capture dump-column data
            size_t total_offset = construct_map(*parse_context->srvInterface, map_reader);
            if (total_offset == 0) {
                parse_context->reject_row = true;
                std::stringstream rejectMessage;
                rejectMessage << "No data written for row [" << (parse_context->num_records) << "].";
                if (parse_context->reject_reason.empty()) {
                    parse_context->reject_reason = rejectMessage.str();
                }
                log("%s", rejectMessage.str().c_str());
                return false;
            } else {
                VString& outvmap = parse_context->result_writer->getStringRef();
                outvmap.copy(map_data_buf, total_offset);
                parse_context->num_rows_emitted++;
            }

        } catch(std::exception& e) {
            log("Exception while processing partition: [%s] while emitting JSON row [%u], skipping further data for this row.", e.what(), parse_context->num_records);
            return false;
        } catch(...) {
            log("Unknown exception while emitting JSON row [%u], skipping futher data for this row.", parse_context->num_records);
            return false;
        }
    }

    return !parse_context->reject_row;
}

/*
* Generates map data and sets its location and length.
*/
int JSONExtractor::handle_nested_map(void* _this, VMapPairWriter &map_writer, const char*& results_buf_loc, size_t & results_buf_size) {
    return static_cast<JSONExtractor*>(_this)->handle_nested_map_impl(map_writer, results_buf_loc, results_buf_size);
}
int JSONExtractor::handle_nested_map_impl(VMapPairWriter &map_writer, const char*& results_buf_loc, size_t & results_buf_size) {
    if (!parse_context->reject_row) {
        try {
            VMapPairReader map_reader(map_writer);
            // Capture dump-column data
            results_buf_size = construct_map(*parse_context->srvInterface, map_reader);
            results_buf_loc = map_data_buf;
        } catch(std::exception& e) {
            PerformFailOp(error_action, "Exception while processing partition: [%s] while emitting JSON Flex Table sub-map in row [%u], skipping field.", e.what(), parse_context->num_records);
            return false;
        } catch(...) {
            PerformFailOp(error_action, "Unknown exception while emitting JSON Flex Table sub-map in row [%u], skipping field.", parse_context->num_records);
            return false;
        }
    }

    return true;
}

/*
 * Resets the parsing context for when the start_point is found to ignore an undesired JSON wrapper.
 */
int JSONExtractor::handle_start_point(void* _this, ServerInterface &srvInterface) {
    return static_cast<JSONExtractor*>(_this)->handle_start_point_impl(srvInterface);
}

int JSONExtractor::handle_start_point_impl(ServerInterface &srvInterface) {
    return reset_parse_context(srvInterface);
}


/*
* Generates the Flex Table map type char* block.
*/
size_t JSONExtractor::construct_map(ServerInterface &srvInterface, VMapPairReader map_reader) {
    size_t total_offset = 0;

    // Capture dump-column data
    VMapBlockWriter::convert_vmap(srvInterface, map_reader,
                                 map_data_buf, map_data_buf_len, total_offset);

    return total_offset;
}
/// END Map Emitters

/// BEGIN Data Processors
/// BEGIN ScalarFunction Overrides
void JSONExtractor::processBlock(ServerInterface &srvInterface, BlockReader &argReader, BlockWriter &resultWriter) {

    uint64 rowNum = 0;
    do {
        rowNum++;
        uint64 dataConsumedThisPass = 0;
        uint64 dataConsumedTot = 0;
        const VString& inputRecord = argReader.getStringRef(0);
        VString& outvmap = resultWriter.getStringRef();
        parse_context->result_writer = &resultWriter;
        parse_context->num_rows_emitted = 0;
        try {
            LogDebugUDBasic("{JSONExtractor.processBlock} received: [%d] new JSON Bytes to process.", inputRecord.length());
            if (inputRecord.length() == 0 || inputRecord.isNull()) {
                outvmap.setNull();
            } else {
                yajl_status status = yajl_status_ok;
                do {
                    dataConsumedTot += dataConsumedThisPass = add_data((const unsigned char*)inputRecord.data(), inputRecord.length(), srvInterface, status);

                    LogDebugUDBasic("{JSONExtractor.processBlock} parsed: [%zu] new JSON Bytes reported as: [%llu] out of [%llu] or: [%d], yajl_status: [%s] and reject_row: [%d].", yajl_get_bytes_consumed(parse_context->yajl_hand), dataConsumedTot, dataConsumedThisPass, inputRecord.length(), yajl_status_to_string(status), parse_context->reject_row);

                } while (status != yajl_status_error &&
                            (!parse_context->reject_row) &&
                            (parse_context->parse_stack.size() == 1) &&
                            dataConsumedTot < inputRecord.length());

                if (status == yajl_status_client_canceled && (!parse_context->reject_row)) {
                    LogDebugUDBasic("Manual JSON parser cancel to emit row: [%u].", parse_context->num_records);
                } else if (status == yajl_status_error || parse_context->reject_row) {
                    outvmap.setNull();
                    PerformFailOp(reject_action, "Rejecting row [%u].", parse_context->num_records);
                } else if (parse_context->num_rows_emitted > 1 ||
                        (parse_context->num_rows_emitted == 1 && dataConsumedTot < inputRecord.length())) {
                    outvmap.setNull();
                    PerformFailOp(reject_action, "Found multiple maps into the same row: [%u], not emitting.", parse_context->num_records);
                } else if (parse_context->parse_stack.size() != 1) {
                    PerformFailOp(error_action, "Unrecoverable JSON parse stack consistency error on record # [%i] with status: [%s] and [%llu] Bytes consumed", parse_context->num_records - 1, yajl_status_to_string(status), dataConsumedTot);
                    outvmap.setNull();
                } else if (dataConsumedTot == inputRecord.length()) {
                    if (parse_context->num_rows_emitted < 1) {
                        // Didn't output any VMap rows, set output to NULL
                        outvmap.setNull();
                        PerformFailOp(reject_action, "No data emitted for the row: [%u].", parse_context->num_records);
                    }
                }


                parse_context->reject_row = false;
                parse_context->reject_reason = "";

                if (parse_context && parse_context->yajl_hand) {
                    yajl_free(parse_context->yajl_hand);
                }
                JSONParseHandlers::RegisterCallbacks(parse_context);
            }

        } catch(std::exception& e) {
            outvmap.setNull();
            LogDebugUDBasic("Exception while processing partition row: [%llu] on map JSON extractor: [%s]", rowNum, e.what());
            throw;
        } catch(...) {
            outvmap.setNull();
            LogDebugUDBasic("Unknown exception while processing partition row: [%llu] on map JSON extractor.", rowNum);
            throw;
        }

        resultWriter.next();

    } while (argReader.next());
}

void JSONExtractor::setup(ServerInterface &srvInterface, const SizedColumnTypes &argTypes) {
    // Validate these arguments now until VER-32893 is fixed (see comment in JSONExtractor::JSONExtractor()) then XXX DELETEME XXX
    if (reject_action < 0) {
        vt_report_error(0, "Invalid failure action for reject_action. The valid failure actions are: [%s].", GetAvailableFailActionsString().c_str());
    }
    if (error_action < 0) {
        vt_report_error(0, "Invalid failure action for error_action. The valid failure actions are: [%s].", GetAvailableFailActionsString().c_str());
    }

    // setup VMap buffers
    map_data_buf_len = estimateNeededSpaceForMap(argTypes.getColumnType(0).getStringLength(), flatten_maps, flatten_arrays);
    if (map_data_buf_len <= 0)
        map_data_buf_len = MAP_FOOTPRINT;
    map_data_buf = (char*)srvInterface.allocator->alloc(map_data_buf_len);

    parse_context = new JSONParseContext(handle_record, handle_nested_map, handle_start_point, this,
                                         flatten_maps, flatten_arrays, omit_empty_keys, start_point, 1, reject_on_duplicate,
                                         false /* reject_on_materialized_type_error */, reject_on_empty_key);
    JSONParseHandlers::RegisterCallbacks(parse_context);

    parse_context->set_buf((char*)srvInterface.allocator->alloc(map_data_buf_len), map_data_buf_len, (char*)srvInterface.allocator->alloc(map_data_buf_len), map_data_buf_len);
}

void JSONExtractor::destroy(ServerInterface &srvInterface, const SizedColumnTypes &returnType) {
    // Emit any accumulated Warnings
    if (warnings_buf != NULL && warnings_buf_len > 0) {
        vt_report_error(0, (std::string(warnings_buf, warnings_buf_len)).c_str());
    }

    cleanup();
}

int JSONExtractor::reset_parse_context(ServerInterface &srvInterface) {
    if (parse_context) {
        parse_context->reset_context();
    } else {
        parse_context = new JSONParseContext(handle_record, handle_nested_map, handle_start_point, this,
                      flatten_maps, flatten_arrays, omit_empty_keys, start_point, 1, reject_on_duplicate,
                      false /* reject_on_materialized_type_error */, reject_on_empty_key);
        JSONParseHandlers::RegisterCallbacks(parse_context);

        try {
            if (map_data_buf_len <= 0) {
                map_data_buf_len = MAP_FOOTPRINT;
                map_data_buf = (char*)srvInterface.allocator->alloc(map_data_buf_len);
                parse_context->set_buf((char*)srvInterface.allocator->alloc(map_data_buf_len), map_data_buf_len, (char*)srvInterface.allocator->alloc(map_data_buf_len), map_data_buf_len);
            }
        } catch(std::exception& e) {
            PerformFailOp(error_action, "Exception resetting parse context buffers while processing partition: [%s].", e.what());
            return false;
        } catch(...) {
            PerformFailOp(error_action, "Unknown exception resetting parse context buffers.");
            return false;
        }
    }

    return true;
}


// Depending on the JSON's structure, uncompressed VMaps can take more space than
// the original JSON. Likewise, if maps or arrays aren't flattened, more space
// will be needed to construct those nested maps then place them as values in
// their parent maps.
inline size_t JSONExtractor::estimateNeededSpaceForMap(size_t inputJSONSize, bool flattenMaps, bool flattenArrays) {
    return (2 * ((double)inputJSONSize)) + (2 * (!flattenMaps) * ((double)inputJSONSize)) + (2 * (!flattenArrays) * ((double)inputJSONSize)); // Estimate better over time
}
/// END ScalarFunction Overrides

/*
* Entry point for COPY commands when new data is ready to be parsed.
*/
uint JSONExtractor::add_data(const unsigned char* data, size_t len, ServerInterface &srvInterface, yajl_status &status) {

//std::cout << "{JSONExtractor.add_data} received: " << len << " new JSON Bytes to process" << std::endl;
//    LogDebugUDBasic("{JSONExtractor.add_data} received: [%zu] new JSON Bytes to process.", len);

    size_t yajlBytesConsumed = 0;
    try {
        parse_context->srvInterface = &srvInterface;
        status = yajl_parse(parse_context->yajl_hand, data, len);
        yajlBytesConsumed += yajl_get_bytes_consumed(parse_context->yajl_hand);

        // Make sure callbacks get called which don't fire without whitespace
        //  indicating end-of-data (as YAJL expects it just received a partial
        //  record it will see more of)
        if (status == yajl_status_ok) {
            status = yajl_parse(parse_context->yajl_hand, ((const unsigned char*)"\n"), 1);
        }

        // Check for cancel from reaching the end of the chosen start_point's children
        if (status == yajl_status_client_canceled &&
                parse_context->start_point_depth > 0 &&
                parse_context->start_point_depth == parse_context->curr_depth) {
            return len; // Finished parse under start_point; ignore the rest

        } else if (status == yajl_status_client_canceled) {
            YajlFreeErrorWrapper yfe(parse_context->yajl_hand, yajl_get_error(parse_context->yajl_hand, 1, data, len));
                LogDebugUDWarn("Non-YAJL parse cancel by the JSON parser on record # [%i] with status: [%s], reject_row: [%d], and Bytes consumed: [%zu]: [%s]", parse_context->num_records - 1, yajl_status_to_string(status), parse_context->reject_row, yajlBytesConsumed, yfe.getStr());

        } else if (status != yajl_status_ok) {
            YajlFreeErrorWrapper yfe(parse_context->yajl_hand, yajl_get_error(parse_context->yajl_hand, 1, data, len));
            vt_report_error(0, "Unrecoverable YAJL Error on record # [%i] with status: [%s] and [%lu] Bytes consumed: [%s]", parse_context->num_records - 1, yajl_status_to_string(status), yajlBytesConsumed, yfe.getStr());
        }

    } catch(std::exception& e) {
        PerformFailOp(error_action, "Unrecoverable parse exception while processing partition: [%s] while parsing JSON data for row [%i] with [%zu] Bytes consumed.", e.what(), parse_context->num_records - 1, yajlBytesConsumed);
        return len;
    } catch(...) {
        PerformFailOp(error_action, "Unknown unrecoverable parse exception while parsing JSON data for row [%u] with [%zu] Bytes consumed.", parse_context->num_records - 1, yajlBytesConsumed);
        return len;
    }

    if (parse_context->num_rows_emitted < 1 && (!parse_context->parse_stack.empty())) {
        PerformFailOp(error_action, "Un-closed JSON on record # [%i] with status: [%s] and [%lu] Bytes consumed", parse_context->num_records - 1, yajl_status_to_string(status), yajlBytesConsumed);
        return len;
    }
    return yajlBytesConsumed;
}
/// END Data Processors

/// END class JSONExtractor



/// BEGIN class FJSONExtractorFactory
FJSONExtractorFactory::FJSONExtractorFactory() {
    vol = IMMUTABLE;
}

void FJSONExtractorFactory::getPrototype(ServerInterface &srvInterface,
                                         ColumnTypes &argTypes,
                                         ColumnTypes &returnType)
{
    argTypes.addLongVarchar();
    returnType.addLongVarbinary();
}

void FJSONExtractorFactory::getReturnType(ServerInterface &srvInterface,
                                          const SizedColumnTypes &input_types,
                                          SizedColumnTypes &output_types)
{
    output_types.addLongVarbinary(VALUE_FOOTPRINT, MAP_VALUE_OUTPUT_NAME);
}

ScalarFunction* FJSONExtractorFactory::createScalarFunction(ServerInterface &srvInterface)
{
    // Args
    ParamReader args(srvInterface.getParamReader());
    vbool flattenMaps = vbool_null;
    if (args.containsParameter("flatten_maps")) {
        flattenMaps = args.getBoolRef("flatten_maps");
    }
    if (flattenMaps == vbool_null) {
        flattenMaps = vbool_true;
    }
    vbool flattenArrays = vbool_null;
    if (args.containsParameter("flatten_arrays")) {
        flattenArrays = args.getBoolRef("flatten_arrays");
    }
    if (flattenArrays == vbool_null) {
        flattenArrays = vbool_false;
    }
    vbool omitEmptyKeys = vbool_null;
    if (args.containsParameter("omit_empty_keys")) {
        omitEmptyKeys = args.getBoolRef("omit_empty_keys");
    }
    if (omitEmptyKeys == vbool_null) {
        omitEmptyKeys = vbool_false;
    }

    std::string startPoint;
    if (args.containsParameter("start_point")) {
        startPoint = args.getStringRef("start_point").str();
    }

    vbool rejectOnDuplicate = vbool_null;
    if (args.containsParameter("reject_on_duplicate")) {
        rejectOnDuplicate = args.getBoolRef("reject_on_duplicate");
    }
    if (rejectOnDuplicate == vbool_null) {
        rejectOnDuplicate = vbool_false;
    }
    vbool rejectOnEmptyKey = vbool_null;
    if (args.containsParameter("reject_on_empty_key")) {
        rejectOnEmptyKey = args.getBoolRef("reject_on_empty_key");
    }
    if (rejectOnEmptyKey == vbool_null) {
        rejectOnEmptyKey = vbool_false;
    }

    std::string rejectAction = "Warn";
    if (args.containsParameter("reject_action")) {
        rejectAction = args.getStringRef("reject_action").str();
    }
    std::string errorAction = "Abort";
    if (args.containsParameter("error_action")) {
        errorAction = args.getStringRef("error_action").str();
    }

    vbool enforceLength = vbool_null;
    if (args.containsParameter("enforce_length")) {
        enforceLength = args.getBoolRef("enforce_length");
    }
    if (enforceLength == vbool_null) {
        enforceLength = vbool_false;
    }

    LogDebugUDBasic("{FJSONExtractorFactory.prepare} Passed flatten_maps: [%s], flatten_arrays: [%s], omit_empty_keys: [%s], start_point: [%s], reject_on_duplicate [%s], reject_on_empty_key: [%s], reject_action: [%s], error_action: [%s] and enforce_length: [%s].", toString((VBool) flattenMaps).c_str(), toString((VBool) flattenArrays).c_str(), toString((VBool) omitEmptyKeys).c_str(), startPoint.c_str(), toString((VBool) rejectOnDuplicate).c_str(), toString((VBool) rejectOnEmptyKey).c_str(), rejectAction.c_str(), errorAction.c_str(), toString((VBool) enforceLength).c_str());

    return vt_createFuncObj(srvInterface.allocator, JSONExtractor,
                            flattenMaps, flattenArrays, omitEmptyKeys,
                            startPoint, rejectOnDuplicate, rejectOnEmptyKey,
                            rejectAction, errorAction, enforceLength);
}

void FJSONExtractorFactory::getParameterType(ServerInterface &srvInterface,
                                             SizedColumnTypes &parameterTypes) {
    parameterTypes.addBool("flatten_maps");
    parameterTypes.addBool("flatten_arrays");
    parameterTypes.addVarchar(FJSON_STARTPOINT_PARAM_LEN, "start_point");
    parameterTypes.addBool("omit_empty_keys");
    parameterTypes.addBool("reject_on_duplicate");
    parameterTypes.addBool("reject_on_empty_key");
    parameterTypes.addVarchar(32, "reject_action");
    parameterTypes.addVarchar(32, "error_action");
    parameterTypes.addBool("enforce_length");
}
/// END class FJSONExtractorFactory



// Register the extractor factory
RegisterFactory(FJSONExtractorFactory);

} /// END namespace flextable

