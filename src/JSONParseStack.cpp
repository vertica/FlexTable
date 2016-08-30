/**
 * Copyright (c) 2005 - 2015 Hewlett Packard Enterprise Development LP
 *
 * Creation Date: December 10, 2013
 */
/**
 * Flex Table JSON parser state stack
 */


extern "C" {
#include <yajl/yajl_parse.h>
}

#include <limits>

#include "JSONParseStack.h"


namespace flextable {






/// BEGIN JSONNestedContext

JSONNestedContext::JSONNestedContext(
        const std::string &sep,
        char* tupleResultsBuf, size_t tupleResultsBufSize,
        char *keyBuf, size_t keyBufLen) :
    jsKey(keyBuf, keyBufLen, sep[0]),
    tuple_results(tupleResultsBuf, tupleResultsBufSize),
    nested_map_data(NULL), nested_map_len(0) { }

void JSONNestedContext::print_stack() {
    std::cout << "\t\tXXX15 STACK [ ";
    for (size_t i = 0; i < jsKey.keyInfo.size(); i++) {
        std::cout << toString(jsKey.keyInfo[i].type) << " ";
    }
    std::cout << "] (" << jsKey.keyInfo.size() << ")" << std::endl;
}

/// END struct JSONNestedContext



/// BEGIN JSONParseContext

JSONParseContext::JSONParseContext(
        FullRowRowRecordHandlerCallback emitrowCallback,
        NestedRecordHandlerCallback emitsubmapCallback,
        StartPointHandlerCallback startpointresetCallback,
        void* callbackCtxt,
        bool flattenMaps,
        bool flattenArrays,
        bool omitEmptyKeys,
        std::string startPoint,
        int spo,
        bool rejectOnDuplicate,
        bool rejectOnMaterializedTypeError,
        bool rejectOnEmptyKey,
        bool throwOnBadState,
        bool alphanumNames,
        std::string sep,
        char* tupleResultsBuf,
        size_t tupleResultsBufSize)
    : tuple_results_buf(tupleResultsBuf), tuple_results_buf_size(tupleResultsBufSize),
      reject_row(false), type_error(false), num_records(1), start_point_depth(0), curr_depth(0),
      flatten_maps(flattenMaps), flatten_arrays(flattenArrays),
      omit_empty_keys(omitEmptyKeys), start_point(startPoint), start_point_occurrence(spo),
      reject_on_duplicate(rejectOnDuplicate),
      reject_on_materialized_type_error(rejectOnMaterializedTypeError),
      reject_on_empty_key(rejectOnEmptyKey),
      suppress_nonalphanumeric_key_chars(alphanumNames),
      separator(sep),
      emitrow_callback(emitrowCallback), emitsubmap_callback(emitsubmapCallback),
      callback_ctxt(callbackCtxt), startpointreset_callback(startpointresetCallback),
      push_to_raw_column(false), throwOnBadState(throwOnBadState), yajl_hand(NULL), result_writer(NULL), num_rows_emitted(0) {
    InitializeCommonFields(tupleResultsBuf, tupleResultsBufSize);
}

JSONParseContext::JSONParseContext(JSONParseContext* sourceCtx)
    : reject_row(sourceCtx->reject_row), type_error(sourceCtx->type_error),
      num_records(sourceCtx->num_records),
      start_point_depth(sourceCtx->start_point_depth), curr_depth(sourceCtx->curr_depth),
      flatten_maps(sourceCtx->flatten_maps), flatten_arrays(sourceCtx->flatten_arrays),
      omit_empty_keys(sourceCtx->omit_empty_keys), start_point(sourceCtx->start_point),
      start_point_occurrence(sourceCtx->start_point_occurrence),
      reject_on_duplicate(sourceCtx->reject_on_duplicate),
      reject_on_materialized_type_error(sourceCtx->reject_on_materialized_type_error),
      reject_on_empty_key(sourceCtx->reject_on_empty_key),
      suppress_nonalphanumeric_key_chars(sourceCtx->suppress_nonalphanumeric_key_chars),
      separator(sourceCtx->separator),
      emitrow_callback(sourceCtx->emitrow_callback), emitsubmap_callback(sourceCtx->emitsubmap_callback),
      callback_ctxt(sourceCtx->callback_ctxt), startpointreset_callback(sourceCtx->startpointreset_callback),
      push_to_raw_column(sourceCtx->push_to_raw_column),
      throwOnBadState(sourceCtx->throwOnBadState), yajl_hand(NULL),
      result_writer(NULL), num_rows_emitted(sourceCtx->num_rows_emitted) { }

void JSONParseContext::InitializeCommonFields(
        char* tupleResultsBuf,
        size_t tupleResultsBufSize) {
    parse_stack.push_back(JSONNestedContext(separator, tupleResultsBuf, tupleResultsBufSize, keyBuf.A, keyBuf.len));
}

void JSONParseContext::print_stack() {
    std::cout << "\t\tXXX14 STACK [ " << std::endl;;
    for (size_t i = 0; i < parse_stack.size(); i++) {
        parse_stack[i].print_stack();
    }
    std::cout << "] (" << parse_stack.size() << ")" << std::endl;
}


void JSONParseContext::set_buf(char* tupleResultsBuf, size_t tupleResultsBufSize,
                               char* keyBuf, size_t keyBufSize) {
    this->tuple_results_buf = tupleResultsBuf;
    this->tuple_results_buf_size = tupleResultsBufSize;

    if (tupleResultsBufSize == 0) { this->tuple_results_buf = NULL; }
    parse_stack.back().tuple_results = VMapPairWriter(tupleResultsBuf, tupleResultsBufSize);

    this->keyBuf = Buffer<char>(keyBuf, keyBufSize);
    this->parse_stack.back().jsKey = JSONKey(keyBuf, keyBufSize, separator[0]);
}

void JSONParseContext::set_push_to_raw_column(bool pushToRawCol) {
    this->push_to_raw_column = pushToRawCol;
}

void JSONParseContext::reset_context() {
    this->parse_stack.clear();
    this->reject_row = false;
    this->reject_reason = "";
    this->num_records = 0;
    this->InitializeCommonFields(this->tuple_results_buf, this->tuple_results_buf_size);
}

int JSONParseContext::check_end_of_record() {
    // If we're not at end-of-(potentially-nested-) object, don't fire this event
    if (!parse_stack.back().jsKey.empty()) {
        return true;
    }

    // Decide whether we're at end-of-record and should emit a row, or
    //  just end-of-nested-object and should emit into the parent record
    bool cancelParse = false;
    if (parse_stack.size() == 1) {
        /*
         * This is the end-of-record case. Need to either emit or reject the
         * record.
         */
        if (emitrow_callback) {
            if (!reject_row) {
                LogDebugUDBasic("{check_end_of_record} "
                                "Emitting a new row of unpacked length: [%lu]! "
                                "Row #: [%u]",
                                parse_stack.back().tuple_results.get_buf_used(),
                                num_records);

                reject_row = !emitrow_callback(
                    callback_ctxt,
                    parse_stack.back().tuple_results);
            }

            if (reject_row) {
                std::stringstream rejectMessage;
                rejectMessage
                    << "Error:  Record failed to emit row for record #["
                    << ((unsigned long)num_records)
                    << "]; row to be rejected";

                if (reject_reason.empty()) {
                    reject_reason = rejectMessage.str();
                }

                LogDebugUDBasic("%s", rejectMessage.str().c_str());

                if (reject_on_materialized_type_error && type_error) {
                    /* reset the reject flags */
                    reject_row    = false;
                    reject_reason = "";
                } else {
                    cancelParse = true;
                }
            } else if (start_point_depth > 0 && start_point_depth == curr_depth) {
                cancelParse = true;   // Reached the end of the record under the start_point; stop parsing further
            }
        }
        num_records++;
    } else {
        /*
         * This is the end-of-submap case.
         */
        if (emitsubmap_callback && !reject_row) {
            LogDebugUDBasic("{check_end_of_record} Emitting a new sub-map of unpacked length: [%lu]! Row #: [%u]", parse_stack.back().tuple_results.get_buf_used(), num_records);
            emitsubmap_callback(callback_ctxt,
                                parse_stack.back().tuple_results,
                                parse_stack.back().nested_map_data,
                                parse_stack.back().nested_map_len);
            if ((parse_stack.back().nested_map_data <= 0 || parse_stack.back().nested_map_data == NULL) && (push_to_raw_column) && (!reject_row)) {
                if (throwOnBadState) {
                    vt_report_error(0, "NULL sub-map, failed to populate");
                } else {
                    // Invalid state. Skip this record (VER-44418)
                    reject_row = true;
                    cancelParse = true;
                }
            }
        }
    }

    parse_stack.back().tuple_results.clear();
    return !cancelParse;
}

void JSONParseContext::add_pair(const char* value, size_t value_len) {
    if (parse_stack.back().jsKey.empty()) {
        if (throwOnBadState) {
            vt_report_error(0, "Corrupt JSON file:  Tried to add element '[%s]' when not in an array or prefixed by a map key.  (If you want to parse flat values, see FDelimitedParser.) on record # [%lu]", std::string(value, value_len).c_str(), num_records);
        } else {
            // Invalid state. Skip this record (VER-44418)
            reject_row = true;
            return;
        }
    }

    if (reject_row) {
        return;
    }

    if ((reject_on_empty_key || omit_empty_keys) &&
        parse_stack.back().jsKey.isTopKeyEmpty()) {
        std::stringstream rejectMessage;
        rejectMessage << "Error:  Found empty key at compound key: [" <<
            parse_stack.back().jsKey.getKeyStr() << "] on record #[" << 
            ((unsigned long)num_records) << "] with ";

        if (reject_on_empty_key) {
            rejectMessage << "reject_on_empty_key specified; row to be rejected";
            if (reject_reason.empty()) {
                reject_reason = rejectMessage.str();
            }
            LogDebugUDBasic("%s", rejectMessage.str().c_str());
            reject_row = true;
        } else {  // omit_empty_keys
            rejectMessage << "omit_empty_keys specified; pair to be omitted";
            LogDebugUDBasic("%s", rejectMessage.str().c_str());
        }
    } else if (!parse_stack.back().tuple_results.append(
                   *srvInterface,
                   parse_stack.back().jsKey.getKey(),
                   parse_stack.back().jsKey.getKeyLength(),
                   (value == NULL),
                   value,
                   value_len)) {
        std::stringstream rejectMessage;
        rejectMessage << "Error:  Record failed to append key: [" <<
            parse_stack.back().jsKey.getKeyStr() << "] on record #[" <<
            ((unsigned long)num_records) << "]; row to be rejected";

        if (reject_reason.empty()) {
            reject_reason = rejectMessage.str();
        }
        LogDebugUDBasic("%s", rejectMessage.str().c_str());
        reject_row = true;
    }

    check_increment_array_index();
}

/// END struct JSONParseContext


} /// END namespace flextable

