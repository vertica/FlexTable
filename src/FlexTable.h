/**
 * Copyright (c) 2005 - 2015 Hewlett Packard Enterprise Development LP
 *
 * Creation Date: December 10, 2013
 *
 * Note that this code is subject to change at any time. Future versions
 *  may or may not maintain the current interface.
 */
/**
 * Global constants and utilities for Flex Table
 */

#ifndef FLEXTABLE_H_
#define FLEXTABLE_H_

#include <string>

#include "StringParsers.h"  // Required for uint32



/**
  * @brief The namespace for all Flexible Tables UDxs
  */
namespace flextable
{


/// BEGIN table internal column names constants
#define RAW_COLUMN_NAME "__raw__"
#define RAW_IDENTITY_COLUMN_NAME "__identity__"
/// END table internal column names constants

/// BEGIN internal code control constants
/// Return value sizes; prefer something based on the FlexTableRawSize confguration from the server
static const uint32 VALUE_FOOTPRINT = 65000;
/// Return map sizes; prefer something based on the FlexTableRawSize confguration from the server
static const uint32 MAP_FOOTPRINT =  130000;
/**
 * The length in Bytes of the header/prefix information including the version header
 * and the boundary lookup giving the location where the keys stop and the values start
 **/
#define MAP_BLOCK_HEADER_SIZE 8
/**
 * The length in Bytes of the chunks in which string normalization buffers and
 * input read buffers should be allocated.
 **/
#define BASE_RESERVE_SIZE 256
/// How long parsed items should be truncated to in standard log messages' output
#define LOG_TRUNCATE_LENGTH 200
/// How long warnings sets should be truncated to for warnings
#define WARN_TRUNCATE_LENGTH 4000 // XXX FUTURE XXX Make 4K more specific/use constant/check if server-side configurable
/// END internal code control constants


/// BEGIN function return column names constants
#define MAP_VALUE_OUTPUT_NAME "mapvalue"
#define MAP_STRING_OUTPUT_NAME "map"
#define MAP_CONTAINS_KEY_OUTPUT_NAME "containskey"
#define MAP_KEY_NAMES_OUTPUT_NAME "keys"
#define MAP_VALUE_NAMES_OUTPUT_NAME "values"
#define MAP_KEYS_LENGTH_OUTPUT_NAME "length"
#define MAP_KEYS_TYPE_OUTPUT_NAME "type_oid"
#define MAP_KEYS_ROW_NUM_OUTPUT_NAME "row_num"
#define MAP_KEYS_KEY_NUM_OUTPUT_NAME "field_num"
#define MAP_RAW_MAP_OUTPUT_NAME "raw_map"
/// END function return column names constants

/// BEGIN common enums
/*
 * Enumeration holding the types of actions which might be taken in case of failures.
 *
 * XXX FUTURE XXX When Boost is available, use <boost/preprocessor.hpp> ala http://stackoverflow.com/questions/5093460/how-to-convert-an-enum-type-variable-to-a-string
 * XXX FUTURE XXX Or, could use <p99_enum.h> ala http://stackoverflow.com/questions/9907160/how-to-convert-enum-names-to-string-in-c
 */
#define GENERATE_ENUM(ENUM) ENUM,
#define GENERATE_STRING(STRING) #STRING,

#define FOREACH_ENUM_VAL(ENUM_VAL) \
        ENUM_VAL(None)   \
        ENUM_VAL(LogDebug)  \
        ENUM_VAL(LogAlways)   \
        ENUM_VAL(Warn)  \
        ENUM_VAL(Abort)  \

#define NUM_FAILACTIONS 5

enum FailAction {
    FOREACH_ENUM_VAL(GENERATE_ENUM)
};

static const char* FailAction_STRINGS[] = {
    FOREACH_ENUM_VAL(GENERATE_STRING)
};

static inline const std::string GetAvailableFailActionsString() {
    std::stringstream toReturn;
    toReturn << FailAction_STRINGS[0];
    for (int currFailActionNum = 1; currFailActionNum < NUM_FAILACTIONS; currFailActionNum++) {
        toReturn << ", " << FailAction_STRINGS[currFailActionNum];
    }
    return toReturn.str();
}

static inline const char* toString(FailAction failAction) {
    if (failAction < 0 || failAction >= NUM_FAILACTIONS) {
        return NULL;
    }
    return FailAction_STRINGS[failAction];
}


/**
 * Error handling helper.
 *
 * Decides on severity of action based on the passed FailAction
 * Assumes there exists in char* warnings_buf to receive the warnings text and a
 *  warnings_buf_len to keep track of how many characters in the buffer have been used
**/
// Currently messy; somewhat waiting on VER-26376: vt_report_warning
#define PerformFailOp(failAction, c...) \
do {\
    switch (failAction) {\
        case None:\
            break;\
        case LogDebug:\
            LogDebugUDBasic(c);\
            break;\
        case Warn:\
            if (warnings_buf == NULL || warnings_buf_len <= 0) {\
                warnings_buf = new char[WARN_TRUNCATE_LENGTH];\
            }\
            if (warnings_buf_len < WARN_TRUNCATE_LENGTH) {\
                /* XXX FUTURE XXX Bucket the warnings */\
                uint printReturn = (uint) snprintf((warnings_buf + warnings_buf_len), (WARN_TRUNCATE_LENGTH - warnings_buf_len), c);\
                if (printReturn < 0) {\
                    LogDebugUDWarn("Failed to register the following warning:");\
                    LogDebugUDWarn(c);\
                } else {\
                    warnings_buf_len += ((printReturn > (WARN_TRUNCATE_LENGTH - warnings_buf_len)) ? (WARN_TRUNCATE_LENGTH - warnings_buf_len) : printReturn);\
                    if (warnings_buf_len < WARN_TRUNCATE_LENGTH) {\
                        snprintf(warnings_buf + (warnings_buf_len), 2, "\n"); /* Separate w/newlines */\
                        warnings_buf_len++;\
                    }\
                }\
            }\
            break;\
        case LogAlways:\
            LogDebugUDWarn(c);\
            break;\
        case Abort:\
        default:\
            vt_report_error(0, c);\
            break;\
    }\
} while(false) 
/// END common enums


} /// END namespace flextable

/// END define FLEXTABLE_H_
#endif

