/**
 * Copyright (c) 2005 - 2015 Hewlett Packard Enterprise Development LP
 *
 * Creation Date: December 10, 2013
 */
/**
 * Flex Table JSON parser state stack
 */


#ifndef _JSON_PARSESTACK_H
#define _JSON_PARSESTACK_H

extern "C" {
#include <yajl/yajl_parse.h>
}

#include <string>

#include "VMap.h"
#include "FlexParsersSupport.h"

namespace flextable {


/*
 * Enumeration holding the type of object currently being parsed.
 */
enum JSONFrameType {
    UNDEFINED,
    ARRAY,
    MAP
};

static inline std::string toString(JSONFrameType type) {
    switch (type) {
        case ARRAY: return "ARRAY";
        case MAP: return "MAP";
        default: return "<unknown>";
    }
}

static inline std::string toString(VBool vboolean) {
    switch (vboolean) {
        case vbool_null: return "NULL";
        case vbool_true: return "TRUE";
        case vbool_false: return "FALSE";
        default: throw "INVALID vbool";
    }
}

/// END enum JSONFrameType


/**
 * A JSON compound key.
 *
 * A compound key is formed by one or more simple keys concatenated
 * with a separator character. Example: "A.b.c.123"
 *
 * The simple keys are organized in a stack data structure.
 */
struct JSONKey {
private:
    struct KeyInfo {
        KeyInfo() :
            type(UNDEFINED), index(0), arrayIndex(0), isEmpty(true) { }

        KeyInfo(const KeyInfo& rhs) :
            type(rhs.type), index(rhs.index), arrayIndex(rhs.arrayIndex), isEmpty(rhs.isEmpty) { }

        KeyInfo(JSONFrameType type, uint32 index, bool isEmpty) :
            type(type), index(index), arrayIndex(0), isEmpty(isEmpty) { }

        // Frame information
        JSONFrameType type; // key is for Map or Array

        // Start index in buf of this key
        uint32 index;

        // Array index key. Start from 0
        uint32 arrayIndex; // only used with type=ARRAY keys

        bool isEmpty; // is the key empty?
    };

    /**
     * Reset content of this key to the empty key
     */
    void reset() {
        buf.next = 0;
        keyInfo.clear();
        totalKeyLen = 0;
    }


public:
    Buffer<char> buf; // user-provided buffer
    std::vector<KeyInfo> keyInfo; // metadata about the simple keys
    uint32 totalKeyLen; // length of the compound key (includes separators)
    char sep; // separator character

    /**
     * JSONKey c-tor
     * 
     * @param buf User-provided buffer to store the JSON key.
     *            The caller remains the owner and responsible to dealllocate it
     * @param buffer length
     */
    JSONKey(char* A, int len, char sep) : buf(A, len), keyInfo(), totalKeyLen(0) {
        reset();
        this->sep = sep;
    }

    JSONKey() : buf(), keyInfo(), totalKeyLen(0) {
        reset();
        this->sep = '.';
    }

    JSONKey(const JSONKey &rhs) :
        buf(rhs.buf), keyInfo(rhs.keyInfo), totalKeyLen(rhs.totalKeyLen), sep(rhs.sep) {
    }

    /**
     * Return the compound key buffer
     *
     * @note The returned buffer is NOT null-terminated.
     *       Callers should only read from it up to getKeyLength() bytes
     */
    const char* getKey() const {
        return buf.A;
    }

    /**
     * Return the length of the compound key string (includes separators)
     */
    size_t getKeyLength() const {
        return totalKeyLen;
    }

    /**
     * Return the compound key string
     */
    std::string getKeyStr() const {
        return std::string(buf.A, totalKeyLen);
    }

    /**
     * Add a key to the compound key. It adds a separator character if needed
     */
    void push(JSONFrameType type, const char* key, int len) {
        VIAssert(buf.A);
        if (getKeyLength() + sizeof(sep)+ len > VMapPair::MaxKeyLength) {
            vt_report_error(0, "Overflow error:  Too-long key.  Length would be [%zu]; max is [%zu].  [%s]", (getKeyLength() + sizeof(sep) + len), VMapPair::MaxKeyLength, len);
        }

        // If this check fails, you probably haven't sized the key buffer correctly
        VIAssert(buf.canWrite(len + sizeof(sep)));
        const int startIdx = buf.next;
        // Add separator character if necessary
        if (buf.next > 0 || (keyInfo.size() && keyInfo.back().isEmpty)) {
            buf.A[buf.next] = sep;
            buf.next++;
        }

        // Remember key position in buffer
        keyInfo.push_back(KeyInfo(type, buf.next, (len == 0)));
        // Copy key into buffer
        memcpy(&buf.A[buf.next], key, len);
        buf.next += len;
        // Update total key length
        totalKeyLen += (buf.next - startIdx);
    }

    /**
     * Pop the top most simple key of the compound key
     * It cleans up separator character if needed
     */
    void pop() {
        if (!keyInfo.size()) {
            // There is no key in the stack
            return;
        }

        // keyIndex points either to the beginning of the buffer
        // or to the key following the last separator character
        const uint32 keyIndex = keyInfo.back().index;
        keyInfo.pop_back();

        if (keyIndex) {
            // rewind buffer
            buf.next = keyIndex - 1;
            // Decrease total key length by the length[simple key] and sep character
            totalKeyLen -= ((totalKeyLen - keyIndex) + 1 /*sep*/);
        } else {
            // This is the last key, or there are only empty keys left
            buf.next = 0;
            totalKeyLen = 0;
        }
    }

    void pushArrayIndex() {
        const char indexStr = '0';
        push(ARRAY, &indexStr, 1);
    }

    /**
     * Increase the array index key by 1. It is a no-op on non-array-index keys
     */
    void increaseArrayIndex() {
        KeyInfo &kInfo = keyInfo.back(); // last simple key
        VIAssert(kInfo.type == ARRAY);

        if (kInfo.arrayIndex == 0xFFFFFFFFU) {
            vt_report_error(0, "JSON array is too large: The maximum length is [%zu] entries",
                            0xFFFFFFFFU);
        }

        // If this check fails, you probably haven't sized the key buffer correctly
        VIAssert((buf.len - kInfo.index) > 5 /*size of the largest index*/);

        // Overwrite the array index with the increased index value
        kInfo.arrayIndex++;
        const uint32 lastKeyLen = (totalKeyLen - kInfo.index);

        // OK to use C-string functions
        // we're writing a number string not containing NULL char
        sprintf(&buf.A[kInfo.index] , "%hu", kInfo.arrayIndex);

        // Advance buffer's next index if necessary
        if (strlen(&buf.A[kInfo.index]) > lastKeyLen) {
            buf.next += (strlen(&buf.A[kInfo.index]) - lastKeyLen);
            totalKeyLen += (strlen(&buf.A[kInfo.index]) - lastKeyLen);
        }
    }

    /**
     * Check if the compound key is equal to a given string
     */
    bool equals(const std::string& key) {
        if (totalKeyLen != key.length()) {
            return false;
        }
        return (memcmp(buf.A, key.data(), totalKeyLen) == 0);
    }

    /**
     * Check if the compound key has no simple keys
     */
    bool empty() const {
        if (!keyInfo.size()) {
            return true;
        } else {
            return false;
        }
    }


    ///////////////////////
    /// Top key methods ///
    ///////////////////////

    /**
     * Set the top simple key with a given string. The key must be of MAP type
     *
     * Callers should make sure they first invoke push()
     */
    void setTopKey(const char* key, int len) {
        VIAssert(keyInfo.size());

        KeyInfo& kinfo = keyInfo.back();
        VIAssert(kinfo.type == MAP);
        
        // Sanity checks on buffer length and current free index
        VIAssert(buf.canWrite(len + sizeof(sep)));
        totalKeyLen -= (buf.next - kinfo.index); // substract old key length
        buf.next = (int) kinfo.index;

        // Copy key into buffer
        kinfo.isEmpty = (len == 0);
        memcpy(&buf.A[buf.next], key, len);
        buf.next += len;
        // Update total key length
        totalKeyLen += len;
    }

    /**
     * Check if the top simple key is equal to a given string
     */
    bool topKeyEquals(const std::string& skey) const {
        if (!keyInfo.size()) {
            // There is no key in the stack
            return false;
        }

        const KeyInfo& kinfo = keyInfo.back();
        const uint32 lastKeyLen = totalKeyLen - kinfo.index;
        if (lastKeyLen != skey.length()) {
            return false;
        } 
        return (memcmp(&buf.A[kinfo.index], skey.data(), lastKeyLen) == 0);
    }

    /**
     * Check if the top simple key is the empty key
     */
    bool isTopKeyEmpty() const {
        if (!keyInfo.size() || keyInfo.back().isEmpty) {
            return true;
        } else {
            return false;
        }
    }

    /**
     * Check if the top simple key is of a given type
     */
    JSONFrameType getTopKeyType() const {
        if (keyInfo.size()) {
            return keyInfo.back().type;
        } else {
            return UNDEFINED;
        }
    }
};



/*
 * Context holding the parse state information needed to parse a (potentially)
 *  nested JSON object or array.
 */
struct JSONNestedContext {
public:

    ////////////////////////////
    /// Parse Stack Elements ///
    ////////////////////////////
/// Keeps track of our current key prefix stack, which may be composed
///   by one or more simple keys depending on the 
//    flattening arguments
    JSONKey jsKey;


/// Stored processed tuples until we are ready to emit them to DB rows
    VMapPairWriter tuple_results;

/// Temporary pointers to nested map results data just for the time between
///   emitting it and adding it to its parent map's VMapPairWriter
    const char* nested_map_data;
    size_t nested_map_len;
/// End Parse Stack Elements


    JSONNestedContext(
            const std::string &sep,
            char*tupleResultsBuf, size_t tupleResultsBufSize,
            char *keyBuf, size_t keyBufLen);

    void print_stack();

    ~JSONNestedContext() { }

}; /// End struct JSONNestedContext



/*
 * Holds state information for use with JSON parsing, notably the different
 *  stacks involved.
 */
struct JSONParseContext {
public:
    /*
     * Callback definitions.
     */
    typedef int (*FullRowRowRecordHandlerCallback)(void*, VMapPairWriter &);
    typedef int (*NestedRecordHandlerCallback)(void*, VMapPairWriter &, const char*&, size_t &);
    typedef int (*StartPointHandlerCallback)(void*, ServerInterface &srvInterface);

    ////////////////////////////
    /// Parse Stack Elements ///
    ////////////////////////////
    std::vector<JSONNestedContext> parse_stack;

    char* tuple_results_buf;
    size_t tuple_results_buf_size;

    bool reject_row;
    bool type_error; // indicate if type error happened while materializing
    std::string reject_reason;
    uint32 num_records;  // For logging

    uint32 start_point_depth;
    uint32 curr_depth;

    Buffer<char> keyBuf; // pre-allocated buffer to store JSON keys
/// End Parse Stack Elements

    ////////////////////////
    /// Config variables ///
    ////////////////////////
    bool flatten_maps;
    bool flatten_arrays;
    bool omit_empty_keys;
    std::string start_point;
    int start_point_occurrence;
    bool reject_on_duplicate;
    bool reject_on_materialized_type_error;
    bool reject_on_empty_key;
    bool suppress_nonalphanumeric_key_chars;
    std::string separator;

    FullRowRowRecordHandlerCallback emitrow_callback;
    NestedRecordHandlerCallback emitsubmap_callback;
    void* callback_ctxt;
    StartPointHandlerCallback startpointreset_callback;

    bool push_to_raw_column;

    // Throw if inconsistent state is reached during parsing? Defaults to TRUE
    // If set to FALSE, it will reject the record instead
    bool throwOnBadState;
/// End Config variables

    //////////////////////
    /// YAJL Callbacks ///
    ////////////////////// 
    yajl_callbacks yajl_callback;
    yajl_handle yajl_hand;
/// End YAJL Callbacks

    ///////////////////////
    /// ServerInterface ///
    ///////////////////////
    ServerInterface* srvInterface;

    ////////////////////////////////////////
    /// Extractor ScalarFunction Members ///
    ////////////////////////////////////////
    BlockWriter* result_writer;
    uint num_rows_emitted;
/// End ServerInterface



    /*
     * Default constructor to make JSONParser construction happy.
     * The UDl framework will create one UDParser instance and re-use it multiple
     *  times when multiple files from the same node are being loaded in a single
     *  COPY statement. The UDl framework does, however, call setup() and destroy()
     *  multiple times, so the JSONParseContext will ignore the JSONParseContext
     *  constructed with this constructor and instead re-create it on each call to
     *  setup() and free it on each call to destroy().
     */
    JSONParseContext() {}

    JSONParseContext(
            FullRowRowRecordHandlerCallback emitrowCallback,
            NestedRecordHandlerCallback emitsubmapCallback,
            StartPointHandlerCallback startpointreset_callback,
            void* callbackCtxt,
            bool flattenMaps = true,
            bool flattenArrays = false,
            bool omitEmptyKeys = false,
            std::string startPoint = "",
            int spo = 1,
            bool rejectOnDuplicate = false,
            bool rejectOnMaterializedTypeError = false,
            bool rejectOnEmptyKey = false,
            bool throwOnBadState = true,
            bool alphanumKeys = false,
            std::string separator = ".",
            char* tupleResultsBuf = NULL,
            size_t tupleResultsBufSize = 0);

    JSONParseContext(JSONParseContext* sourceCtx);

    void InitializeCommonFields(
            char* tupleResultsBuf = NULL,
            size_t tupleResultsBufSize = 0);

    void print_stack();


    void set_buf(char* tupleResultsBuf, size_t tupleResultsBufSize,
                 char* keyBuf, size_t keyBufSize);

    void set_push_to_raw_column(bool pushToRawCol);

    void reset_context();

    //////////
    // YAJL callbacks -- instance methods
    //////////
    int handle_start_map() {
        LogDebugUDBasic("{FlexTable} {handle_start_map} is beginning to parse a nested map object.");

        curr_depth++;
        if ((!start_point.empty()) && start_point_depth == 0) {
            if ((!parse_stack.empty()) &&
                parse_stack.back().jsKey.topKeyEquals(start_point) &&
                !(--(start_point_occurrence))) {
                start_point_depth = curr_depth - 1;
                if (startpointreset_callback) {
                    LogDebugUDBasic("{handle_start_map_impl} Resetting context for start_point!");
                    startpointreset_callback(callback_ctxt,* srvInterface);
                }
            }
        }

        // Add a new frame to the parse stack if we're about to parse a nested map
        if (!(flatten_maps || parse_stack.back().jsKey.empty())) {  // Empty frame stack check to avoid an extra frame on top-level maps representing whole rows not enclosed in []'s
//std::cout << "{handle_start_map} Creating a new JSONNestedContext for a nested map object with the reduced buffer size: " << (parse_stack.back().tuple_results.get_buf_total_size() - parse_stack.back().tuple_results.get_buf_used()) << ", currently-used buffer space: " << parse_stack.back().tuple_results.get_buf_used() << ", and global parse_stack.back().tuple_results.get_buf_total_size(): " << parse_stack.back().tuple_results.get_buf_total_size() << " of total size: " << parse_stack.back().tuple_results.get_buf_total_size() << " rooted at address: " << (void*) parse_stack.back().tuple_results.get_buf() << std::endl;
            LogDebugUDBasic("{FlexTable} {handle_start_map} Creating a new JSONNestedContext for a nested map object with the reduced buffer size: [%zu], global and parse_stack.back().tuple_results.get_buf_total_size(): [%zu].", (parse_stack.back().tuple_results.get_buf_total_size() - parse_stack.back().tuple_results.get_buf_used()), parse_stack.back().tuple_results.get_buf_total_size());

            const JSONNestedContext &jsnctx = parse_stack.back();
            parse_stack.push_back(JSONNestedContext(separator,(parse_stack.back().tuple_results.get_buf() + parse_stack.back().tuple_results.get_buf_used()), (parse_stack.back().tuple_results.get_buf_total_size() - parse_stack.back().tuple_results.get_buf_used()), jsnctx.jsKey.buf.getNext(), jsnctx.jsKey.buf.free()));
        }

        // Push a temporary NULL key. We expect to fill it with the real map key in handle_map_key
        parse_stack.back().jsKey.push(MAP, NULL, 0);

        return true;
    }

    int handle_end_map() {
        LogDebugUDBasic("{FlexTable} {handle_end_map} just finished parsing a nested map object.");

        curr_depth--;
        validate_stack(MAP);
        parse_stack.back().jsKey.pop();

        check_increment_array_index();

        // If the stack is now empty, we have a record to emit
        if (!check_end_of_record()) {
            return false;
        }

        // Go up to the previous frame in the parse stack and emit a nested map, if requested
        if (!flatten_maps && parse_stack.back().jsKey.empty() && parse_stack.size() > 1) {
            const char* nested_map_data = parse_stack.back().nested_map_data;
            size_t nested_map_len = parse_stack.back().nested_map_len;
            parse_stack.pop_back();

            add_pair(nested_map_data, nested_map_len);
        }

        return true;
    }

    int handle_start_array() {
        curr_depth++;
        if ((!start_point.empty()) && start_point_depth == 0) {
            if ((!parse_stack.empty()) &&
                parse_stack.back().jsKey.topKeyEquals(start_point) &&
                !(--(start_point_occurrence))) {
                start_point_depth = curr_depth - 1;
                if (startpointreset_callback) {
                    LogDebugUDBasic("{handle_start_array_impl} Resetting context for start_point!");
                    startpointreset_callback(callback_ctxt,* srvInterface);
                }
            }
        }
        // Hack to ignore outer arrays
        if (parse_stack.back().jsKey.empty()) {
            return true;  // Hack to ignore outer arrays
        }
        LogDebugUDBasic("{FlexTable} {handle_start_array} is beginning to parse a nested array object.");

        // Add a new frame to the parse stack if we're about to parse a nested array
        curr_depth++;
        if (!flatten_arrays) {
//std::cout << "{handle_start_array} Creating a new JSONNestedContext for a nested array object with the reduced buffer size: " << (parse_stack.back().tuple_results.get_buf_total_size() - parse_stack.back().tuple_results.get_buf_used()) << ", currently-used buffer space: " << parse_stack.back().tuple_results.get_buf_used() << ", and global parse_stack.back().tuple_results.get_buf_total_size(): " << parse_stack.back().tuple_results.get_buf_total_size() << " of total size: " << parse_stack.back().tuple_results.get_buf_total_size() << " rooted at address: " << (void*) parse_stack.back().tuple_results.get_buf() << std::endl;
            LogDebugUDBasic("{handle_start_array} Creating a new JSONNestedContext for a nested array object with the reduced buffer size: [%zu], global parse_stack.back().tuple_results.get_buf_total_size(): [%zu]", (parse_stack.back().tuple_results.get_buf_total_size() - parse_stack.back().tuple_results.get_buf_used()), parse_stack.back().tuple_results.get_buf_total_size());

            const JSONNestedContext &jsnctx = parse_stack.back();
            parse_stack.push_back(JSONNestedContext(separator,(parse_stack.back().tuple_results.get_buf() + parse_stack.back().tuple_results.get_buf_used()), (parse_stack.back().tuple_results.get_buf_total_size() - parse_stack.back().tuple_results.get_buf_used()), jsnctx.jsKey.buf.getNext(), jsnctx.jsKey.buf.free()));
        }

        parse_stack.back().jsKey.pushArrayIndex();

        return true;
    }

    int handle_end_array() {
        curr_depth--;
        if ((!start_point.empty()) && curr_depth == start_point_depth &&
            parse_stack.back().jsKey.empty()) {
            // Good thing we checked for this
        }

        // Hack to ignore outer arrays
        if (parse_stack.back().jsKey.empty()) {
            return (!((start_point_depth > 0) && (start_point_depth == curr_depth)));
        }
        LogDebugUDBasic("{FlexTable} {handle_end_array} just finished parsing a nested array object.");

        curr_depth--;
        validate_stack(ARRAY);
        parse_stack.back().jsKey.pop();

        check_increment_array_index();

        // If the stack is now empty, we have a record to emit
        if (!check_end_of_record()) {
            return false;
        }

        // Go up to the previous frame in the parse stack and emit a nested map, if requested
        if (!flatten_arrays && parse_stack.back().jsKey.empty()) {
            const char* nested_map_data = parse_stack.back().nested_map_data;
            size_t nested_map_len = parse_stack.back().nested_map_len;
            parse_stack.pop_back();

            add_pair(nested_map_data, nested_map_len);
        }

        return true;
    }

    int handle_map_key(const char* key, size_t key_len) {
        char replacementKey[1024];
        if (suppress_nonalphanumeric_key_chars && key_len <= sizeof(replacementKey)) {
            for (size_t i = 0; i< key_len; ++i) {
                replacementKey[i] = (isalnum(key[i]) ? key[i] : '_');
            }
            key = replacementKey;
        }

        parse_stack.back().jsKey.setTopKey(key, key_len);

        return true;
    }

    int handle_null() {
        add_pair(NULL, 0);
        return true;
    }

    int handle_boolean(bool val) {
        add_pair(val ? "T" : "F", 1);
        return true;
    }

    int handle_string(const char* val, size_t val_len) {
        add_pair(val, val_len);
        return true;
    }

    int handle_number(const char* val, size_t val_len) {
        // May someday want to pre-cast to an int or double
        // or numeric or (...)
        // For now, punt.  Defer the problem.
        add_pair(val, val_len);
        return true;
    }

private:
    int check_end_of_record();
    void add_pair(const char* value, size_t value_len);

    bool validate_stack(JSONFrameType type, bool abortOnError=true) {
        if (parse_stack.back().jsKey.getTopKeyType() != type) {
            if (abortOnError) {
                vt_report_error(0, "Corrupt JSON file:  Expecting END OF [%s], got END OF [%s] on record # [%lu]",
                                toString(type).c_str(), toString(parse_stack.back().jsKey.getTopKeyType()).c_str(), num_records);
            } else {
                return false;
            }
        }

        return true;
    }

    void check_increment_array_index() {
        if (parse_stack.back().jsKey.getTopKeyType() == ARRAY) {
            parse_stack.back().jsKey.increaseArrayIndex();
        }
    }
}; /// End struct JSONParseContext


} /// End namespace flextable

#endif /// _JSON_PARSESTACK_H
