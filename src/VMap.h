/**
 * Copyright (c) 2005 - 2015 Hewlett Packard Enterprise Development LP
 *
 * Creation Date: December 10, 2013
 *
 * Note that this code is subject to change at any time. Future versions
 *  may or may not maintain the current interface.
 */
/**
 * Implementation of a basic interface to and from to V1 map type which is
 *  packed into a single varbinary column.
 *
 * Version history:
 *  V0:   The unversioned 6.1SP1 prototype taking up two columns. Not supported.
 *  V1:   The first official release for Crane 7.0 taking up a single column.
 */




#ifndef VMAP_H
#define VMAP_H


#include <iostream>
#include <limits>
#include <map>
#include <set>
#include <iterator>

/// NB: Utilizes jsoncpp-src-0.6.0-rc2
#include <json/json.h>

#include "Vertica.h"
#include "VerticaUDx.h"

#include "FlexTable.h"

#include "VMapPair.h"


using namespace Vertica;



namespace flextable
{


/////////////////////// BEGIN VMapPairWriter Declaration ///////////////////////

///@brief Helper class to pack and store key/value pairs into packed, VMapPair
//    form in an internal buffer.
class VMapPairWriter {
public:
    ///@brief Constructor which accepts an empty buffer to store VMapPairs within.
    inline VMapPairWriter(char* buf, size_t buf_size);

    ///@brief Adds a single already-packed VMapPair key/value pair into this
    ///  instance's pairs buffer.
    inline bool append(ServerInterface &srvInterface, const VMapPair &pair);
    ///@brief Packs and adds a single key/value pair into this instance's pairs buffer.
    inline bool append(ServerInterface &srvInterface,
                       const char* key,
                       uint32 key_len,
                       bool is_null,
                       const char* value,
                       uint32 value_len);

    ///@brief Returns a pointer to this instance's pairs buffer.
    inline char* get_buf();
    ///@brief Returns the length of the pairs currently stored in this instance's
    ///  pairs buffer.
    inline size_t get_buf_used();
    ///@brief Returns the total length of this instance's pairs buffer including both
    ///  used and unused buffer space.
    inline size_t get_buf_total_size();

    ///@brief Reset this instance's pairs buffer to empty
    inline void clear();

private:
    /// BEGIN data members
    /*
     * Buffer to store the pairs within.
     */
    char* buf;
    /*
     * The total size of the pairs buffer.
     */
    size_t buf_size;
    /*
     * How much of the pairs buffer is currently in use.
     */
    size_t buf_used;
    /// END data members
};

//////////////////////// END VMapPairWriter Declaration ////////////////////////



/////////////////////// BEGIN VMapPairReader Declaration ///////////////////////

///@brief Helper class to separate and serve key/value pairs which it has received as a
///  a block of sequentially serialized VMapPairs.
class VMapPairReader {
public:
    ///@brief Construct a VMapPairReader from a VString containing serialized VMapPairs.
    inline VMapPairReader(const VString& map_data);
    ///@brief Construct a VMapPairReader from a char* containing serialized VMapPairs.
    inline VMapPairReader(const char* buf, size_t len);
    ///@brief Construct a VMapPairReader from a VMapPairWriter containing serialized VMapPairs.
    inline VMapPairReader(VMapPairWriter pairWriter);

    ///@brief Retrieve a container of VMapPairs.
    std::vector<VMapPair> &get_pairs() { return pairs; }

private:
    /// BEGIN data members
    /*
     * Container of loaded VMapPairs.
     */
    std::vector<VMapPair> pairs;

    /*
     * Read in/deserialize a set of VMapPairs stored in a VString.
     */
    inline void setup_map_data(const VString& str);
    /*
     * Read in/deserialize a set of VMapPairs stored in a char*.
     */
    inline void setup_map_data(const char* start, const char* end);
    /// END data members
};

//////////////////////// END VMapPairReader Declaration ////////////////////////

////////////////////////////////////////////////////////////////////////////////
//////////////////////// END Map Pair Read/Write Helpers ///////////////////////
////////////////////////////////////////////////////////////////////////////////



////////////////////////////////////////////////////////////////////////////////
///////////////////////// BEGIN Map Block Declarations /////////////////////////
////////////////////////////////////////////////////////////////////////////////



///@brief This along with MapV1ValuesBlock and MapV1KeysBlock is the on-disk
///   representation of the VMap as you read it from the __raw__ column.
///@details Base class which stores the common fields of blocks representing
///   the stable cross-version portion of the on-disk Vertica Flexible
///    Table Maps.
struct MapBlock {
public:
    uint32 map_version;                 //00-03: version of this map
    char map_data[];                    //04-...: version-specific information
}__attribute__((packed));


/*
 * Classes used for reading the contents of a single-column Crane/V1 map from a
 *  map block in on-disk format. The prototype (V0) two-column format is no
 *  longer supported.
 *
 * V1 blocks are serialized as:
 *  uint32 version #
 *  uint32 length of the values half of the lookup
 *  uint32 number of values present
 *  uint32 array of starting points for each of the values
 *  char*  array of values
 *  uint32 number of keys present (for now)
 *  uint32 array of starting points for each of the key names
 *  char*  array of key names
 */

///@brief This along with MapBlock and MapV1KeysBlock is the on-disk
///   representation of the VMap as you read it from the __raw__ column.
///@details Crane/V1 map type block values on-disk storage class.
///   Starts at the beginning of the MapBlock.
struct MapV1ValuesBlock : public MapBlock {
public:
//  uint32 map_version;                 //00-03: version of this map, inherited from MapBlock
    uint32 value_length;                //04-07: length in Bytes of the value section
    uint32 value_count;                 //08-11: number of values present in this map
    char value_lookup[];                //12-...: value lookup followed by values

    /*
     * Retrieve the length of the values section of the MapBlock to determine
     *  where the keys section starts in the on-disk Vertica Flexible Table Maps.
     */
    inline vsize get_values_part_size() const {
        return (MAP_BLOCK_HEADER_SIZE + value_length);
    }
}__attribute__((packed));

///@brief This along with MapBlock and MapV1ValuesBlock is the on-disk
///   representation of the VMap as you read it from the __raw__ column.
///@details Crane/V1 map type block keys on-disk storage class.
///   Starts from somewhere in the middle of the MapBlock, just after
///   the MapV1ValuesBlock.
struct MapV1KeysBlock {
public:
    uint32 key_count;                   //00-03: number of keys present in this map
    char key_lookup[];                  //04-...: key lookup followed by keys
}__attribute__((packed));

////////////////////////////////////////////////////////////////////////////////
////////////////////////// END Map Block Declarations //////////////////////////
////////////////////////////////////////////////////////////////////////////////



////////////////////////////////////////////////////////////////////////////////
//////////////////////////// BEGIN Full VMap Helpers ///////////////////////////
////////////////////////////////////////////////////////////////////////////////

/////////////////////// BEGIN VMapBlockReader Declaration //////////////////////

///@brief Class used for reading the contents of a single-column map from a
///  map block in on-disk format.
class VMapBlockReader {
public:
    ///@brief Construct a VMapBlockReader from a VString containing a VMap MapBlock in on-disk format.
    inline VMapBlockReader(ServerInterface &srvInterface, const VString& dictblock);
    ///@brief Construct a VMapBlockReader from a StringValue containing a VMap MapBlock in on-disk format.
    inline VMapBlockReader(ServerInterface &srvInterface, EE::StringValue& dictblock);
    ///@brief Construct a VMapBlockReader from a char* containing a VMap MapBlock in on-disk format.
    inline VMapBlockReader(ServerInterface &srvInterface, const char* map_data, const vsize map_size);

    ///@brief Return the number of pairs present in the VMap.
    inline uint32 get_count();

    ///@brief Return the size in Bytes of the key at the requested index in the VMap.
    inline uint32 get_key_size(uint32 keyIndex);
    ///@brief Return the char* name of the key at the requested index in the VMap.
    // @note This function can return NULL when the VMap structure is corrupted
    inline const char* get_key(uint32 keyIndex);

    ///@brief Return the size in Bytes of the value at the requested index in the VMap.
    inline uint32 get_value_size(uint32 valueIndex);
    ///@brief Return the char* name of the value at the requested index in the VMap.
    // @note This function can return NULL when the VMap structure is corrupted
    inline const char* get_value(uint32 valueIndex);

    /**
      *@brief Get the index of the requested key in the VMap.
      *
      *@returns -1 if the key was not found,
      * -2 if the value of the key is NULL, or
      ** a non-negative integer index at which the key was found
      */
    inline int32 search(const VString& key, bool case_sensitive = false);

    ///@brief Return whether the requested key exitst in the VMap.
    inline bool contains_key(const VString& key, bool case_sensitive = false);

private:
    /// BEGIN to_string
    /*
     * Computes the canonical JSON string representation of the data held in
     *  the VMap.
     */
    inline std::string compute_to_string_canonical(ServerInterface &srvInterface);
    /*
     * Translates the VMap into JSONCPP format for stringifying.
     */
    inline Json::Value compute_json_map(ServerInterface &srvInterface);
    /*
     * Computes the legacy, but complete, JSON string representation of the data
     *  held in the VMap.
     *
     * The differences between this and the canonical version genrated by
     *  JSONCPP are:
     *    1. When duplicate keys exist, this method will include them both
     *        which violates canonical JSON's rules, which only allows one
     *        of each key.
     *    2. No special-character escaping is performed.
     */
    inline std::string compute_to_string_manual(ServerInterface &srvInterface, uint indent_level = 0);
    /**
     * Check if a key offset in a VMap is valid
     */
    inline bool isKeyOffsetValid(const uint32 &offset) {
        // Key offsets start at the key count
        // Their values should be greather than the index size (count size + offset space)
        const uint32 indexSize = sizeof(uint32) /*value count*/
            + keys_block->key_count * sizeof(uint32) /*offsets*/;
        if (offset < indexSize || offset > key_length) {
            // Offset out of bound
            return false;
        } else {
            return true;
        }
    }
    /**
     * Check if a non-NULL value offset in a VMap is valid
     */
    inline bool isValueOffsetValid(const uint32 &offset) {
        // Value offsets start after the map header, at the value count
        // Their values should be greather than the index size (count size + offset space)
        const uint32 indexSize = sizeof(uint32) /*value count*/
            + full_block->value_count * sizeof(uint32) /*offsets*/;
        if (offset < indexSize || offset > full_block->value_length) {
            // Offset out of bound
            return false;
        } else {
            return true;
        }
    }

public:
    ///@brief Returns the address of the char* containing the stringified repsentation of the VMap.
    inline std::string to_string(ServerInterface &srvInterface,
                                 bool canonicalJSON = true,
                                 uint indent_level = 0);

    //// END to_string

    //// BEGIN map validation

    ///@brief Returns the integer version of the passed VMap in on-disk format
    ///  contained in the passed VString, or -1 if it does not contain a valid map.
    static inline const int map_version(ServerInterface &srvInterface, const VString& dictblock);

    ///@brief Returns whether the VMap in on-disk format contained in this 
    ///  VMapBlockReader's instance is a valid map.
    inline const bool is_valid_map(ServerInterface &srvInterface, uint32 valueIndex, bool logInvalidMaps = false);
    ///@brief Returns whether the passed VMap in on-disk format contained in the passed
    ///  VString is a valid map.
    static inline const bool is_valid_map(ServerInterface &srvInterface, const VString& dictblock, bool logInvalidMaps = false);
    ///@brief Returns whether the passed VMap in on-disk format contained in the passed
    ///  StringValue is a valid map.
    static inline const bool is_valid_map(ServerInterface &srvInterface, EE::StringValue& dictblock, bool logInvalidMaps = false);

private:
    /*
     * Returns whether the passed VMap values-part (MapV1ValuesBlock) in on-disk
     *  format is a valid map.
     */
    static inline const bool is_valid_map(ServerInterface &srvInterface,
                                        vsize length,
                                        const MapV1ValuesBlock* mapBlock,
                                        bool logInvalidMaps = false);
    /// END map validation


    /// BEGIN internal lookup helpers
    /*
     * Return the Byte offset of the name of the key at the requested index in
     *  the VMap's MapKeysBlock (MapV1KeysBlock).
     *
     *
     *
     *
     * keys_block->key_lookup
     */
    inline uint32 get_key_end_loc(uint32 keyIndex);
    inline uint32 get_key_start_loc(uint32 keyIndex);

    uint32 get_value_end_loc(uint32 valueIndex);
    inline uint32 get_value_start_loc(uint32 valueIndex);
    /// END internal lookup helpers


    /// BEGIN data members
    const MapV1ValuesBlock* full_block;
    const MapV1KeysBlock* keys_block;
    const uint32* value_index;
    const uint32* key_index;
    uint32 key_length;
    /// END data members
};

//////////////////////// END VMapBlockReader Declaration ///////////////////////


/////////////////////// BEGIN VMapBlockWriter Declaration //////////////////////

///@brief Class of static utilities used for writing VMapPair key/value pairs into a single-column (V1) map
///  in on-disk form
///
/// Read from:
///  uint32 version #
///  uint32 length of the values half of the lookup
///  uint32 number of values present
///  uint32 array of starting points for each of the values
///  char*  array of values
///  uint32 number of keys present (for now)
///  uint32 array of starting points for each of the key names
///  char*  array of key names
class VMapBlockWriter {
public:
    ///@brief Helper for case comparing
    static inline bool VMapPairInsensitiveCompare(const VMapPair &pair0, const VMapPair &pair1);

    // \internal Helper struct for case comparing
    ///@brief Helper struct for case comparing
    struct StringInsensitiveCompare {
        inline bool operator() (const std::string& a, const std::string& b) const;
    };


    /**
      *@brief Converts from a map stored within a VMapPairReader into the single-column map representation.
      *
      *@param pair_reader     Contains the map in pairs form.
      *@param map             The destination char buffer to write the single-column map into.
      *@param map_len         The amount of space available to store the single-column map in the map.
      *@param used_map_bytes  Returns the number of Bytes used to store the just-created map.
      */
    static inline bool convert_vmap(ServerInterface &srvInterface, VMapPairReader &pair_reader,
                            char* map, ssize_t map_len, size_t &used_map_bytes);
    /**
      *@brief Converts from a map stored as a vector of VMapPairs into the single-column map representation.
      *
      *@param vpairs          Contains the map pairs.
      *@param map             The destination char buffer to write the single-column map into.
       *@param map_len         The amount of space available to store the single-column map in the map.
      *@param used_map_bytes  Returns the number of Bytes used to store the just-created map.
      */
    static inline bool convert_vmap(ServerInterface &srvInterface, std::vector<VMapPair> &vpairs,
                            char* map, ssize_t map_len, size_t &used_map_bytes);
};

//////////////////////// END VMapBlockWriter Declaration ///////////////////////

////////////////////////////////////////////////////////////////////////////////
///////////////////////////// END Full VMap Helpers ////////////////////////////
////////////////////////////////////////////////////////////////////////////////





/*******************************************************************************
********************************************************************************
************************ END VMAP INTERFACE DECLARATION ************************
********************************************************************************
*******************************************************************************/





////////////////////////////////////////////////////////////////////////////////
///////////////////////////// BEGIN String Helpers /////////////////////////////
////////////////////////////////////////////////////////////////////////////////


////////////////////////// BEGIN String/Length Structs /////////////////////////

///@brief Convenience struct wrapping a char *, its length, and how much if it
///   is in use.
struct MutableStringPtr {
    MutableStringPtr() : ptr(NULL), tot_size(0), used_len(0) {}
    MutableStringPtr(char* string, size_t size, size_t str_len = 0) : ptr(string), tot_size(size), used_len(str_len) {}
    MutableStringPtr(std::string string) : ptr(string.c_str()), tot_size(string.length()), used_len(string.length()) {}
    void set(const char* string, size_t size) { ptr = string; tot_size = size; used_len = size; }

    const char* ptr;
    size_t tot_size;
    size_t used_len;
};
//bool operator<(const MutableStringPtr& p1, const MutableStringPtr& p2);

///@brief Convenience struct wrapping a char * const and its length.
struct ImmutableStringPtr {
    ImmutableStringPtr() : ptr(NULL), len(0) {}
    ImmutableStringPtr(const char* const string, size_t length) : ptr(string), len(length) {}
    ImmutableStringPtr(const ImmutableStringPtr &rhs) : ptr(rhs.ptr), len(rhs.len) {}

    const char* const ptr;
    size_t len;
};
bool operator<(const MutableStringPtr& p1, const MutableStringPtr& p2);
bool operator<(const ImmutableStringPtr& p1, const ImmutableStringPtr& p2);

typedef std::map<ImmutableStringPtr, size_t> ColLkup;

/////////////////////////// END String/Length Structs //////////////////////////


//////////////////////// BEGIN String Whitespace Helpers ///////////////////////
/*
 * Trims whitespace from the start and end of a passed string.
 */
std::string trim(std::string input);
/*
 * Trims whitespace from the start and end of a passed string.
 */
void trim_buf_ends(char** base, size_t* bound);
/*
 * Trims whitespace from the start and end of a passed string.
 */
void trim_buf_ends(const char** base, size_t* bound);
///////////////////////// END String Whitespace Helpers ////////////////////////


//////////////////////// BEGIN String Comparison Helpers ///////////////////////
typedef int (*string_comparer)(const char*, const char*, size_t);


class NormalizedKeyComparator {
public:
    NormalizedKeyComparator(size_t fieldSize, bool norm=true) : normalized(norm), maxSize(fieldSize) {}

    void computeNormalizedString(const char* s, size_t l, MutableStringPtr &dest, std::vector<char> &optBuf)
    {
        // scan first, checking to see if tolower will have any impact
        size_t currLett = 0;
        while (currLett < l && s[currLett] == tolower(s[currLett])) currLett++;
        // nothing to do
        if (currLett == l) dest.set(s, l);
        // otherwise, make space
        if (l > optBuf.capacity()) { 
            optBuf.reserve(std::max((long unsigned int)BASE_RESERVE_SIZE, // min reservation
                                    std::min(maxSize,                // do not exceed field width
                                             optBuf.capacity()*2))); // otherwise double
        }
        // indicate output location
        char* out = &optBuf[0];
        dest.set(out, l);
        // copy over characters that don't need to change
        memcpy(out, s, currLett);
        // tolower the remainder
        for (size_t i=currLett; i < l; i++) {
            out[i] = tolower(s[i]);
        }
    }
    void setLeft(const char* s, size_t len) {
        if (normalized) {
            computeNormalizedString(s, len, left, leftBuf);
        } else {
            left.set(s, len);
        }
    }
    void setRight(const char* s, size_t len) {
        if (normalized) {
            computeNormalizedString(s, len, right, rightBuf);
        } else {
            right.set(s, len);
        }
    }
    int cmp() {
        int result = strncmp(left.ptr, right.ptr, left.used_len);
        if (result == 0) return left.used_len == right.used_len ? 0 : (left.used_len < right.used_len ? -1 : 1);
        else return result;
    }
    bool equal() { return left.used_len == right.used_len && strncmp(left.ptr, right.ptr, left.used_len) == 0; }

private:
    bool normalized;
    size_t maxSize;
    MutableStringPtr left;
    MutableStringPtr right;

    std::vector<char> leftBuf;
    std::vector<char> rightBuf;
};

/*
 * Non-destructive helper method which sets key_destination to a a normalized copy of key_source.
 * For now, this just means lower-casing English letters.
 */
static inline void normalize_key(const char* const key_source, uint16 key_len, char* const key_destination) {
    for (int currLett = 0; currLett < key_len; currLett++) {
        key_destination[currLett] = tolower(key_source[currLett]);
    }
    key_destination[key_len] = '\0';
}
///////////////////////// END String Comparison Helpers ////////////////////////


////////////////////////////////////////////////////////////////////////////////
////////////////////////////// END String Helpers //////////////////////////////
////////////////////////////////////////////////////////////////////////////////





/*******************************************************************************
********************************************************************************
************************ BEGIN VMAP INTERFACE DEFINITION ***********************
********************************************************************************
*******************************************************************************/





////////////////////////////////////////////////////////////////////////////////
/////////////////////// BEGIN Map Pair Read/Write Helpers //////////////////////
////////////////////////////////////////////////////////////////////////////////


//////////////////////// BEGIN VMapPairWriter Definition ///////////////////////
inline VMapPairWriter::VMapPairWriter(char* buf, size_t buf_size)
    : buf(buf), buf_size(((buf_size < 0) ? 0 : buf_size)), buf_used(0) {}

inline bool VMapPairWriter::append(ServerInterface &srvInterface, const VMapPair &pair) {
    // Faster than appending the key and value individually
    // because we have the raw data in a helpful format
    if (pair.record_length() <= buf_size - buf_used) {
        memcpy(&buf[buf_used], pair.record_data(), pair.record_length());
        buf_used += pair.record_length();
        // Key in the pair is not null terminated
        // Copy (a prefix of) key for logging purposes
        std::string key(pair.key_str(), std::min(pair.key_length(),
                                                 (uint32) LOG_TRUNCATE_LENGTH));
        LogDebugUDBasic("Success:  Pair with key: [%s] is the correct size to fit into the buffer from a pair with: [%zu] space left.", key.c_str(), (buf_size - buf_used));
        return true;
    } else {
        // Key in the pair is not null terminated
        // Copy (a prefix of) key for logging purposes
        std::string key(pair.key_str(), std::min(pair.key_length(),
                                                 (uint32) LOG_TRUNCATE_LENGTH));
        LogDebugUDWarn("Error:  Pair with key: [%s] is too big to fit into the buffer from a pair; row to be rejected. Requested adding: [%zu] Bytes to a buffer with: [%zu] space left.", key.c_str(), pair.record_length(), (buf_size - buf_used));
        LogDebugUDBasic("Error:  Pair with key: [%s] is too big to fit into the buffer from a pair; row to be rejected. Requested adding: [%zu] Bytes to a buffer with: [%zu] space left.", key.c_str(), pair.record_length(), (buf_size - buf_used));
        return false;
    }
}

inline bool VMapPairWriter::append(ServerInterface &srvInterface,
                   const char* key,
                   uint32 key_len,
                   bool is_null,
                   const char* value,
                   uint32 value_len)
{
    size_t sz = VMapPair::serialize(key, key_len,
                          is_null, value, value_len,
                          buf + buf_used, buf_size - buf_used);

    if (sz != 0) {
        buf_used += sz;
        // key is not null terminated
        // Copy (a prefix of) key for logging purposes
        std::string keyStr(key, std::min(key_len, (uint32) LOG_TRUNCATE_LENGTH));
        LogDebugUDBasic("Success:  Pair with key: [%s] is the correct size to fit into the buffer from strings with: [%zu] space left.", keyStr.c_str(), (buf_size - buf_used));
        return true;
    } else {
        // key is not null terminated
        // Copy (a prefix of) key for logging purposes
        std::string keyStr(key, std::min(key_len, (uint32) LOG_TRUNCATE_LENGTH));
        LogDebugUDWarn("Error (buffer too small):  Pair with key: [%s] is too big to fit into the buffer from strings; row to be rejected. Requested adding: [%zu] Bytes to a buffer with: [%zu] space left.", keyStr.c_str(), VMapPair::size_needed(key_len, value_len), (buf_size - buf_used));
        LogDebugUDBasic("Error (buffer too small):  Pair with key: [%s] is too big to fit into the buffer from strings; row to be rejected. Requested adding: [%zu] Bytes to a buffer with: [%zu] space left.", keyStr.c_str(), VMapPair::size_needed(key_len, value_len), (buf_size - buf_used));
        return false;
    }
}

inline char* VMapPairWriter::get_buf() { return buf; }
inline size_t VMapPairWriter::get_buf_used() { return buf_used; }
inline size_t VMapPairWriter::get_buf_total_size() { return buf_size; }

inline void VMapPairWriter::clear() { buf_used = 0; }

//////////////////////// BEGIN VMapPairWriter Definition ///////////////////////


//////////////////////// BEGIN VMapPairReader Definition ///////////////////////
inline VMapPairReader::VMapPairReader(const VString& map_data)
    : pairs() { setup_map_data(map_data); }
inline VMapPairReader::VMapPairReader(const char* buf, size_t len)
    : pairs() { setup_map_data(buf, buf + len); }
inline VMapPairReader::VMapPairReader(VMapPairWriter pairWriter)
    : pairs() { setup_map_data(pairWriter.get_buf(), pairWriter.get_buf() + pairWriter.get_buf_used()); }


inline void VMapPairReader::setup_map_data(const VString& str) {
    if (!str.isNull()) {
        const char* start = str.data();
        const char* end = str.data() + str.length();
        setup_map_data(start, end);
    }
}

inline void VMapPairReader::setup_map_data(const char* start, const char* end) {
    char const* curr = start;

    while (curr < end) {
        VMapPair pair(curr);
        pairs.push_back(pair);
        curr += pair.record_length();
    }
}
///////////////////////// END VMapPairReader Definition ////////////////////////

////////////////////////////////////////////////////////////////////////////////
//////////////////////// END Map Pair Read/Write Helpers ///////////////////////
////////////////////////////////////////////////////////////////////////////////




////////////////////////////////////////////////////////////////////////////////
//////////////////////////// BEGIN Full VMap Helpers ///////////////////////////
////////////////////////////////////////////////////////////////////////////////

/////////////////////// BEGIN VMapBlockReader Definition ///////////////////////
inline VMapBlockReader::VMapBlockReader(ServerInterface &srvInterface, const VString& dictblock) {
    if (!is_valid_map(srvInterface, dictblock)) {
        full_block = NULL;
        value_index = NULL;

        keys_block = NULL;
        key_length = 0;
        key_index = NULL;
        vt_report_error(0, "Tried to retrieve Flex Table data from a cell not containing an Flex Table map.");
    } else {
        full_block = reinterpret_cast<const MapV1ValuesBlock*>(dictblock.data());
        value_index = reinterpret_cast<const uint32*>(full_block->value_lookup);

        keys_block = reinterpret_cast<const MapV1KeysBlock*>(dictblock.data() + full_block->get_values_part_size());
        key_length = dictblock.length() - full_block->get_values_part_size();
        key_index = reinterpret_cast<const uint32*>(keys_block->key_lookup);

        if (full_block->get_values_part_size() + 4 * keys_block->key_count > dictblock.length())
            vt_report_error(0, "Tried to retrieve Flex Table data from a cell not containing an Flex Table map.");
    }
}

inline VMapBlockReader::VMapBlockReader(ServerInterface &srvInterface, EE::StringValue& dictblock) {
    if (!is_valid_map(srvInterface, dictblock)) {
        full_block = NULL;
        value_index = NULL;

        keys_block = NULL;
        key_length = 0;
        key_index = NULL;
        vt_report_error(0, "Tried to retrieve Flex Table data from a cell not containing an Flex Table map.");
    } else {
        full_block = reinterpret_cast<const MapV1ValuesBlock*>(dictblock.inlinePtr());
        value_index = reinterpret_cast<const uint32*>(full_block->value_lookup);

        keys_block = reinterpret_cast<const MapV1KeysBlock*>(dictblock.inlinePtr() + full_block->get_values_part_size());
        key_length = dictblock.slen - full_block->get_values_part_size();
        key_index = reinterpret_cast<const uint32*>(keys_block->key_lookup);

        if (full_block->get_values_part_size() + 4 * keys_block->key_count > dictblock.slen)
            vt_report_error(0, "Tried to retrieve Flex Table data from a cell not containing an Flex Table map.");
    }
}

inline VMapBlockReader::VMapBlockReader(ServerInterface &srvInterface, const char* map_data, const vsize map_size) {
    if (!is_valid_map(srvInterface,
                    map_size,
                    reinterpret_cast<const MapV1ValuesBlock*>(map_data))) {
        full_block = NULL;
        value_index = NULL;

        keys_block = NULL;
        key_length = 0;
        key_index = NULL;
        vt_report_error(0, "Tried to retrieve Flex Table data from a string not containing an Flex Table map.");
    } else {
        full_block = reinterpret_cast<const MapV1ValuesBlock*>(map_data);
        value_index = reinterpret_cast<const uint32*>(full_block->value_lookup);

        keys_block = reinterpret_cast<const MapV1KeysBlock*>(map_data + full_block->get_values_part_size());
        key_length = map_size - full_block->get_values_part_size();
        key_index = reinterpret_cast<const uint32*>(keys_block->key_lookup);

        if (full_block->get_values_part_size() + 4 * keys_block->key_count > map_size)
            vt_report_error(0, "Tried to retrieve Flex Table data from a cell not containing an Flex Table map.");
    }
}

inline uint32 VMapBlockReader::get_count() { return ((keys_block) ? keys_block->key_count : 0); }

inline uint32 VMapBlockReader::get_key_size(uint32 keyIndex) {
    const uint32 start = get_key_start_loc(keyIndex);
    const uint32 end = get_key_end_loc(keyIndex);
    if (!keys_block || start > end) {
        return 0;
    } else {
        return (end - start);
    }
}

inline const char* VMapBlockReader::get_key(uint32 keyIndex) {
    const uint32 offset = get_key_start_loc(keyIndex);
    if (!keys_block || !isKeyOffsetValid(offset)) {
        return NULL;
    }
    return &(keys_block->key_lookup[offset - sizeof(uint32) /*key count*/]);
}

inline uint32 VMapBlockReader::get_value_size(uint32 valueIndex) {
    const uint32 start = get_value_start_loc(valueIndex);
    const uint32 end = get_value_end_loc(valueIndex);
    if (!keys_block || start > end) {
        return 0;
    } else {
        return (end - start);
    }
}

inline const char* VMapBlockReader::get_value(uint32 valueIndex) {
    const uint32 offset = get_value_start_loc(valueIndex);
    if (!keys_block || value_index[valueIndex] == std::numeric_limits<uint32>::max() ||
        // Sanity check on value offset
        !isValueOffsetValid(offset)) {
        return NULL;
    }
    return &(full_block->value_lookup[offset - sizeof(uint32) /*value count*/]);
}

inline int32 VMapBlockReader::search(const VString& key, bool case_sensitive) {
    if (!keys_block || key.isNull())
        return -1;
    size_t keyLen = key.length();

    if (case_sensitive) {
        // Simple linear search for now
        for (uint32 keyIndex = 0; keyIndex < keys_block->key_count; keyIndex++) {
            if (keyLen == get_key_size(keyIndex)) {
                const char *curKey = get_key(keyIndex);
                if (curKey && strncmp(key.data(), curKey, keyLen) == 0) {
                    if (value_index[keyIndex] == std::numeric_limits<uint32>::max() ||
                        !isValueOffsetValid(value_index[keyIndex])) {
                        return -2;  // NULL value
                    } else {
                        return keyIndex;
                    }
                }
            }
        }

    } else {
        // Simple linear search for now
        for (uint32 keyIndex = 0; keyIndex < keys_block->key_count; keyIndex++) {
            if (keyLen == get_key_size(keyIndex)) {
                const char *currKey = get_key(keyIndex);
                if (currKey && strncasecmp(key.data(), currKey, keyLen) == 0) {
                    if (value_index[keyIndex] == std::numeric_limits<uint32>::max() ||
                        !isValueOffsetValid(value_index[keyIndex])) {
                        return -2;  // NULL value
                    } else {
                        return keyIndex;
                    }
                }
            }
        }
    }

    // Didn't find the key
    return -1;
}

inline bool VMapBlockReader::contains_key(const VString& key, bool case_sensitive) {
    return (search(key, case_sensitive) != -1);
}

/// BEGIN to_string
inline std::string VMapBlockReader::compute_to_string_canonical(ServerInterface &srvInterface) {
    if (!keys_block) {
        return "";
    }

    Json::Value jsonMap = compute_json_map(srvInterface);
    Json::StyledWriter styledWriter;
    return styledWriter.write(jsonMap);
}

inline Json::Value VMapBlockReader::compute_json_map(ServerInterface &srvInterface) {
    Json::Value jsonMap = Json::objectValue;
    if (keys_block) {
        for (uint32 keyIndex = 0; keyIndex < keys_block->key_count; keyIndex++) {
            std::string keyName = std::string(get_key(keyIndex), get_key_size(keyIndex));
            if (!jsonMap.isMember(keyName)) {
                const char* currValue = get_value(keyIndex);
                if (is_valid_map(srvInterface, keyIndex, false)) {
                    VMapBlockReader subMapReader(srvInterface, currValue, get_value_size(keyIndex));
                    jsonMap[keyName] = subMapReader.compute_json_map(srvInterface);
                } else {
                    jsonMap[keyName] = ((currValue != NULL) ? std::string(currValue, get_value_size(keyIndex)) : Json::Value::null);
                }
            }
        }
    }

    return jsonMap;
}

inline std::string VMapBlockReader::compute_to_string_manual(ServerInterface &srvInterface, uint indent_level) {
    if (!keys_block) {
        return "";
    }

    const char indent = '\t';
    const char* const keySep = ":\t";
    const char* const recordSep = ",\n";
    const char* const subMapSep = "\n";
    const char* const mapStart = "{\n";
    const char* const mapPrefix = " {\n";
    const char* const mapSuffix = " }";
    const char* const stringPrefix = "\"";
    const char* const stringSuffix = "\"";

    std::ostringstream toReturn;
    for (size_t i=0; i < indent_level; i++) toReturn << indent;
    if (!indent_level) toReturn << mapStart;
    else toReturn << mapPrefix;

    for (uint32 keyIndex = 0; keyIndex < keys_block->key_count; keyIndex++) {
        if (keyIndex > 0)
            toReturn << recordSep;

        const char* currKey = get_key(keyIndex);
        uint32 currKeySize = get_key_size(keyIndex);
        const char* currValue = get_value(keyIndex);
        uint32 currValueSize = get_value_size(keyIndex);
        for (size_t i=0; i < indent_level+1; i++) toReturn << indent;
        toReturn << stringPrefix;
        toReturn.write(currKey, currKeySize);
        toReturn << stringSuffix << keySep;
        if (is_valid_map(srvInterface, keyIndex, false)) {
            toReturn << subMapSep;
            VMapBlockReader subMapReader(srvInterface, currValue, currValueSize);
            toReturn << subMapReader.to_string(srvInterface, false, indent_level + 1);
        } else {
            if (currValue == NULL) {
                toReturn << "null";
            } else {
                toReturn << stringPrefix;
                toReturn.write(currValue, currValueSize);
                toReturn << stringSuffix;
            }
        }
    }

    toReturn << subMapSep;
    for (size_t i=0; i < indent_level; i++) toReturn << indent;
    toReturn << mapSuffix;
    return toReturn.str();
}

inline std::string VMapBlockReader::to_string(ServerInterface &srvInterface,
                                             bool canonicalJSON,
                                             uint indent_level) {
    if (canonicalJSON) {
        return compute_to_string_canonical(srvInterface);
    } else {
        return compute_to_string_manual(srvInterface, indent_level);
    }
}
//// END to_string

//// BEGIN map validation
inline const int VMapBlockReader::map_version(ServerInterface &srvInterface, const VString& dictblock) {
    if (!is_valid_map(srvInterface,
            dictblock,
            false))
    {
        return -1;
    } else {
        return reinterpret_cast<const MapBlock*>(dictblock.data())->map_version;
    }
}

inline const bool VMapBlockReader::is_valid_map(ServerInterface &srvInterface, uint32 valueIndex, bool logInvalidMaps) {
    return is_valid_map(srvInterface,
            get_value_size(valueIndex),
            reinterpret_cast<const MapV1ValuesBlock*>(get_value(valueIndex)),
            logInvalidMaps);
    }

inline const bool VMapBlockReader::is_valid_map(ServerInterface &srvInterface, const VString& dictblock, bool logInvalidMaps) {
    if (dictblock.length() < 4 || dictblock.isNull())
        return false;
    return is_valid_map(srvInterface,
            dictblock.length(),
            reinterpret_cast<const MapV1ValuesBlock*>(dictblock.data()),
            logInvalidMaps);
    }

inline const bool VMapBlockReader::is_valid_map(ServerInterface &srvInterface, EE::StringValue& dictblock, bool logInvalidMaps) {
    if (dictblock.slen < 4)
        return false;
    return is_valid_map(srvInterface,
            dictblock.slen,
            reinterpret_cast<const MapV1ValuesBlock*>(dictblock.inlinePtr()),
            logInvalidMaps);
}

inline const bool VMapBlockReader::is_valid_map(ServerInterface &srvInterface,
                                    vsize length,
                                    const MapV1ValuesBlock* mapBlock,
                                    bool logInvalidMaps) {

    if (mapBlock == NULL ||                                               // Do we actually have any data?
            length < 4)                                                   // Do we have enough data at least for the header?
    {
        if (logInvalidMaps) {
            log("VMapBlockReader.is_valid_map() found an invalid map with checks: mapBlock == NULL: [%d], length < 4: [%d]", (mapBlock == NULL), (length < 4));
        } else {
            LogDebugUDBasic("VMapBlockReader.is_valid_map() found an invalid map with checks: mapBlock == NULL: [%d], length < 4: [%d]", (mapBlock == NULL), (length < 4));
        }
        return false;
    }

    if (mapBlock->map_version < 1 ||                                      // Do we have a valid smallest-supported map version?
            mapBlock->map_version > 1 ||                                  // Do we have a valid largest-supported map version?
            mapBlock->value_length + MAP_BLOCK_HEADER_SIZE > length ||    // Do we have enough data for the keys header?
            mapBlock->value_count < 0 ||                                  // Do we have a valid count of values?
            MAP_BLOCK_HEADER_SIZE + sizeof(uint32) + (sizeof(uint32) * mapBlock->value_count) > length)   // Do we have key offsets for each key?
    {
        if (logInvalidMaps) {
            log("VMapBlockReader.is_valid_map() found an invalid map with checks: mapBlock->map_version < 1: [%d], mapBlock->map_version > 1: [%d], mapBlock->value_length + MAP_BLOCK_HEADER_SIZE > length: [%d], mapBlock->value_count < 0: [%d], and MAP_BLOCK_HEADER_SIZE + sizeof(uint32) + (sizeof(uint32) * mapBlock->value_count) > length: [%d]", (mapBlock->map_version < 1), (mapBlock->map_version > 1), (mapBlock->value_length + MAP_BLOCK_HEADER_SIZE > length), (mapBlock->value_count < 0), (MAP_BLOCK_HEADER_SIZE + sizeof(uint32) + (sizeof(uint32) * mapBlock->value_count) > length));
        } else {
            LogDebugUDBasic("VMapBlockReader.is_valid_map() found an invalid map with checks: mapBlock->map_version < 1: [%d], mapBlock->map_version > 1: [%d], mapBlock->value_length + MAP_BLOCK_HEADER_SIZE > length: [%d], mapBlock->value_count < 0: [%d], and MAP_BLOCK_HEADER_SIZE + sizeof(uint32) + (sizeof(uint32) * mapBlock->value_count) > length: [%d]", (mapBlock->map_version < 1), (mapBlock->map_version > 1), (mapBlock->value_length + MAP_BLOCK_HEADER_SIZE > length), (mapBlock->value_count < 0), (MAP_BLOCK_HEADER_SIZE + sizeof(uint32) + (sizeof(uint32) * mapBlock->value_count) > length));
        }
        return false;
    }

    const MapV1KeysBlock* keysBlock = reinterpret_cast<const MapV1KeysBlock*>(((char*)mapBlock) + mapBlock->get_values_part_size());
    const uint32* temp_key_index = reinterpret_cast<const uint32*>(keysBlock->key_lookup);
    if (keysBlock->key_count < 0 ||                                                 // Do we have a valid count of keys?
            mapBlock->value_count != keysBlock->key_count ||                        // Do we have the same number of keys and values?
            mapBlock->get_values_part_size() + sizeof(uint32) + (sizeof(uint32) * keysBlock->key_count) > length || // Do we have key offsets for each key?
            (keysBlock->key_count > 0 && mapBlock->get_values_part_size() + temp_key_index[keysBlock->key_count-1] > length)    // Does the beginning of the last key fall within limits?
        )
    {
        if (logInvalidMaps) {
            if (mapBlock->value_count == keysBlock->key_count) {
                log("VMapBlockReader.is_valid_map() found an invalid map with checks: keysBlock->key_count < 0: [%d], mapBlock->value_count != keysBlock->key_count: [%d], mapBlock->get_values_part_size() + sizeof(uint32) + (sizeof(uint32) * keysBlock->key_count) > length: [%d] and (keysBlock->key_count > 0 && mapBlock->get_values_part_size() + temp_key_index[keysBlock->key_count-1] > length): [%d]",
                    (keysBlock->key_count < 0),
                    (mapBlock->value_count != keysBlock->key_count),
                    (mapBlock->get_values_part_size() + sizeof(uint32) + (sizeof(uint32) * keysBlock->key_count) > length),
                    (keysBlock->key_count > 0 && mapBlock->get_values_part_size() + temp_key_index[keysBlock->key_count-1] > length));
            } else {
                log("VMapBlockReader.is_valid_map() found an invalid map with checks: keysBlock->key_count < 0: [%d], mapBlock->value_count != keysBlock->key_count: [%d], mapBlock->get_values_part_size() + sizeof(uint32) + (sizeof(uint32) * keysBlock->key_count) > length: [%d], or maybe the beginning of the last key is out-of-bounds.",
                    (keysBlock->key_count < 0),
                    (mapBlock->value_count != keysBlock->key_count),
                    (mapBlock->get_values_part_size() + sizeof(uint32) + (sizeof(uint32) * keysBlock->key_count) > length));
            }
        } else {
            if (mapBlock->value_count == keysBlock->key_count) {
                LogDebugUDBasic("VMapBlockReader.is_valid_map() found an invalid map with checks: keysBlock->key_count < 0: [%d], mapBlock->value_count != keysBlock->key_count: [%d], mapBlock->get_values_part_size() + sizeof(uint32) + (sizeof(uint32) * keysBlock->key_count) > length: [%d] and (keysBlock->key_count > 0 && mapBlock->get_values_part_size() + temp_key_index[keysBlock->key_count-1] > length): [%d]",
                    (keysBlock->key_count < 0),
                    (mapBlock->value_count != keysBlock->key_count),
                    (mapBlock->get_values_part_size() + sizeof(uint32) + (sizeof(uint32) * keysBlock->key_count) > length),
                    (keysBlock->key_count > 0 && mapBlock->get_values_part_size() + temp_key_index[keysBlock->key_count-1] > length));
            } else {
                LogDebugUDBasic("VMapBlockReader.is_valid_map() found an invalid map with checks: keysBlock->key_count < 0: [%d], mapBlock->value_count != keysBlock->key_count: [%d], mapBlock->get_values_part_size() + sizeof(uint32) + (sizeof(uint32) * keysBlock->key_count) > length: [%d], or maybe the beginning of the last key is out-of-bounds.",
                    (keysBlock->key_count < 0),
                    (mapBlock->value_count != keysBlock->key_count),
                    (mapBlock->get_values_part_size() + sizeof(uint32) + (sizeof(uint32) * keysBlock->key_count) > length));
            }

        }
        return false;
    }
    return true;
}
/// END map validation


/// BEGIN lookup helpers
inline uint32 VMapBlockReader::get_key_end_loc(uint32 keyIndex) {
    if (!keys_block) {
        return 0;
    }
    if ((uint32)keyIndex < keys_block->key_count - 1) {
        const uint32 offset = key_index[keyIndex + 1];
        if (isKeyOffsetValid(offset)) {
            return offset;
        } else {
            return 0;
        }
    } else {
        return key_length;
    }
}

inline uint32 VMapBlockReader::get_key_start_loc(uint32 keyIndex) {
    const uint32 offset = key_index[keyIndex];
    if (!keys_block || !isKeyOffsetValid(offset)) {
        return 0;
    } else {
        return offset;
    }
}

inline uint32 VMapBlockReader::get_value_start_loc(uint32 valueIndex) {
    if (!keys_block) {
        return 0;
    }
    if (value_index[valueIndex] == std::numeric_limits<uint32>::max()) {
        return get_value_end_loc(valueIndex);
    } else {
        if (isValueOffsetValid(value_index[valueIndex])) {
            return value_index[valueIndex];
        } else {
            return 0;
        }
    }
}
//// END lookup helpers

//////////////////////// END VMapBlockReader Definition ////////////////////////


/////////////////////// BEGIN VMapBlockWriter Definition ///////////////////////
/*
 * Helper for case comparing
 */
inline bool VMapBlockWriter::VMapPairInsensitiveCompare(const VMapPair &pair0, const VMapPair &pair1)
{
    int res = strncasecmp(pair0.key_str(), pair1.key_str(), std::min(pair0.key_length(), pair1.key_length()));
    return res < 0 || (res == 0 && pair0.key_length() < pair1.key_length());
}

/*
 * Helper for case comparing
 */
inline bool VMapBlockWriter::StringInsensitiveCompare::operator() (const std::string& a, const std::string& b) const {
    return strcasecmp(a.c_str(), b.c_str()) < 0;
}


/*
 * Converts from a map stored within a VMapPairReader into the single-column map representation.
 *
 * pair_reader:     Contains the map in pairs form.
 * map:             The destination char buffer to write the single-column map into.
 * map_len:         The amount of space available to store the single-column map in the map.
 * used_map_bytes:  Returns the number of Bytes used to store the just-created map.
 *
 * Convenience wrapper for emptymap().
 */
inline bool VMapBlockWriter::convert_vmap(ServerInterface &srvInterface, VMapPairReader &pair_reader,
                        char* map, ssize_t map_len, size_t &used_map_bytes)
{
    return convert_vmap(srvInterface, pair_reader.get_pairs(), map, map_len, used_map_bytes);
}


/*
 * Converts from a map stored as a vector of VMapPairs into the single-column map representation.
 *
 * vpairs:          Contains the map pairs.
 * map:             The destination char buffer to write the single-column map into.
 * map_len:         The amount of space available to store the single-column map in the map.
 * used_map_bytes:  Returns the number of Bytes used to store the just-created map.
 */
inline bool VMapBlockWriter::convert_vmap(ServerInterface &srvInterface, std::vector<VMapPair> &vpairs,
                        char* map, ssize_t map_len, size_t &used_map_bytes)
{
    LogDebugUDBasic("About to attempt generating a map with %zu pairs.", vpairs.size());

    used_map_bytes = 0;

    uint32* value_index = (uint32*)map;
    uint32* data_start = value_index;
    // Space used by value (key) count and value (key) offset indexes
    int indexsize = ((vpairs.size() + 1) * sizeof(uint32));
    if (map_len < 0 || map_len < indexsize) {
        LogDebugUDWarn("VMapBlockWriter.convert_vmap() can't write map as it estimates it needs: [%d], Bytes and only: [%zu] are available.", indexsize, map_len);
        return false;
    }
    uint32 u_key   = indexsize;
    uint32 u_value = indexsize;

    std::sort(vpairs.begin(), vpairs.end(), VMapPairInsensitiveCompare);

    // Store header (version # and values length/boundary lookup)
    value_index[0] = ((uint32) 1);
    value_index++;
    value_index++; // The 2nd chunk will receive the length of the keys section once that has been summed below

    // Store number of values
    value_index[0] = vpairs.size();
    value_index++;

    // Process values in the first pass
    for (uint valNum = 0; valNum < vpairs.size(); valNum++) {
        VMapPair &mapPair = vpairs[valNum];

        // Check there is enough space left in output buffer
        if (MAP_BLOCK_HEADER_SIZE + u_value + mapPair.value_length() > map_len) {
            LogDebugUDWarn("Error (buffer too small):  VMapBlockWriter.convert_vmap() can't write map as it needs: [%d], Bytes and only:  [%zu] are available while processing non-null-valued key: [%s].", (MAP_BLOCK_HEADER_SIZE + u_value + mapPair.value_length()), map_len, std::string(mapPair.key_str(), mapPair.key_length()).c_str());
            return false;
        }
        // Compute and store values value_index lookup
        if (mapPair.is_null()) {
            value_index[valNum] = std::numeric_limits<uint32>::max();
        } else {
            value_index[valNum] = u_value;
        }
        // Store values
        memcpy(map + MAP_BLOCK_HEADER_SIZE + u_value, mapPair.value_str(), mapPair.value_length());
        u_value += mapPair.value_length();
    }

    // Store the length of the values section before the keys section starts
    data_start[1] = u_value; // value length: count + offsets + values
    uint32* key_index = ((uint32*) (map + MAP_BLOCK_HEADER_SIZE + u_value));

    // Store number of keys
    key_index[0] = vpairs.size();
    key_index++;

    // Process keys in the second pass
    for (uint keyNum = 0; keyNum < vpairs.size(); keyNum++) {
        VMapPair &mapPair = vpairs[keyNum];
        // Compute and store keys key_index lookup
        if (MAP_BLOCK_HEADER_SIZE + u_value + u_key + mapPair.key_length() > map_len) {
            LogDebugUDWarn("Error (buffer too small):  VMapBlockWriter.convert_vmap() can't write map as it needs: [%d], Bytes and only:  [%zu] are available.", (MAP_BLOCK_HEADER_SIZE + u_value + u_key + mapPair.key_length()), map_len);
            return false;
        }
        key_index[keyNum] = u_key;
        // Store key names
        memcpy(map + u_key + u_value + MAP_BLOCK_HEADER_SIZE, mapPair.key_str(), mapPair.key_length());
        u_key += mapPair.key_length();
    }

    used_map_bytes = u_key + u_value + MAP_BLOCK_HEADER_SIZE;
    LogDebugUDBasic("Just generated a map with %zu pairs.", vpairs.size());
    return true;
}
//////////////////////// END VMapBlockWriter Definition ////////////////////////







/*******************************************************************************
********************************************************************************
***************************** BEGIN VMAP INTERFACE *****************************
********************************************************************************
*******************************************************************************/
/// XXX TODO XXX inlining choices XXX TODO XXX

/////////////////////////////// BEGIN Base VMap Interface /////////////////////////////
/*
 * Factory class which produces VMap interface instances for working with
 *    Flexible Tables' Vertica Maps.
 */
class VMapFactory {
//XXX TODO XXX
};


/*
 * Interface including the VMap functionality common to all VMap instances for
 *    working with Flexible Tables' Vertica Maps.
 *
 * Likely want to use strncmp or strincmp for the template parameter.
 */
class VMap {
public:
/// BEGIN initializers
//    /*
//     * Default constructor to create an empty VMap.
//     */
//    VMap();
//    /*
//     * Constructor which accepts and loads the passed MapBlock, stored in VString form.
//     *
//     * Throws on non-VMap mapBlocks.
//     */
//    VMap(VString& mapBlock);
//    /*
//     * Constructor which accepts and loads the passed MapBlock, stored in char* form.
//     *
//     * Throws on non-VMap mapBlocks.
//     */
//    VMap(ImmutableStringPtr mapBlock);
    /*
     * Destructor to keep old gcc (vdev) happy by removing the "has virtual
     *  functions but non-virtual destructor" warning, sinced removed from gcc:
     *    http://stackoverflow.com/questions/127426/gnu-compiler-warning-class-has-virtual-functions-but-non-virtual-destructor.
     */
    virtual inline ~VMap() {}
/// END initializers

/// BEGIN full-map writers
    /*
     * Writes the data contained in this VMap into a VMapBlock for storing.
     *
     * map_buf:             The destination VString buffer to write the VMap into.
     *
     * map_bytes_used:      Returns the number of Bytes used to store the just-created map.
     *
     *  XXX QUESTION XXX Move out into a static in the flextable namespace?
     */
    virtual inline bool writeVMapBlock(VString& map_buf,
                               size_t& map_bytes_used) = 0;
    /*
     * Writes the data contained in this VMap into a VMapBlock for storing.
     *
     * map_buf:             The destination char* buffer to write the VMap into.
     *
     * map_bytes_used:      Returns the number of Bytes used to store the just-created map.
     *
     *  XXX QUESTION XXX Move out into a static in the flextable namespace?
     */
    virtual inline bool writeVMapBlock(MutableStringPtr map_buf,
                               size_t& map_bytes_used) = 0;
/// END full-map writers

/// BEGIN pair accessors
    /*
     * Returns whether the VMap is empty (i.e. whether its size is 0).
     */
    virtual bool empty() const = 0;
    /*
     * Returns the number of pairs in the VMap.
     */
    virtual uint32 size() const = 0;

    /*
     * Accessor to retrieve values by key name.
     */
    virtual const ImmutableStringPtr at(VString& key_name) const = 0;
    /*
     * Accessor to retrieve values by key name.
     */
    virtual ImmutableStringPtr at(const ImmutableStringPtr key_name) = 0;
    /*
     * Accessor to retrieve values by key name.
     */
    virtual const ImmutableStringPtr at(const ImmutableStringPtr key_name) const = 0;
    /*
     * Returns the index of the pair with the given key stored in this VMap.
     *
     * XXX QUESTION XXX Remove as not part of std::map<,> in favor of the above?
     */
    virtual inline int64 search(const VString& key_name, bool case_sensitive = true) = 0;
    /*
     * Returns a count of keys with the given name in this VMap.
     */
    virtual uint32 count(const VString& key_name, bool case_sensitive = true) const = 0;
    /*
     * Returns a count of keys with the given name in this VMap.
     */
    virtual uint32 count(const ImmutableStringPtr key_name, bool case_sensitive = true) const = 0;
/// END pair accessors

/// BEGIN map validation
    /*
     * Returns the integer version of the passed VMap in on-disk format
     *  contained in the passed VString, or -1 if it does not contain a
     *  valid map.
     *
     *  XXX QUESTION XXX Move out into the flextable namespace?
     */
    static inline const int map_version(const VString& mapBlock);

    /*
     * Returns the integer version of the passed VMap in on-disk format
     *  contained in the passed VString, or -1 if it does not contain a
     *  valid map.
     *
     *  XXX QUESTION XXX Remove?
     */
    inline const int map_version();
    /*
     * Returns whether the VMap in on-disk format contained in this
     *  VMap instance is a valid map.
     *
     *  XXX QUESTION XXX Move out into a static in the flextable namespace?
     */
    virtual inline const bool is_valid_map(bool logInvalidMaps = false) = 0;
    /*
     * Returns whether the VMap in on-disk format contained in this 
     *  VMap instance is a valid map.
     *
     *  XXX QUESTION XXX Move out into a static in the flextable namespace?
     */
    virtual inline const bool is_valid_map(uint32 value_index, bool logInvalidMaps = false) = 0;
    /*
     * Returns whether the passed VMap in on-disk format contained in the passed
     *  VString is a valid map.
     *
     *  XXX QUESTION XXX Move out into the flextable namespace?
     */
    static inline const bool is_valid_map(const VString& mapBlock, bool logInvalidMaps = false);
    /*
     * Returns whether the passed VMap in on-disk format contained in the passed
     *  char* is a valid map.
     *
     *  XXX QUESTION XXX Move out into the flextable namespace?
     */
    static inline const bool is_valid_map(ImmutableStringPtr mapBlock, bool logInvalidMaps = false);
/// END map validation
};
/////////////////////////////// END Base VMap Interface /////////////////////////////



/////////////////////////////// BEGIN Immutable VMap Interface /////////////////////////////
/*
 * Interface including the VMap functionality specific to read-only access of
 *    a MapBlock (the on-disk VMap format).
 *
 * Likely want to use strncmp or strincmp for the template parameter.
 */
template <string_comparer string_compare>
class ImmutableVMap : public VMap {
public:
/// BEGIN initializers
//    /*
//     * Default constructor to create an empty VMap.
//     */
//    ImmutableVMap();
//    /*
//     * Constructor which accepts and loads the passed MapBlock, stored in VString form.
//     *
//     * Throws on non-VMap mapBlocks.
//     */
//    ImmutableVMap(VString& mapBlock);
//    /*
//     * Constructor which accepts and loads the passed MapBlock, stored in char* form.
//     *
//     * Throws on non-VMap mapBlocks.
//     */
//    ImmutableVMap(ImmutableStringPtr mapBlock);
/// END initializers

/// BEGIN pair accessors
//    /*
//     * Returns the index of the pair with the given key stored in this VMap.
//     */
//    virtual inline int64 search(const VString& key_name, bool case_sensitive = true) = 0;
    /*
     * Returns an iterator pointing to the pair with the given key stored
     *    in this VMap.
     */
    virtual std::iterator<std::bidirectional_iterator_tag, VMapPair> find(const VString& key_name, bool case_sensitive = true) = 0;
//    /*
//     * Returns an iterator pointing to the pair at, if present, or just after
//     *    the passed key in this VMap.
//     */
//    virtual const_iterator find(const VString& key_name, bool case_sensitive = true) const = 0;
    /*
     * Returns an iterator pointing to the pair at, if present, or just after
     *    the passed key in this VMap.
     */
    virtual std::iterator<std::bidirectional_iterator_tag, VMapPair> lower_bound(const VString& key_name) = 0;
//    /*
//     * Returns an iterator pointing to the pair just after the passed key
//     *    in this VMap.
//     */
//    virtual const_iterator lower_bound(const VString& key_name) const = 0;
    /*
     * Returns an iterator pointing to the pair just after the passed key
     *    in this VMap.
     */
    virtual std::iterator<std::bidirectional_iterator_tag, VMapPair> upper_bound(const VString& key_name) = 0;
//    /*
//     * Returns an iterator pointing to the pair at or just after the passed
//     *    key in this VMap.
//     */
//    virtual const_iterator upper_bound(const VString& key_name) const = 0;
    /*
     * Returns a pair of iterators with the first pointing to the pair at, if
     *    present, or just after the passed key in this VMap (the output of
     *    lower_bound) and with the second pointing to the pair just after the
     *    passed key (the output of upper_bound) in this VMap.
     */
    virtual std::iterator<std::bidirectional_iterator_tag, VMapPair> equal_range(const VString& key_name) = 0;
    /*
     * Returns a pair of iterators with the first pointing to the pair at, if
     *    present, or just after the passed key in this VMap (the output of
     *    lower_bound) and with the second pointing to the pair just after the
     *    passed key (the output of upper_bound) in this VMap.
     */
    virtual std::iterator<std::bidirectional_iterator_tag, VMapPair> equal_range(ImmutableStringPtr key_name) = 0;
//    /*
//     * Returns a pair of iterators with the first pointing to the pair at, if
//     *    present, or just after the passed key in this VMap (the output of
//     *    lower_bound) and with the second pointing to the pair just after the
//     *    passed key (the output of upper_bound) in this VMap.
//     */
//    virtual pair<const_iterator,const_iterator> equal_range(const VString& key_name) const = 0;
//    /*
//     * Returns a pair of iterators with the first pointing to the pair at, if
//     *    present, or just after the passed key in this VMap (the output of
//     *    lower_bound) and with the second pointing to the pair just after the
//     *    passed key (the output of upper_bound) in this VMap.
//     */
//    virtual pair<const_iterator,const_iterator> equal_range(ImmutableStringPtr key_name) const = 0;


/// END pair accessors

/// BEGIN iterators
    /*
     * Returns an iterator referring to the first pair in the VMap.
     */
    virtual std::iterator<std::bidirectional_iterator_tag, VMapPair> begin() = 0;
//    /*
//     * Returns an iterator referring to the first pair in the VMap.
//     */
//    virtual const_iterator begin() const = 0;
//    /*
//     * Returns an iterator referring to the first pair in the VMap.
//     */
//    virtual const_iterator cbegin() const noexcept = 0;
    /*
     * Returns an iterator referring to the past-the-end pair in the VMap.
     */
    virtual std::iterator<std::bidirectional_iterator_tag, VMapPair> end() = 0;
//    /*
//     * Returns an iterator referring to the past-the-end pair in the VMap.
//     */
//    virtual const_iterator end() const = 0;
//    /*
//     * Returns an iterator referring to the first pair in the VMap.
//     */
//    virtual const_iterator cend() const noexcept = 0;
//
//    /*
//     * Returns a reverse-iterator referring to the last pair in the VMap
//     *    (i.e. its reverse beginning).
//     */
//    virtual std::reverse_iterator<(std::iterator<std::bidirectional_iterator_tag, VMapPair>)> rbegin() = 0;
//    /*
//     * Returns a reverse-iterator referring to the last pair in the VMap
//     *    (i.e. its reverse beginning).
//     */
//    virtual const_reverse_iterator rbegin() const = 0;
//    /*
//     * Returns a reverse-iterator referring to the last pair in the VMap
//     *    (i.e. its reverse beginning).
//     */
//    virtual const_reverse_iterator crbegin() const noexcept = 0;
//    /*
//     * Returns a reverse-iterator referring to the past-the-end pair in the VMap.
//     *    (i.e. its reverse end).
//     */
//    virtual std::reverse_iterator<std::bidirectional_iterator_tag, VMapPair> rend() = 0;
//    /*
//     * Returns a reverse-iterator referring to the past-the-end pair in the VMap.
//     *    (i.e. its reverse end).
//     */
//    virtual const_reverse_iterator rend() const = 0;
//    /*
//     * Returns a reverse-iterator referring to the past-the-end pair in the VMap.
//     *    (i.e. its reverse end).
//     */
//    virtual const_reverse_iterator crend() const noexcept = 0;
/// END iterators
};
/////////////////////////////// END Immutable VMap Interface /////////////////////////////

/////////////////////////////// BEGIN Mutable VMap Interface /////////////////////////////
/*
 * Interface including the VMap functionality specific to read-write access of
 *    an optional MapBlock plus the ability to add/remove pairs.
 *
 * Likely want to use strncmp or strincmp for the template parameter.
 */
template <string_comparer string_compare>
class MutableVMap : public VMap {
public:
/// BEGIN initializers
//    /*
//     * Default constructor to create an empty VMap.
//     */
//    MutableVMap();
//    /*
//     * Constructor which accepts and loads the passed MapBlock, stored in VString form.
//     *
//     * Throws on non-VMap mapBlocks.
//     */
//    MutableVMap(VString& mapBlock);
//    /*
//     * Constructor which accepts and loads the passed MapBlock, stored in char* form.
//     *
//     * Throws on non-VMap mapBlocks.
//     */
//    MutableVMap(ImmutableStringPtr mapBlock);
////    /*
////     * Constructor which accepts and loads the passed collection of VMapPairs.
////     */
////    MutableVMap(std::const_iterator<std::bidirectional_iterator_tag, VMapPair> first_pair, std::const_iterator<std::bidirectional_iterator_tag, VMapPair> last_pair);
//    /*
//     * Constructor which accepts and loads the passed collection of VMapPairs.
//     */
//    MutableVMap(std::iterator<std::bidirectional_iterator_tag, VMapPair> first_pair, std::iterator<std::bidirectional_iterator_tag, VMapPair> last_pair);

    /*
     * Sets the internal "added pairs" buffer.
     * An error will occur if a user attempts to add too many pairs than the
     *    size of the added pairs buffer has space for.
     *
     * Will error if an "added pairs" buffer already exists and is in use. XXX FOR NOW XXX
     */
    virtual void setAddedPairsBuf(MutableStringPtr added_pair_buffer) = 0;

    /*
     * Sets the internal "removed pairs" buffer.
     * An error will occur if a user attempts to remove a higher-index pair
     *    than the removed pairs buffer has space for.
     *
     * Will error if a removed pairs buffer already exists and is in use. XXX FOR NOW XXX
     */
    virtual void setRemovedPairsBuf(char* removed_pair_buffer, size_t bytes_in_buffer) = 0;
/// END initializers

/// BEGIN full-map writers
/// END full-map writers

/// BEGIN modifiers
    /*
     * Adds the passed VMapPair to the VMap or returns an iterator pointing
     *  to the existing value and true if a new element was inserted or false
     *  if it already existed.
     * As compared to emplace(), the VMapPair must be constructed before adding it to the map.
     */
    virtual std::pair<std::iterator<std::bidirectional_iterator_tag, VMapPair>, bool> insert(const VMapPair& val) = 0;
//    /*
//     * Adds the passed collection of VMapPairs to the VMap.
//     */
//    void insert(const_iterator first_pair, const_iterator last_pair);
    /*
     * Adds the passed collection of VMapPairs to the VMap.
     */
    virtual void insert(std::iterator<std::bidirectional_iterator_tag, VMapPair> first_pair, std::iterator<std::bidirectional_iterator_tag, VMapPair> last_pair) = 0;
    /*
     * Adds the passed name/value pair to the VMap or returns an iterator pointing
     *  to the existing value and true if a new element was inserted or false
     *  if it already existed.
     * As compared to insert(VMapPair), constructs the VMapPair only if it is adding it to the map.
     */
    virtual std::pair<std::iterator<std::bidirectional_iterator_tag, VMapPair>, bool> emplace(const VString& key_name, const VString& value, bool case_sensitive=true) = 0;
    /*
     * Adds the passed name/value pair to the VMap or returns an iterator pointing
     *  to the existing value and true if a new element was inserted or false
     *  if it already existed.
     * As compared to insert(VMapPair), constructs the VMapPair only if it is adding it to the map.
     */
    virtual std::pair<std::iterator<std::bidirectional_iterator_tag, VMapPair>, bool> emplace(const ImmutableStringPtr key_name, const ImmutableStringPtr value, bool case_sensitive=true) = 0;
    /*
     * Assigns the pairs from the assigning VMap to the current VMap, replacing its current set of pairs.
     */
    virtual VMap& operator=(const VMap& rightOperand) = 0;

    /*
     * Removes the pair with the passed key from the VMap.
     */
    virtual uint32 erase(const ImmutableStringPtr key_name) = 0;
    /*
     * Removes the passed collection of keys from the VMap.
     */
    virtual void erase(std::iterator<std::bidirectional_iterator_tag, VMapPair> pair_loc) = 0;
    /*
     * Removes the passed collection of keys from the VMap.
     */
    virtual void erase(std::iterator<std::bidirectional_iterator_tag, VMapPair> first_key, std::iterator<std::bidirectional_iterator_tag, VMapPair> last_key) = 0;
    /*
     * Removes all keys from the VMap.
     */
    virtual void clear() = 0;
/// END modifiers
};
/////////////////////////////// END Mutable VMap Interface /////////////////////////////


/////////////////////////////// BEGIN VMap Helper Functions/////////////////////////////
/// BEGIN to_string
/*
 * Returns the stringified representation of the VMap in the requested format
 *    (Either canonical JSON or a fully-accurate but un-escaped JSON-like format.)
 */
inline bool to_string(VMap* vmap, MutableStringPtr result_buf, bool canonicalJSON = true);

/// BEGIN to_string canonical JSON support
/*
 * Returns the stringified representation of the VMap in canonical JSON format.
 */
inline bool toStringCanonical(VMap* vmap, MutableStringPtr result_buf);
/*
 * Translates the VMap into JSONCPP format for stringifying.
 */
inline Json::Value compute_json_map(VMap* vmap);
/// END to_string canonical JSON support

/// BEGIN to_string manual JSON-like support
/*
 * Returns the stringified representation of the VMap in a fully-accurate but
 *    un-escaped JSON-like format.
 */
inline bool toStringManual(VMap* vmap, MutableStringPtr result_buf, uint indent_level = 0);
    /// END to_string manual JSON-like support
/// END to_string
/////////////////////////////// END VMap Helper Functions/////////////////////////////







/*******************************************************************************
********************************************************************************
************************* END VMAP INTERFACE DECARATION ************************
********************************************************************************
*******************************************************************************/




/////////////////////////// BEGIN VMap Pair Container ///////////////////////////
//////////////////////////// END VMap Pair Container ////////////////////////////




////////////////////////// BEGIN Map Block Definitions ////////////////////////
/////////////////////////// END Map Block Definitions ///////////////////////////





/////////////////////////////// BEGIN Immutable VMap V1 Immplementation /////////////////////////////
/*
 * V1 implementation of the VMap functionality specific to read-only access of
 *    a MapBlock (the on-disk VMap format).
 *
 * Likely want to use strncmp or strincmp for the template parameter.
 */
template <string_comparer string_compare>
class V1ImmutableVMap : public ImmutableVMap<string_compare> {
public:
/// BEGIN initializers
    /*
     * Default constructor to create an empty VMap.
     */
    V1ImmutableVMap() { // XXX TODO XXX
    };
    /*
     * Constructor which accepts and loads the passed MapBlock, stored in VString form.
     *
     * Throws on non-VMap mapBlocks.
     */
    V1ImmutableVMap(const VString& mapBlock) : map_block(NULL), values_block(NULL), keys_block(NULL),
                             value_index(0), key_index(0), map_length(0), key_length(0) {
        if (is_valid_map(mapBlock)) {
            map_block = reinterpret_cast<const MapBlock*>(mapBlock.data());
            values_block = reinterpret_cast<const MapV1ValuesBlock*>(map_block);
            keys_block = reinterpret_cast<const MapV1KeysBlock*>(mapBlock.data() + values_block->get_values_part_size());
            value_index = reinterpret_cast<const uint32*>(values_block->value_lookup);
            key_index = reinterpret_cast<const uint32*>(keys_block->key_lookup);
            map_length = mapBlock.length();
            key_length = map_length - values_block->get_values_part_size();

            if (values_block->get_values_part_size() + 4 * keys_block->key_count > mapBlock.length())
                vt_report_error(0, "Tried to retrieve Flex Table data from a cell not containing an Flex Table map.");
        }
//        else XXX THROW XXX
    };

    /*
     * Constructor which accepts and loads the passed MapBlock, stored in char* form.
     *
     * Throws on non-VMap mapBlocks.
     */
    V1ImmutableVMap(ImmutableStringPtr mapBlock) : map_block(NULL), values_block(NULL), keys_block(NULL),
                             value_index(0), key_index(0), map_length(0), key_length(0) {
        if (is_valid_map(mapBlock)) {
            map_block = reinterpret_cast<const MapBlock*>(mapBlock.ptr);
            values_block = reinterpret_cast<const MapV1ValuesBlock*>(map_block);
            keys_block = reinterpret_cast<const MapV1KeysBlock*>(mapBlock.ptr + values_block->get_values_part_size());
            value_index = reinterpret_cast<const uint32*>(values_block->value_lookup);
            key_index = reinterpret_cast<const uint32*>(keys_block->key_lookup);
            map_length = mapBlock.len;
            key_length = map_length - values_block->get_values_part_size();

            if (values_block->get_values_part_size() + 4 * keys_block->key_count > mapBlock.len)
                vt_report_error(0, "Tried to retrieve Flex Table data from a cell not containing an Flex Table map.");
        }
//        else XXX THROW XXX
    };
/// END initializers

/// BEGIN full-map writers
    /*
     * Writes the data contained in this VMap into a VMapBlock for storing.
     *
     * map_buf:             The destination VString buffer to write the VMap into.
     *
     * map_bytes_used:      Returns the number of Bytes used to store the just-created map.
     */
    virtual inline bool writeVMapBlock(VString& map_buf,
                               size_t& map_bytes_used) { return false; /* XXX TODO XXX */ };
    /*
     * Writes the data contained in this VMap into a VMapBlock for storing.
     *
     * map_buf:             The destination char* buffer to write the VMap into.
     *
     * map_bytes_used:      Returns the number of Bytes used to store the just-created map.
     */
    virtual inline bool writeVMapBlock(MutableStringPtr map_buf,
                               size_t& map_bytes_used) { return false; /* XXX TODO XXX */ };
/// END full-map writers

/// BEGIN pair accessors
    /*
     * Returns whether the VMap is empty (i.e. whether its size is 0).
     */
    inline virtual bool empty() const {
        return (size() == 0);
    }

    /*
     * Returns the number of pairs in the VMap.
     */
    inline virtual uint32 size() const {
        if (map_block && keys_block)
            return keys_block->key_count;
        else
            return 0;
    }


    /*
     * Accessor to retrieve values by key name.
     */
    virtual const ImmutableStringPtr at(VString& key_name) const { return (ImmutableStringPtr) { NULL, 0 }; }
    /*
     * Accessor to retrieve values by key name.
     */
    virtual ImmutableStringPtr at(const ImmutableStringPtr key_name) { return (ImmutableStringPtr) { NULL, 0 }; /* XXX TODO XXX */ };
    /*
     * Accessor to retrieve values by key name.
     */
    const ImmutableStringPtr at(const ImmutableStringPtr key_name) const { return (ImmutableStringPtr) { NULL, 0 }; /* XXX TODO XXX */ };
    /*
     * Returns the index of the pair with the given key stored in this VMap.
     */
    virtual inline int64 search(const VString& key_name, bool case_sensitive = true) { return -1; /* XXX TODO XXX */ };
    /*
     * Returns an iterator pointing to the pair with the given key stored
     *    in this VMap.
     */
    virtual std::iterator<std::bidirectional_iterator_tag, VMapPair> find(const VString& key_name, bool case_sensitive = true) { return std::iterator<std::bidirectional_iterator_tag, VMapPair>(); /* XXX TODO XXX */ };
//    /*
//     * Returns an iterator pointing to the pair at, if present, or just after
//     *    the passed key in this VMap.
//     */
//    virtual const_iterator find(const VString& key_name, bool case_sensitive = true) const { return NULL; /* XXX TODO XXX */ };
    /*
     * Returns a count of keys with the given name in this VMap.
     */
    virtual uint32 count(const VString& key_name, bool case_sensitive = true) const { return std::numeric_limits<uint32>::max(); /* XXX TODO XXX */ };
    /*
     * Returns a count of keys with the given name in this VMap.
     */
    virtual uint32 count(const ImmutableStringPtr key_name, bool case_sensitive = true) const { return std::numeric_limits<uint32>::max(); /* XXX TODO XXX */ };
    /*
     * Returns an iterator pointing to the pair at, if present, or just after
     *    the passed key in this VMap.
     */
    virtual std::iterator<std::bidirectional_iterator_tag, VMapPair> lower_bound(const VString& key_name) { return std::iterator<std::bidirectional_iterator_tag, VMapPair>(); /* XXX TODO XXX */ };
//    /*
//     * Returns an iterator pointing to the pair just after the passed key
//     *    in this VMap.
//     */
//    virtual const_iterator lower_bound(const VString& key_name) const;
    /*
     * Returns an iterator pointing to the pair just after the passed key
     *    in this VMap.
     */
    virtual std::iterator<std::bidirectional_iterator_tag, VMapPair> upper_bound(const VString& key_name) { return std::iterator<std::bidirectional_iterator_tag, VMapPair>(); /* XXX TODO XXX */ };
//    /*
//     * Returns an iterator pointing to the pair at or just after the passed
//     *    key in this VMap.
//     */
//    virtual const_iterator upper_bound(const VString& key_name) const;
    /*
     * Returns a pair of iterators with the first pointing to the pair at, if
     *    present, or just after the passed key in this VMap (the output of
     *    lower_bound) and with the second pointing to the pair just after the
     *    passed key (the output of upper_bound) in this VMap.
     */
    virtual std::iterator<std::bidirectional_iterator_tag, VMapPair> equal_range(const VString& key_name) { return std::iterator<std::bidirectional_iterator_tag, VMapPair>(); /* XXX TODO XXX */ };
    /*
     * Returns a pair of iterators with the first pointing to the pair at, if
     *    present, or just after the passed key in this VMap (the output of
     *    lower_bound) and with the second pointing to the pair just after the
     *    passed key (the output of upper_bound) in this VMap.
     */
    virtual std::iterator<std::bidirectional_iterator_tag, VMapPair> equal_range(ImmutableStringPtr key_name) { return std::iterator<std::bidirectional_iterator_tag, VMapPair>(); /* XXX TODO XXX */ };
//    /*
//     * Returns a pair of iterators with the first pointing to the pair at, if
//     *    present, or just after the passed key in this VMap (the output of
//     *    lower_bound) and with the second pointing to the pair just after the
//     *    passed key (the output of upper_bound) in this VMap.
//     */
//    virtual pair<const_iterator,const_iterator> equal_range(const VString& key_name) const;
//    /*
//     * Returns a pair of iterators with the first pointing to the pair at, if
//     *    present, or just after the passed key in this VMap (the output of
//     *    lower_bound) and with the second pointing to the pair just after the
//     *    passed key (the output of upper_bound) in this VMap.
//     */
//    virtual pair<const_iterator,const_iterator> equal_range(ImmutableStringPtr key_name) const;


private:
    /*
     * Returns a pointer to the key string at the passed index.
     */
    /// XXX TODO XXX Base of Iterator XXX TODO XXX
    /// XXX TODO XXX uint32 is too low when keys are removed XXX TODO XXX
    const char* get_key(uint32 keyNum) {
        if (map_block && keys_block && keyNum <= keys_block->key_count) {
            return getMapBlockKey(keyNum);
        }

        return NULL;
    }

    /*
     * Returns a length of the key string at the passed index.
     */
    /// XXX TODO XXX uint32 is too low when keys are removed XXX TODO XXX
    size_t get_key_size(uint32 keyNum) {
        if (map_block && keys_block && keyNum <= keys_block->key_count) {
            return getMapBlockKeySize(keyNum);
        }

        return 0;
    }

    /*
     * Returns a pointer to the key string at the passed index and its length.
     */
    /// XXX TODO XXX Base of Iterator XXX TODO XXX
    /// XXX TODO XXX uint32 is too low when keys are removed XXX TODO XXX
    /// XXX TODO XXX Switch to MutableStringPtr XXX TODO XXX
    const char* get_key(uint32 keyNum, size_t &key_bytes) {
        if (map_block && keys_block && keyNum <= keys_block->key_count) {
            key_bytes = getMapBlockKeySize(keyNum);
            return getMapBlockKey(keyNum);
        }

        key_bytes = 0;
        return NULL;
    }

    /*
     * Returns a pointer to the value string at the passed index.
     */
    /// XXX TODO XXX Base of Iterator XXX TODO XXX
    /// XXX TODO XXX uint32 is too low when keys are removed XXX TODO XXX
    const char* get_value(uint32 valueNum) {
        if (map_block && valueNum <= values_block->value_count) {
            return getMapBlockValue(valueNum);
        }

        return NULL;
    }

    /*
     * Returns a length of the value string at the passed index.
     */
    /// XXX TODO XXX uint32 is too low when keys are removed XXX TODO XXX
    size_t get_value_size(uint32 valueNum) {
        if (map_block && valueNum <= values_block->value_count) {
            return getMapBlockValueSize(valueNum);
        }

        return 0;
    }


    /*
     * Returns a pointer to the value string at the passed index and its length.
     */
    /// XXX TODO XXX Base of Iterator XXX TODO XXX
    /// XXX TODO XXX uint32 is too low when keys are removed XXX TODO XXX
    /// XXX TODO XXX Switch to MutableStringPtr XXX TODO XXX
    const char* get_value(uint32 valueNum, size_t &value_bytes) {
        if (map_block && valueNum <= values_block->value_count) {
            value_bytes = getMapBlockValueSize(valueNum);
            return getMapBlockValue(valueNum);
        }

        value_bytes = 0;
        return NULL;
    }

/// BEGIN low-level MapBlock accessors
    inline uint32 getMapBlockCount() { return ((keys_block) ? keys_block->key_count : 0); }

    inline uint32 getMapBlockKeySize(uint32 keyNum) {
        return ((keys_block) ? (get_key_end_loc(keyNum) - get_key_start_loc(keyNum)) : 0);
    }

    inline const char* getMapBlockKey(uint32 keyNum) {
        return ((keys_block) ? &(keys_block->key_lookup[get_key_start_loc(keyNum) - 4]) : NULL);
    }

    inline uint32 getMapBlockValueSize(uint32 valueNum) {
        return  ((keys_block) ? (get_value_end_loc(valueNum) - get_value_start_loc(valueNum)) : 0);
    }

    inline const char* getMapBlockValue(uint32 valueNum) {
        if (!keys_block || value_index[valueNum] == std::numeric_limits<uint32>::max())
            return NULL;

        return &(values_block->value_lookup[get_value_start_loc(valueNum) - 4]);
    }


    inline uint32 get_key_end_loc(uint32 keyIndex) {
        if (!keys_block)
            return 0;
        if ((uint32)keyIndex < keys_block->key_count - 1)
            return key_index[keyIndex + 1];
        else
            return key_length;
    }

    inline uint32 get_key_start_loc(uint32 keyIndex) {
        return ((keys_block) ? key_index[keyIndex] : 0);
    }

    inline uint32 get_value_start_loc(uint32 valueIndex) {
        if (!keys_block) {
            return 0;
        }
        if (value_index[valueIndex] == std::numeric_limits<uint32>::max()) {
            return get_value_end_loc(valueIndex);
        } else {
            return value_index[valueIndex];
        }
    }

    uint32 get_value_end_loc(uint32 valueIndex) {
        if (!keys_block)
            return 0;
        if ((uint32)valueIndex < values_block->value_count-1) {
            if (value_index[valueIndex + 1] == std::numeric_limits<uint32>::max()) {
                return get_value_end_loc(valueIndex + 1);
            } else {
                return value_index[valueIndex + 1];
            }
        } else {
            return values_block->value_length;
        }
    }
/// END low-level MapBlock accessors


/// BEGIN iterators
public:
    /*
     * Returns an iterator referring to the first pair in the VMap.
     */
    virtual std::iterator<std::bidirectional_iterator_tag, VMapPair> begin() { return std::iterator<std::bidirectional_iterator_tag, VMapPair>(); /* XXX TODO XXX */ };
//    /*
//     * Returns an iterator referring to the first pair in the VMap.
//     */
//    virtual const_iterator begin() const { return NULL; /* XXX TODO XXX */ };
//    /*
//     * Returns an iterator referring to the first pair in the VMap.
//     */
//    virtual const_iterator cbegin() const noexcept { return NULL; /* XXX TODO XXX */ };
    /*
     * Returns an iterator referring to the past-the-end pair in the VMap.
     */
    virtual std::iterator<std::bidirectional_iterator_tag, VMapPair> end() { return std::iterator<std::bidirectional_iterator_tag, VMapPair>(); /* XXX TODO XXX */ };
//    /*
//     * Returns an iterator referring to the past-the-end pair in the VMap.
//     */
//    virtual const_iterator end() const { return NULL; /* XXX TODO XXX */ };
//    /*
//     * Returns an iterator referring to the first pair in the VMap.
//     */
//    virtual const_iterator cend() const noexcept { return NULL; /* XXX TODO XXX */ };
//
//    /*
//     * Returns a reverse-iterator referring to the last pair in the VMap
//     *    (i.e. its reverse beginning).
//     */
//    virtual std::reverse_iterator<(std::iterator<std::bidirectional_iterator_tag, VMapPair>)> rbegin() { return std::iterator<std::bidirectional_iterator_tag, VMapPair>(); /* XXX TODO XXX */ };
//    /*
//     * Returns a reverse-iterator referring to the last pair in the VMap
//     *    (i.e. its reverse beginning).
//     */
//    virtual const_reverse_iterator rbegin() const { return NULL; /* XXX TODO XXX */ };
//    /*
//     * Returns a reverse-iterator referring to the last pair in the VMap
//     *    (i.e. its reverse beginning).
//     */
//    virtual const_reverse_iterator crbegin() const noexcept { return NULL; /* XXX TODO XXX */ };
//    /*
//     * Returns a reverse-iterator referring to the past-the-end pair in the VMap.
//     *    (i.e. its reverse end).
//     */
//    virtual std::reverse_iterator<std::bidirectional_iterator_tag, VMapPair> rend() { return NULL; /* XXX TODO XXX */ };
//    /*
//     * Returns a reverse-iterator referring to the past-the-end pair in the VMap.
//     *    (i.e. its reverse end).
//     */
//    virtual const_reverse_iterator rend() const { return NULL; /* XXX TODO XXX */ };
//    /*
//     * Returns a reverse-iterator referring to the past-the-end pair in the VMap.
//     *    (i.e. its reverse end).
//     */
//    virtual const_reverse_iterator crend() const noexcept { return NULL; /* XXX TODO XXX */ };
/// END iterators

/// BEGIN map validation
    /*
     * Returns the integer version of the passed VMap in on-disk format
     *  contained in this VMap, or -1 if it does not contain a
     *  valid map.
     *
     *  XXX QUESTION XXX Move out into a static?
     */
    inline const int map_version() {
        return map_version(map_block);
    }

    /*
     * Returns the integer version of the passed VMap in on-disk format
     *  contained in the passed VString, or -1 if it does not contain a
     *  valid map.
     */
    static inline const int map_version(const VString& dictBlock) {
        if (!V1ImmutableVMap<string_compare>::is_valid_map(dictBlock, false))
        {
            return -1;
        } else {
            return reinterpret_cast<const MapBlock*>(dictBlock.data())->map_version;
        }
    }

    /*
     * Returns whether the VMap in on-disk format contained in this
     *  VMap instance is a valid map.
     *
     *  XXX QUESTION XXX Move out into a static?
     *
     *  XXX TODO XXX Figure out why static versions aren't working (at least the VString version) XXX TODO XXX
     */
    inline const bool is_valid_map(bool logInvalidMaps = false) {
        return V1ImmutableVMap<string_compare>::is_valid_map(map_length,
                          values_block,
                          logInvalidMaps);
    }

    /*
     * Returns whether the VMap in on-disk format contained in this 
     *  VMap instance is a valid map.
     *
     *  XXX QUESTION XXX Move out into a static?
     */
    const bool is_valid_map(uint32 valueIndex, bool logInvalidMaps = false) {
        return V1ImmutableVMap<string_compare>::is_valid_map(get_value_size(valueIndex),
                          reinterpret_cast<const MapV1ValuesBlock*>(get_value(valueIndex)),
                          logInvalidMaps);
    }

    /*
     * Returns whether the passed VMap in on-disk format contained in the passed
     *  VString is a valid map.
     */
    static inline const bool is_valid_map(const VString& dictBlock, bool logInvalidMaps = false) {
        if (dictBlock.length() < 4 || dictBlock.isNull())
            return false;
        return V1ImmutableVMap<string_compare>::is_valid_map(dictBlock.length(),
                          reinterpret_cast<const MapV1ValuesBlock*>(dictBlock.data()),
                          logInvalidMaps);
    }

    /*
     * Returns whether the passed VMap in on-disk format contained in the passed
     *  char* is a valid map.
     */
    static inline const bool is_valid_map(ImmutableStringPtr mapBlock, bool logInvalidMaps = false) {
        if (mapBlock.len < 4)
            return false;
        return V1ImmutableVMap<string_compare>::is_valid_map(mapBlock.len,
                          reinterpret_cast<const MapV1ValuesBlock*>(mapBlock.ptr),
                          logInvalidMaps);
    }
private:
    /*
     * Returns whether the passed VMap values-part (MapV1ValuesBlock) in on-disk
     *  format is a valid map.
     */
    static inline const bool is_valid_map(vsize length,
                                        const MapV1ValuesBlock* mapBlock,
                                        bool logInvalidMaps = false) {
        if (mapBlock == NULL ||                                               // Do we actually have any data?
                length < 4)                                                   // Do we have enough data at least for the header?
        {
//            if (logInvalidMaps) {
//                log("VMap.is_valid_map() found an invalid map with checks: mapBlock == NULL: [%d], length < 4: [%d]", (mapBlock == NULL), (length < 4));
//            } else {
//                LogDebugUDBasic("VMap.is_valid_map() found an invalid map with checks: mapBlock == NULL: [%d], length < 4: [%d]", (mapBlock == NULL), (length < 4));
//            }
            return false;
        }

        if (mapBlock->map_version < 1 ||                                      // Do we have a valid smallest-supported map version?
                mapBlock->map_version > 1 ||                                  // Do we have a valid largest-supported map version?
                mapBlock->value_length + MAP_BLOCK_HEADER_SIZE > length ||    // Do we have enough data for the keys header?
                mapBlock->value_count < 0 ||                                  // Do we have a valid count of values?
                MAP_BLOCK_HEADER_SIZE + sizeof(uint32) + (sizeof(uint32) * mapBlock->value_count) > length)   // Do we have key offsets for each key?
        {
//            if (logInvalidMaps) {
//                log("VMap.is_valid_map() found an invalid map with checks: mapBlock->map_version < 1: [%d], mapBlock->map_version > 1: [%d], mapBlock->value_length + MAP_BLOCK_HEADER_SIZE > length: [%d], mapBlock->value_count < 0: [%d], and MAP_BLOCK_HEADER_SIZE + 4 * mapBlock->value_count > length: [%d]", (mapBlock->map_version < 1), (mapBlock->map_version > 1), (mapBlock->value_length + MAP_BLOCK_HEADER_SIZE > length), (mapBlock->value_count < 0), (MAP_BLOCK_HEADER_SIZE + 4 * mapBlock->value_count > length));
//            } else {
//                LogDebugUDBasic("VMap.is_valid_map() found an invalid map with checks: mapBlock->map_version < 1: [%d], mapBlock->map_version > 1: [%d], mapBlock->value_length + MAP_BLOCK_HEADER_SIZE > length: [%d], mapBlock->value_count < 0: [%d], and MAP_BLOCK_HEADER_SIZE + 4 * mapBlock->value_count > length: [%d]", (mapBlock->map_version < 1), (mapBlock->map_version > 1), (mapBlock->value_length + MAP_BLOCK_HEADER_SIZE > length), (mapBlock->value_count < 0), (MAP_BLOCK_HEADER_SIZE + 4 * mapBlock->value_count > length));
//            }
            return false;
        }

        const MapV1KeysBlock* keysBlock = reinterpret_cast<const MapV1KeysBlock*>(((char*)mapBlock) + mapBlock->get_values_part_size());
        const uint32* temp_key_index = reinterpret_cast<const uint32*>(keysBlock->key_lookup);
        if (keysBlock->key_count < 0 ||                                          // Do we have a valid count of keys?
                mapBlock->value_count != keysBlock->key_count ||                      // Do we have the same number of keys and values?
                mapBlock->get_values_part_size() + sizeof(uint32) + (sizeof(uint32) * keysBlock->key_count) > length ||   // Do we have key offsets for each key?
                (keysBlock->key_count > 0 && mapBlock->get_values_part_size() + temp_key_index[keysBlock->key_count-1] > length)    // Does the beginning of the last key fall within limits?
            )
        {
//            if (logInvalidMaps) {
//                log("VMap.is_valid_map() found an invalid map with checks: keysBlock->key_count < 0: [%d], mapBlock->value_count != keysBlock->key_count: [%d], and mapBlock->get_values_part_size() + 4 * keysBlock->key_count > length: [%d]", (keysBlock->key_count < 0), (mapBlock->value_count != keysBlock->key_count), (mapBlock->get_values_part_size() + 4 * keysBlock->key_count > length));
//            } else {
//                LogDebugUDBasic("VMap.is_valid_map() found an invalid map with checks: keysBlock->key_count < 0: [%d], mapBlock->value_count != keysBlock->key_count: [%d], and mapBlock->get_values_part_size() + 4 * keysBlock->key_count > length: [%d]", (keysBlock->key_count < 0), (mapBlock->value_count != keysBlock->key_count), (mapBlock->get_values_part_size() + 4 * keysBlock->key_count > length));
//            }
            return false;
        }

        return true;
    }
/// END map validation

private:
/// BEGIN data members
    ///// BEGIN Map Block (Read-only) /////
    /*
     * The map block currently loaded into this VMap.
     *
     * Used for reading only.
     *
     * Each item here is simply a cast on a different part of the MapBlock
     */
    const MapBlock* map_block;
    const MapV1ValuesBlock* values_block;
    const MapV1KeysBlock* keys_block;
    const uint32* value_index;
    const uint32* key_index;
    uint64 map_length;
    uint32 key_length;
    ///// END Map Block (Read-only) /////


    ///// BEGIN Utility Helpers /////
    /*
     * Speedup for searching for the same key from blocks of VMaps.
     * The last location the key being searched for was found.
     */
    static uint32 last_found_loc;
    ///// END Utility Helpers /////

/// END data members
};
/////////////////////////////// END Immutable VMap V1 Immplementation /////////////////////////////










//std::map<,> methods not included:
//
//=====Modifiers:
//swap
//Swap content (public member function )
//operator[]
//Access element (public member function )
//
//=====Observers:
//key_comp
//Return key comparison object (public member function )
//value_comp
//Return value comparison object (public member function )
//
//=====Allocator:
//get_allocator
//Get allocator (public member function )
//
//=====Operations:
//equal_range
//Get range of equal elements (public member function )
} /// END namespace flextable



// End of define VMAP_H
#endif

