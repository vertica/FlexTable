
#ifndef FLEXTABLE_VMAPPAIR_H_
#define FLEXTABLE_VMAPPAIR_H_

#include "Vertica.h"
#include "BasicsUDxShared.h"
#include <cstddef>

namespace flextable {

using namespace Vertica;

/**
  * @brief Buffer storing a single key/value pair in packed form.
  *
  * @details VMapPairs are constructed using VMapPairWriter and read using
  * VMapPairReader.
  */
class VMapPair {
private:
    /*
     * Byte-packed representation of the key/value pair stored as:
     *  A single-byte "is null" indicator (uint8 off-disk since we can't address
     *    less than a Byte anyway and to be clear we're using a whole Byte)
     *  A four-byte key name length (uint32)
     *  A variable-length key name (char*)
     *  A four-byte value length (uint32)
     *  A variable-length value (char*)
     */
    const char* pair_data;

    // Serialization constructor, not for use.
    inline VMapPair() : pair_data(NULL) {}

    /*
     * How much space is needed to encode whether a pair's value is null.
     */
    const static size_t IsNullByteLength = sizeof(uint8);
    /*
     * How much space is needed to encode the length of a pair's key.
     */
    const static size_t KeyLengthByteLength = sizeof(uint32);
    /*
     * How much space is needed to encode the length of a pair's value.
     */
    const static size_t ValueLengthByteLength = sizeof(uint32);

public:
    /**
     * Constructor which accepts VMapPair data, to be called by a
     * VMapPairWriter.
     */
    VMapPair(const char* record_data) : pair_data(record_data) { }

    /** Get the length in Bytes of the key. */
    uint32 key_length() const {
        const char* ptr = pair_data + IsNullByteLength;
        return *((uint32*)ptr);
    }

    /**
     * Get a pointer to the key string. This string is not null-terminated. Use
     * {@link #key_length()} to know the size. If the size is 0, the address
     * returned may not be valid.
     *
     * Note: keys should not be 0 length, though this may not currently be
     * enforced.
     */
    const char* key_str() const {
        const char* ptr = pair_data + (IsNullByteLength + KeyLengthByteLength);
        return ptr;
    }

    /** Get the length in Bytes of the value. */
    uint32 value_length() const {
        const char* ptr = pair_data + (IsNullByteLength + KeyLengthByteLength + key_length());
        return *((uint32*)ptr);
    }

    /**
     * Get a pointer to the value string. This string is not null-terminated.
     * Use {@link #is_null()} and {@link #value_length()} to know the size. If
     * the value is null or the size is 0, the address returned may not be
     * valid.
     */
    const char* value_str() const {
        const char* ptr = pair_data + (IsNullByteLength + KeyLengthByteLength + key_length() + ValueLengthByteLength);
        return ptr;
    }

    /** Checks whether the value is null. */
    uint8 is_null() const {
        // TODO: Why don't we use length = vint_null like in other places?
        return (uint8)pair_data[0];
    }

    /** Get a pointer to the full data buffer for this VMapPair. */
    const char* record_data() const {
        return pair_data;
    }

    /** Get the length in Bytes of the full data buffer for this VMapPair. */
    size_t record_length() const {
        return size_needed(key_length(), value_length());
    }

    /**
     * Get the length in Bytes of the size needed to add this VMapPair to an
     * on-disk-form MapBlock.
     */
    static size_t size_needed(uint32 key_len, uint32 value_len) {
        return IsNullByteLength +
            KeyLengthByteLength +
            key_len +
            ValueLengthByteLength +
            value_len;
    }

    /*
     * The maximum length of a key in a vmap entry.
     *
     * 32,000,000 bytes is the maximum length of a long data type in Vertica
     * (see MaxLongAttrSize, which is an internal-only config param).
     */
    const static size_t MaxKeyLength = 32000000;
    /*
     * The maximum length of a key in a vmap entry.
     *
     * 32,000,000 bytes is the maximum length of a long data type in Vertica
     * (see MaxLongAttrSize, which is an internal-only config param).
     */
    const static size_t MaxValueLength = 32000000;

    /**
     * Write a vmap pair into buf, returning the length of bytes consumed. If
     * there is not enough space, return 0.
     */
    static size_t serialize(const char* key, uint32 key_len,
                                   bool is_null,
                                   const char* value, uint32 value_len,
                                   char* buf, size_t buf_len) {

        size_t size = size_needed(key_len, value_len);
        if (size > buf_len) { return 0; }

        // set up a bunch of pointers then do the assignment. Easier to read.
        uint8* p_null;
        uint32* p_key_len;
        uint32* p_val_len;
        char* p_key;
        char* p_val;

        {
            char * ptr = buf;
            p_null = (uint8*) ptr;
            ptr += IsNullByteLength;
            p_key_len = (uint32*) ptr;
            ptr += KeyLengthByteLength;
            p_key = (char*) ptr;
            ptr += key_len;
            p_val_len = (uint32*) ptr;
            ptr += ValueLengthByteLength;
            p_val = (char*) ptr;
        }

        *p_null = (uint8) is_null;
        *p_key_len = key_len;
        *p_val_len = value_len;
        memcpy(p_key, key, key_len);
        memcpy(p_val, value, value_len);

        return size;
    }
};

////////////////////// END VMapPair Container Declaration //////////////////////

}

#endif // include guard
