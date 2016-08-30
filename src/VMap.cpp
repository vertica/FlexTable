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
 *  V1:   The first offical release for 7.0 taking up a single column.
 */


#include "Vertica.h"

#include "VMap.h"


using namespace Vertica;



namespace flextable
{

///////////////// BEGIN String Whitespace Helpers' Definitions /////////////////
std::string trim(std::string input) {
    size_t len = input.size();
    size_t start, end;
    for (start = 0; start < len && isspace(input[start]); start++)
      ;
    for (end = len; end > 0 && isspace(input[end-1]); end--)
      ;
    return input.substr(start, end-start);
}

void trim_buf_ends(char** base, size_t *bound)
{
    return trim_buf_ends(((const char**) base), bound);
}

void trim_buf_ends(const char** base, size_t *bound)
{
    // advance pointer
    const char* start = *base;
    const char* end = start + *bound;
    while (start != end && isspace(start[0])) {
        start++;
    }
    while (start != end && isspace( end[-1])) {
        end--;
    }
    *base = start;
    *bound = end-start;
}


bool operator< (const ImmutableStringPtr &p1, const ImmutableStringPtr &p2) {
    int cmp = strncmp(p1.ptr, p2.ptr, std::min(p1.len, p2.len));
    return cmp < 0 || (cmp == 0 && p1.len < p2.len);
}
///////////////// END String Whitespace Helpers' Definitions //////////////////


/////////////////////// BEGIN VMapBlockReader Definition ///////////////////////

/// BEGIN lookup helpers
uint32 VMapBlockReader::get_value_end_loc(uint32 valueIndex) {
    if (!keys_block)
        return 0;
    if ((uint32)valueIndex < full_block->value_count-1) {
        if (value_index[valueIndex + 1] == std::numeric_limits<uint32>::max()) {
            return get_value_end_loc(valueIndex + 1);
        } else {
            if (isValueOffsetValid(value_index[valueIndex + 1])) {
                return value_index[valueIndex + 1];
            } else {
                return 0;
            }
        }
    } else {
        return full_block->value_length;
    }
}
/// END lookup helpers

//////////////////////// END VMapBlockReader Definition ////////////////////////








} /// END namespace flextable

