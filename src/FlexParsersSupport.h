/* Copyright (c) 2005 - 2015 Hewlett Packard Enterprise Development LP  -*- C++ -*-*/
/****************************
 * Vertica Analytic Database
 *
 * Helper data structures and functions to parse various data formats supported by Flex.
 *
 ****************************/

#ifndef FLEX_PARSERS_SUPPORT_H_
#define FLEX_PARSERS_SUPPORT_H_

#include "FlexTable.h"

namespace flextable {
/**
 * Generic buffer class
 */
template<class T>
struct Buffer {
    T *A; // pre-allocated array
    int len; // buffer size
    int next; // next free entry

    Buffer() : A(NULL), len(0), next(-1) { }

    Buffer(const Buffer &rhs) : A(rhs.A), len(rhs.len), next(rhs.next) { }

    Buffer(T *A, int len) :
        A(A), len(len), next(0) { }

    /**
     * Check if there is sufficient space in the buffer to write 'entries' number of items
     */
    bool canWrite(int entries) const {
        return ((len - next) >= entries);
    }

    /**
     * Get the number of free buffer entries
     */
    int free() const {
        return (len - next);
    }

    /**
     * Get a writable pointer to the next free buffer entry
     */
    T* getNext() const {
        return &A[next];
    }
};

/**
 * Buffer with memory allocation managed by Vertica
 *
 * NB: Will be used for the lifetime of the query so does not need to support
 *     deallocating
 */
template<class T>
struct BufferWithAllocator : public Buffer<T> {
    ServerInterface *srvInterface;
    T *loc; // next location in buffer to write

    BufferWithAllocator() : Buffer<T>(), srvInterface(NULL), loc(NULL) { }
    BufferWithAllocator(ServerInterface *srvInterface, int len) : Buffer<T>(), srvInterface(srvInterface) {
        this->A = (T *) srvInterface->allocator->alloc(sizeof(T) * len);
        this->len = len;
        this->loc = this->A;
    }

    void initialize(ServerInterface *srvInterface, int len) {
        this->srvInterface = srvInterface;
        this->A = (T *) srvInterface->allocator->alloc(sizeof(T) * len);
        this->len = len;
        this->loc = this->A;
    }

    // Resize the buffer if necessary to at least the requested size
    void allocate(size_t requested) {
        bool needsAlloc = false;
        size_t bufUsedBytes = 0;

        // Allocate a buffer if not is allocated yet
        if (this->len <= 0) {
            this->len = BASE_RESERVE_SIZE;
            needsAlloc = true;
        } else {
            bufUsedBytes = this->loc - this->A;
        }

        // Create a new buffer if the requested length is too large to fit in the
        // remaining buffer and increase the default buffer size if it is too
        // large to fit in a new, empty buffer.
        if (((size_t) this->len - bufUsedBytes) < requested) {
            needsAlloc = true;
            while ((size_t) this->len < requested) {
                this->len *= 2;
            }
        }

        // Re-allocate a new buffer leaving the existing buffer with pointers to it as-is
        if (needsAlloc) {
            this->A = (char *) srvInterface->allocator->alloc(this->len);
            this->loc = this->A;
        }
    }

};

} // namespace flextable

#endif // FLEX_PARSERS_SUPPORT_H
