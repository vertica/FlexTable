/**
 * Copyright (c) 2005 - 2015 Hewlett Packard Enterprise Development LP
 *
 * Creation Date: December 10, 2013
 */
/**
 * Flex Table JSON parser YAJL callbacks
 */


#ifndef JSON_PARSEHANDLERS_H
#define JSON_PARSEHANDLERS_H

extern "C" {
#include <yajl/yajl_parse.h>
}

#include <iostream>
#include <ctype.h>


#include "JSONParseStack.h"


namespace flextable
{


/*
 * YAJL Callbacks for JSON parsing
 */
class JSONParseHandlers {
private:
    //////////
    // YAJL callbacks -- static wrappers to make C happy
    //////////

    // Wrapping the callbacks into our desired end form, currently
    //  only including casting the context stack.
    static int handle_start_map(void* ctx) {
        return ToJSONParseContext(ctx)->handle_start_map();
    }
    static int handle_end_map(void* ctx) {
        return ToJSONParseContext(ctx)->handle_end_map();
    }
    static int handle_start_array(void* ctx) {
        return ToJSONParseContext(ctx)->handle_start_array();
    }
    static int handle_end_array(void* ctx) {
        return ToJSONParseContext(ctx)->handle_end_array();
    }
    static int handle_map_key(void* ctx, const unsigned char* ch, size_t len) {
        return ToJSONParseContext(ctx)->handle_map_key((char*) ch, len);
    }
    static int handle_null(void* ctx) {
        return ToJSONParseContext(ctx)->handle_null();
    }
    static int handle_boolean(void* ctx, int val) {
        return ToJSONParseContext(ctx)->handle_boolean((bool) val);
    }
    static int handle_string(void* ctx, const unsigned char* ch, size_t len) {
        return ToJSONParseContext(ctx)->handle_string((const char*) ch, len);
    }
    static int handle_number(void* ctx, const char* ch, size_t len) {
        return ToJSONParseContext(ctx)->handle_number((const char*)ch, len);
    }

    //////////
    // Helper Functions
    //////////

    static inline JSONParseContext* ToJSONParseContext(void* ctx) { return static_cast<JSONParseContext*>(ctx); }


public:
/*
 * Register YAJL callbacks, allocates a new Yajl parse handle, and configure parse options
 */
static void RegisterCallbacks(JSONParseContext* jsctx) {
        // A bunch of yajl setup
        // Set all the callbacks
        jsctx->yajl_callback.yajl_start_map = &JSONParseHandlers::handle_start_map;
        jsctx->yajl_callback.yajl_end_map = &JSONParseHandlers::handle_end_map;
        jsctx->yajl_callback.yajl_start_array = &JSONParseHandlers::handle_start_array;
        jsctx->yajl_callback.yajl_end_array = &JSONParseHandlers::handle_end_array;

        jsctx->yajl_callback.yajl_map_key = &JSONParseHandlers::handle_map_key;

        jsctx->yajl_callback.yajl_null = &JSONParseHandlers::handle_null;
        jsctx->yajl_callback.yajl_boolean = &JSONParseHandlers::handle_boolean;
        jsctx->yajl_callback.yajl_string = &JSONParseHandlers::handle_string;
        jsctx->yajl_callback.yajl_number = &JSONParseHandlers::handle_number;

        jsctx->yajl_callback.yajl_integer = NULL;  // Ignored; we're using yajl_number
        jsctx->yajl_callback.yajl_double = NULL;  // Ignored; we're using yajl_number

        // Allocate a yajl yajl_hand
        jsctx->yajl_hand = yajl_alloc(&(jsctx->yajl_callback), NULL, ((void*)jsctx));
        yajl_config(jsctx->yajl_hand, yajl_allow_comments, 1);
        yajl_config(jsctx->yajl_hand, yajl_allow_multiple_values, 1);
}

}; /// END class JSONParseHandlers



} /// END namespace flextable

#endif // JSON_PARSEHANDLERS_H
