/* Copyright (c) 2005 - 2015 Hewlett Packard Enterprise Development LP  -*- C++ -*-*/
/****************************
 * Vertica Analytic Database
 *
 * Helper functions and objects to parse strings into the various internal Vertica formats
 *
 ****************************/

#include <errno.h>
#include <strings.h>
#include <alloca.h>
#include <time.h>

#include "Vertica.h"

#include "StringParsers.h"


using namespace Vertica;

#ifndef FLEXSTRINGPARSERS_H_
#define FLEXSTRINGPARSERS_H_


class FlexStringParsers : public StringParsers
{
public:

    /***************************************************
     * Customizations that differ from StringParsers...
     ***************************************************/

    /**
     * Parse a string to a binary field
     */
    bool parseBinary(char *str, size_t len, size_t colNum, Vertica::VString &target, const Vertica::VerticaType &type) {
        // Same implementation as StringParsers, but use the local parseVarchar
        return parseVarchar(str, len, colNum, target, type);
    }

    /**
     * Parse a string to a varbinary field
     */
    bool parseVarbinary(char *str, size_t len, size_t colNum, Vertica::VString &target, const Vertica::VerticaType &type) {
        // Same implementation as StringParsers, but use the local parseVarchar
        return parseVarchar(str, len, colNum, target, type);
    }

    /**
     * Parse a string to a CHAR field
     */
    bool parseChar(char *str, size_t len, size_t colNum, Vertica::VString &target, const Vertica::VerticaType &type) {
        // Same implementation as StringParsers, but use the local parseVarchar
        return parseVarchar(str, len, colNum, target, type);
    }

    /**
     * Parse a string to a VARCHAR field
     */
    bool parseVarchar(char *str, size_t len, size_t colNum, Vertica::VString &target, const Vertica::VerticaType &type) {
        // String types are NOT null because they are len==0!

        if ((size_t)type.getStringLength() < len) {
            // too long!
            return false;
        } else {
            target.copy(str, len);
            return true;
        }
    }
};

#endif // FLEXSTRINGPARSERS_H_
