/* Copyright (c) 2005 - 2015 Hewlett Packard Enterprise Development LP -*- C++ -*- */
/* 
 * Description: User Defined Transform Function to Tokenize Values Within a Flexible Table VMap
 * 
 * Creation Date: October 03, 2014
 */
#include "Vertica.h"
#include <sstream>
#include <algorithm>

#include "FlexTable.h"
#include "VMap.h"
#include "FlexTokenizer.h"

using namespace Vertica;
using namespace std;

namespace flextable
{


/*
 * XXX TODO XXX
 *
 * Flex string tokenizer - This transform function takes in a string argument and
 * tokenizes it. For example, it will turn the following input:
 *
 *       url           description
 * ----------------+------------------------------
 * www.amazon.com  | Online retailer
 * www.hp.com      | Major hardware vendor
 *
 * into the following output:
 *
 *       url           token
 * ----------------+------------------
 * www.amazon.com  | Online
 * www.amazon.com  | retailer
 * www.hp.com      | Major
 * www.hp.com      | hardware
 * www.hp.com      | vendor
 *
 */
FlexTokenizer::FlexTokenizer(TokenizeOpts tokenizeType)
        : tokenize_type(tokenizeType), pattern_regex(NULL), pattern_extra(NULL), ovec(NULL)
{}

void FlexTokenizer::setup(ServerInterface &srvInterface, const SizedColumnTypes &argTypes)
{
    if (tokenize_type != SPACES)
    {
        const char* error;
        int erroffset;
        ovec = new int[0];
        pattern_regex = pcre_compile("[^A-Za-z]", 0 /*options*/, &error, &erroffset, NULL);
        pattern_extra = pcre_study(pattern_regex, 0, &error);
    }

    argTypes.getArgumentColumns(input_cols);

}

void FlexTokenizer::destroy(ServerInterface &srvInterface, const SizedColumnTypes &argTypes)
{
    if (pattern_regex) pcre_free(pattern_regex);
    if (pattern_extra) pcre_free(pattern_extra);
    delete[] ovec;
    ovec = NULL;
}

void FlexTokenizer::processPartition(ServerInterface &srvInterface,
                                     PartitionReader &arg_reader,
                                     PartitionWriter &result_writer)
{
    uint64 currRow = 0;
    try {
        if (arg_reader.getNumCols() < 2)
            vt_report_error(0, "Function only accepts 2 or more  arguments, but [%zu] provided on map values retrieval", arg_reader.getNumCols());

        do {
            currRow++;
            const VString& fullmap = arg_reader.getStringRef(1);

            // If input string is NULL, then output is NULL as well
            if (!fullmap.isNull())
            {
                tokenizeMap(srvInterface, arg_reader, result_writer, fullmap.data(), fullmap.length());
            }
        } while (arg_reader.next());
    } catch(std::exception& e) {
        vt_report_error(0, "Exception while processing partition row: [%llu] on map values retrieval: [%s]", currRow, e.what());
    } catch(...) {
        vt_report_error(0, "Unknown exception while processing partition row: [%llu] on map values retrieval.", currRow);
    }
}

void FlexTokenizer::tokenizeMap(ServerInterface &srvInterface, PartitionReader &arg_reader, PartitionWriter &result_writer, const char* map_value, const int32 &map_size)
{
    VMapBlockReader map_reader(srvInterface, map_value, map_size);
    for (size_t pairNum = 0; pairNum < map_reader.get_count(); pairNum++)
    {
        if (map_reader.is_valid_map(srvInterface, pairNum))
        {
            //Recursively process map
            FlexTokenizer::tokenizeMap(srvInterface, arg_reader, result_writer, map_reader.get_value(pairNum), map_reader.get_value_size(pairNum));
        }
        else
        {
            tokenizeStringValue(map_reader.get_value(pairNum),
                                map_reader.get_value_size(pairNum),
                                arg_reader,
                                result_writer,
                                tokenize_type);
        }
    }
}

/// XXX TODO XXX Implement the 3 options for handling special characters
void FlexTokenizer::tokenizeStringValue(const char * value,
                                        uint32 value_size,
                                        PartitionReader &arg_reader,
                                        PartitionWriter &result_writer,
                                        TokenizeOpts tokenizationOptions)
{
    // If input string is NULL, then output is NULL as well
    if (value == NULL || value_size == 0) {
        VString &word = result_writer.getStringRef(0);
        word.setNull();

        // check for pass-through inputs
        if (input_cols.size() > 2)
        {
            // write the remaining arguments to output
            size_t outputIdx = 1;
            for (size_t inputIdx = 2; inputIdx < input_cols.size(); inputIdx++)
            {
                result_writer.copyFromInput(outputIdx, arg_reader, inputIdx);
                outputIdx++;
            }
        }

        result_writer.next();
    } else {
        uint32 word_start = 0, word_end = 0;
        for ( ; word_end < value_size && isWordEndChar(value[word_end], tokenizationOptions); word_end++) {}
        word_start = word_end;

        while (word_end < value_size) {
            for ( ; word_end < value_size && !isWordEndChar(value[word_end], tokenizationOptions); word_end++) {}

            VString &word = result_writer.getStringRef(0);
            word.copy(&value[word_start], min((word_end - word_start), MAX_TOKEN_LENGTH));
            word_start = ++word_end;

            // check for pass-through inputs
            if (input_cols.size() > 2)
            {
                // write the remaining arguments to output
                size_t outputIdx = 1;
                for (size_t inputIdx = 2; inputIdx < input_cols.size(); inputIdx++)
                {
                    result_writer.copyFromInput(outputIdx, arg_reader, inputIdx);
                    outputIdx++;
                }
            }

            result_writer.next();

            for ( ; word_end < value_size && isWordEndChar(value[word_end], tokenizationOptions); word_end++) {}
            word_start = word_end;
        }
    }
}

/// XXX DELETEME XXX Just for testing SPECIAL_CHARACTERS until switching to support SPACES_AND_SPECIAL_CHARACTERS
bool FlexTokenizer::isWordEndChar(char textCharacter,
                                  TokenizeOpts tokenizationOptions)
{
    switch (tokenizationOptions)
    {
        case SPACES:
            return isspace(textCharacter);

        case SPECIAL_CHARACTERS:
            return (0 == pcre_exec(pattern_regex, pattern_extra, &textCharacter,
                             1 /*length*/, 0, 0, ovec, 0));

        case SPACES_AND_SPECIAL_CHARACTERS:
vt_report_error(0, "Program logic error: passed an invalid TokenizeOpts #: [%i] with string value: [%s]. XXX DELETEME XXX", tokenizationOptions, toString(tokenizationOptions));


        default:
            vt_report_error(0, "Program logic error: passed an invalid TokenizeOpts #: [%i] with string value: [%s].", tokenizationOptions, toString(tokenizationOptions));
    }

    return false;
}

// Tell Vertica that we take in a row with 1 string, and return a row with 1 string
void FlexTokenizerFactory::getPrototype(ServerInterface &srvInterface, ColumnTypes &arg_types, ColumnTypes &returnType)
{
    arg_types.addAny();
    returnType.addAny();
}

// Tell Vertica what our return string length will be, given the input
// string length
void FlexTokenizerFactory::getReturnType(ServerInterface &srvInterface,
                           const SizedColumnTypes &arg_types,
                           SizedColumnTypes &return_types)
{
    // Error out if we're called with anything but 1 argument
    if (arg_types.getColumnCount() < 2)
        vt_report_error(0, "Function only accepts 2 or more arguments, but [%zu] provided on map values retrieval", arg_types.getColumnCount());

    int input_len = arg_types.getColumnType(1).getStringLength();
    input_len = max(input_len, (int)MAX_TOKEN_LENGTH);

    // Our output size will never be more than the input size
    return_types.addVarchar(input_len, "token");
    
    // Handle output rows for added pass-through input rows
    std::vector<size_t> argCols;
    arg_types.getArgumentColumns(argCols);
    size_t colIdx = 0;
    for (std::vector<size_t>::iterator currCol = argCols.begin() + 2; currCol < argCols.end(); currCol++)
    {
        std::string inputColName = arg_types.getColumnName(*currCol);
        std::stringstream cname;
        if (inputColName.empty()) {
            cname << "col" << colIdx;
        } else {
            cname << inputColName;
        }
        colIdx++;
        return_types.addArg(arg_types.getColumnType(*currCol), cname.str());
    }

}

void FlexTokenizerFactory::getParameterType(ServerInterface &srvInterface,
                                               SizedColumnTypes &parameterTypes)
{
    parameterTypes.addVarchar(256, "TokenizeType");
}


TransformFunction * FlexTokenizerFactory::createTransformFunction(ServerInterface &srvInterface)
{
    TokenizeOpts tokenizeType = SPACES;
    ParamReader args = srvInterface.getParamReader();
    if (args.containsParameter("TokenizeType")) {
        tokenizeType = parse_tokenize_option(args.getStringRef("TokenizeType").str());
    }

    return vt_createFuncObj(srvInterface.allocator, FlexTokenizer, tokenizeType);
}

/// Faster to linearly search in such a small enum set than incur the overhead of generating a map lookup
TokenizeOpts FlexTokenizerFactory::parse_tokenize_option(std::string tokenizeTypeString) {
    for (int currTokenizeOptNum = 0; currTokenizeOptNum < NUM_TOKENIZE_OPTIONS; currTokenizeOptNum++) {
        if (strcasecmp(tokenizeTypeString.c_str(), TokenizeOpts_STRINGS[currTokenizeOptNum]) == 0) {
            return ((TokenizeOpts)currTokenizeOptNum);
        }
    }

    // Passed an invalid tokenization option
    std::stringstream availableTokenizeOpts;
    availableTokenizeOpts << TokenizeOpts_STRINGS[0];
    for (int currTokenizeOptNum = 1; currTokenizeOptNum < NUM_TOKENIZE_OPTIONS; currTokenizeOptNum++) {
        availableTokenizeOpts << ", " << TokenizeOpts_STRINGS[currTokenizeOptNum];
    }

    // XXX TODO XXX Add this back once VER-32893 is resolved and vt_report_error() here doesn't crash Vertica any more
//    vt_report_error(0, "Invalid tokenization option: [%s]. The valid tokenization options are: [%s].", tokenizeTypeString.c_str(), GetAvailableTokenizeOptsString().c_str());
    log("Invalid tokenization option: [%s]. The valid tokenization options are: [%s].", tokenizeTypeString.c_str(), GetAvailableTokenizeOptsString().c_str());
    return ((TokenizeOpts)-1);  // To remove the "no return" warning; this will always get skipped due to vt_report_error()'s Exception throw
}

void FlexSpecialCharactersTokenizerFactory::getParameterType(ServerInterface &srvInterface,
                                                             SizedColumnTypes &parameterTypes)
{ }

TransformFunction * FlexSpecialCharactersTokenizerFactory::createTransformFunction(ServerInterface &srvInterface)
{
    return vt_createFuncObj(srvInterface.allocator, FlexTokenizer, SPECIAL_CHARACTERS);
}

// Tell Vertica that we take in a row with 1 string, and return a row with 1 string
void FlexBinSpecialCharactersTokenizerFactory::getPrototype(ServerInterface &srvInterface, ColumnTypes &arg_types, ColumnTypes &returnType)
{
    arg_types.addLongVarbinary();

    returnType.addVarchar();
}

RegisterFactory(FlexTokenizerFactory);
//RegisterFactory(FlexSpecialCharactersTokenizerFactory);
//RegisterFactory(FlexBinSpecialCharactersTokenizerFactory);


} /// END namespace flextable
