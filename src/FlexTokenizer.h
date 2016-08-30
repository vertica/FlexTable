/**
 * Copyright (c) 2005 - 2015 Hewlett Packard Enterprise Development LP
 *
 * Creation Date: October 03, 2014
 */
/**
 * Description: UDx plugins for Flex Table SQL functions.
 */


#ifndef FLEXTOKENIZER_H_
#define FLEXTOKENIZER_H_

#include "Vertica.h"
#include "VerticaUDx.h"
#include "pcre.h"

#include "FlexTable.h"

using namespace Vertica;



namespace flextable
{


/// BEGIN Constants/Enums
static const uint32 MAX_TOKEN_LENGTH = 65000;


#define FOREACH_TOKENIZEOPTS_ENUM_VAL(ENUM_VAL) \
        ENUM_VAL(SPACES)   \
        ENUM_VAL(SPECIAL_CHARACTERS)  \
        ENUM_VAL(SPACES_AND_SPECIAL_CHARACTERS)   \

#define NUM_TOKENIZE_OPTIONS 3

enum TokenizeOpts {
    FOREACH_TOKENIZEOPTS_ENUM_VAL(GENERATE_ENUM)
};

static const char* TokenizeOpts_STRINGS[] = {
    FOREACH_TOKENIZEOPTS_ENUM_VAL(GENERATE_STRING)
};

static inline const std::string GetAvailableTokenizeOptsString() {
    std::stringstream toReturn;
    toReturn << TokenizeOpts_STRINGS[0];
    for (int currTokenizeOptNum = 1; currTokenizeOptNum < NUM_TOKENIZE_OPTIONS; currTokenizeOptNum++) {
        toReturn << ", " << TokenizeOpts_STRINGS[currTokenizeOptNum];
    }
    return toReturn.str();
}

static inline const char* toString(TokenizeOpts tokenizeOpt) {
    if (tokenizeOpt < 0 || tokenizeOpt >= NUM_TOKENIZE_OPTIONS) {
        return NULL;
    }
    return TokenizeOpts_STRINGS[tokenizeOpt];
}
/// END Constants/Enums



class FlexTokenizer : public TransformFunction
{

private:

     /*
     * The only non-TransformFunction member.
     *
     * Data member to store the passed arguments so we don't have to look them
     *  up again for each partition.
     */
    std::vector<size_t> input_cols;

public:
    FlexTokenizer(TokenizeOpts tokenizeType = SPACES);

    virtual void setup(ServerInterface &srvInterface, const SizedColumnTypes &argTypes);

    virtual void destroy(ServerInterface &srvInterface, const SizedColumnTypes &argTypes);

    virtual void processPartition(ServerInterface &srvInterface,
                                  PartitionReader &arg_reader,
                                  PartitionWriter &result_writer);

    void tokenizeMap(ServerInterface &srvInterface, PartitionReader &arg_reader, PartitionWriter &result_writer, const char * map_value, const int32 &map_size);

    void tokenizeStringValue(const char * value,
                             uint32 value_size,
                             PartitionReader &arg_reader,
                             PartitionWriter &result_writer,
                             TokenizeOpts tokenizationOptions);

    bool isWordEndChar(char textCharacter,
                       TokenizeOpts tokenizationOptions);

    private:
        TokenizeOpts tokenize_type;
        pcre *pattern_regex;
        pcre_extra *pattern_extra;
        int *ovec;
};

class FlexTokenizerFactory : public TransformFunctionFactory
{
    virtual void getPrototype(ServerInterface &srvInterface, ColumnTypes &argTypes, ColumnTypes &returnType);

    virtual void getReturnType(ServerInterface &srvInterface,
                               const SizedColumnTypes &argTypes,
                               SizedColumnTypes &returnTypes);

    void getParameterType(ServerInterface &srvInterface,
                          SizedColumnTypes &parameterTypes);

    virtual TransformFunction * createTransformFunction(ServerInterface &srvInterface);

    TokenizeOpts parse_tokenize_option(std::string tokenizeTypeString);
};

class FlexSpecialCharactersTokenizerFactory : public FlexTokenizerFactory
{
    void getParameterType(ServerInterface &srvInterface,
                          SizedColumnTypes &parameterTypes);

    virtual TransformFunction * createTransformFunction(ServerInterface &srvInterface);
};

class FlexBinSpecialCharactersTokenizerFactory : public FlexSpecialCharactersTokenizerFactory
{
    virtual void getPrototype(ServerInterface &srvInterface, ColumnTypes &argTypes, ColumnTypes &returnType);
};


} /// END namespace flextable

/// END define FLEXTOKENIZER_H_
#endif

