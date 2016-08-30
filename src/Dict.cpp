/**
 * Copyright (c) 2005 - 2015 Hewlett Packard Enterprise Development LP
 *
 * Creation Date: December 10, 2013
 */
/**
 * Description: UDx plugins for Flex Table SQL functions.
 */


#include <iostream>
#include "FlexTable.h"
#include "VMap.h"
#include "Dict.h"


namespace flextable
{




//////////////////////////// Single column map for 7.0


//// BEGIN maplookup()
void MapLookup::setup(ServerInterface &srvInterface, const SizedColumnTypes &arg_types)
{
    ParamReader args = srvInterface.getParamReader();
    if (args.containsParameter("case_sensitive")) {
        case_sensitive = args.getBoolRef("case_sensitive");
    }
    if (case_sensitive == vbool_null) {
        case_sensitive = vbool_false;
    }
    out_size = MapLookupFactory::getOutSize(srvInterface, arg_types);
}

void MapLookup::processBlock(ServerInterface &srvInterface,
                                     BlockReader &arg_reader,
                                     BlockWriter &result_writer)
{
    uint64 currRow = 0;
    do {
        currRow++;
        const VString& fullmap = arg_reader.getStringRef(0);
        VString& resultCol = result_writer.getStringRef();

        try {
            if (fullmap.isNull())
            {
                resultCol.setNull();
                result_writer.next();
                continue;
            }

            const VString& key = arg_reader.getStringRef(1);
            VMapBlockReader map_reader(srvInterface, fullmap);

            int32 index = map_reader.search(key, case_sensitive);
            if (index >= 0) {
                // XXX TODO? XXX Paramaterize the option to truncate: VER-32753
                const uint32 valueSize = map_reader.get_value_size(index);
                if (out_size < valueSize) {
                    LogDebugUDWarn("Error (buffer too small):  MapLookup was asked to return a value of length: [%d] but only had: [%d] Bytes to output to.", valueSize, out_size);
                    resultCol.setNull();
                } else {
                    const char *value = map_reader.get_value(index);
                    if (value) {
                        resultCol.copy(value, valueSize);
                    } else {
                        resultCol.setNull();
                    }
                }
            } else {
                resultCol.setNull();
            }
        } catch(std::exception& e) {
            resultCol.setNull();
            LogDebugUDBasic("Exception while processing partition row: [%llu] on map value lookup: [%s]", currRow, e.what());
        } catch(...) {
            resultCol.setNull();
            LogDebugUDBasic("Unknown exception while processing partition row: [%llu] on map value lookup.", currRow);
        }

        result_writer.next();
    } while (arg_reader.next());
}


MapLookupFactory::MapLookupFactory() {
    vol = IMMUTABLE;
}

// Tell Vertica that we take in a row with no columns, and return a row with 1 string
void MapLookupFactory::getPrototype(ServerInterface &srvInterface, ColumnTypes &arg_types, ColumnTypes &returnType)
{
    arg_types.addLongVarchar();
    arg_types.addVarchar();
    returnType.addLongVarchar();
}

// Tell Vertica what our return string length will be, given the input
// string length, iff we could do this per-row
void MapLookupFactory::getReturnType(ServerInterface &srvInterface,
                                     const SizedColumnTypes &arg_types,
                                     SizedColumnTypes &return_types)
{
    return_types.addLongVarchar(getOutSize(srvInterface, arg_types), MAP_VALUE_OUTPUT_NAME);
}

void MapLookupFactory::getParameterType(ServerInterface &srvInterface,
                                        SizedColumnTypes &arg_types)
{
    arg_types.addBool("case_sensitive");
    arg_types.addInt("buffer_size");
}

ScalarFunction * MapLookupFactory::createScalarFunction(ServerInterface &srvInterface)
{ return vt_createFuncObject<MapLookup>(srvInterface.allocator); }


MapBinLookupFactory::MapBinLookupFactory() {
    vol = IMMUTABLE;
}

// Tell Vertica that we take in a row with no columns, and return a row with 1 string
void MapBinLookupFactory::getPrototype(ServerInterface &srvInterface,
                                               ColumnTypes &arg_types,
                                               ColumnTypes &returnType)
{
    arg_types.addLongVarbinary();
    arg_types.addVarchar();
    returnType.addLongVarchar();
}
//// END maplookup()

//// BEGIN maptostring()
void MapToString::setup(ServerInterface &srvInterface, const SizedColumnTypes &arg_types)
{
    ParamReader args = srvInterface.getParamReader();
    if (args.containsParameter("canonical_json")) {
        canonical_json = args.getBoolRef("canonical_json");
    }
    if (canonical_json == vbool_null) {
        canonical_json = vbool_true;
    }
}

void MapToString::processBlock(ServerInterface &srvInterface,
                               BlockReader &arg_reader,
                               BlockWriter &result_writer)
{
    uint64 currRow = 0;
    do {
        currRow++;
        const VString& fullmap = arg_reader.getStringRef(0);
        VString& resultCol = result_writer.getStringRef();

        try {
            if (fullmap.isNull())
            {
                resultCol.setNull();
                result_writer.next();
                continue;
            }

            VMapBlockReader map_reader(srvInterface, fullmap);
            resultCol.copy(map_reader.to_string(srvInterface,canonical_json));
        } catch(std::exception& e) {
            resultCol.setNull();
            LogDebugUDBasic("Exception while processing partition row: [%llu] on map value to string: [%s]", currRow, e.what());
        } catch(...) {
            resultCol.setNull();
            LogDebugUDBasic("Unknown exception while processing partition row: [%llu] on map value to string.", currRow);
        }

        result_writer.next();
    } while (arg_reader.next());
}


MapToStringFactory::MapToStringFactory() {
    vol = IMMUTABLE;
}

// Tell Vertica that we take in a row with no columns, and return a row with 1 string
void MapToStringFactory::getPrototype(ServerInterface &srvInterface, ColumnTypes &arg_types, ColumnTypes &returnType)
{
    arg_types.addLongVarchar();
    returnType.addLongVarchar();
}

// Tell Vertica what our return string length will be, given the input
// string length
void MapToStringFactory::getReturnType(ServerInterface &srvInterface,
                           const SizedColumnTypes &arg_types,
                           SizedColumnTypes &return_types)
{
    return_types.addLongVarchar(arg_types.getColumnType(0).getStringLength(), MAP_STRING_OUTPUT_NAME);
}

void MapToStringFactory::getParameterType(ServerInterface &srvInterface,
                            SizedColumnTypes &arg_types)
{
    arg_types.addBool("canonical_json");
}

ScalarFunction * MapToStringFactory::createScalarFunction(ServerInterface &srvInterface)
{ return vt_createFuncObj(srvInterface.allocator, MapToString); }



MapBinToStringFactory::MapBinToStringFactory() {
    vol = IMMUTABLE;
}

// Tell Vertica that we take in a row with no columns, and return a row with 1 string
void MapBinToStringFactory::getPrototype(ServerInterface &srvInterface, ColumnTypes &arg_types, ColumnTypes &returnType)
{
    arg_types.addLongVarbinary();
    returnType.addLongVarchar();
}
//// END maptostring()


//// BEGIN mapcontainskey()
void MapContainsKey::processBlock(ServerInterface &srvInterface,
                           BlockReader &arg_reader,
                           BlockWriter &result_writer)
{
    uint64 currRow = 0;
    do {
        currRow++;
        const VString& fullmap = arg_reader.getStringRef(0);

        try {
            if (fullmap.isNull())
            {
                result_writer.setBool(vbool_null);
                result_writer.next();
                continue;
            }

            const VString& key = arg_reader.getStringRef(1);
            VMapBlockReader map_reader(srvInterface, fullmap);

            result_writer.setBool(map_reader.contains_key(key));
        } catch(std::exception& e) {
            result_writer.setBool(vbool_null);
            LogDebugUDBasic("Exception while processing partition row: [%llu] on map contains key: [%s]", currRow, e.what());
        } catch(...) {
            result_writer.setBool(vbool_null);
            LogDebugUDBasic("Unknown exception while processing partition row: [%llu] on map contains key.", currRow);
        }

        result_writer.next();
    } while (arg_reader.next());
}



MapContainsKeyFactory::MapContainsKeyFactory() {
    vol = IMMUTABLE;
}

// Tell Vertica that we take in a row with no columns, and return a row with 1 string
void MapContainsKeyFactory::getPrototype(ServerInterface &srvInterface, ColumnTypes &arg_types, ColumnTypes &returnType)
{
    arg_types.addLongVarchar();
    arg_types.addVarchar();
    returnType.addBool();
}

// Tell Vertica what our return string length will be, given the input
// string length
void MapContainsKeyFactory::getReturnType(ServerInterface &srvInterface,
                           const SizedColumnTypes &arg_types,
                           SizedColumnTypes &return_types)
{
    return_types.addBool(MAP_CONTAINS_KEY_OUTPUT_NAME);
}

ScalarFunction * MapContainsKeyFactory::createScalarFunction(ServerInterface &srvInterface)
{ return vt_createFuncObj(srvInterface.allocator, MapContainsKey); }



MapBinContainsKeyFactory::MapBinContainsKeyFactory() {
    vol = IMMUTABLE;
}

// Tell Vertica that we take in a row with no columns, and return a row with 1 string
void MapBinContainsKeyFactory::getPrototype(ServerInterface &srvInterface, ColumnTypes &arg_types, ColumnTypes &returnType)
{
    arg_types.addLongVarbinary();
    arg_types.addVarchar();
    returnType.addBool();
}
//// END mapcontainskey()


//// BEGIN mapcontainsvalue()
void MapContainsValue::processBlock(ServerInterface &srvInterface,
                           BlockReader &arg_reader,
                           BlockWriter &result_writer)
{
    uint64 currRow = 0;
    do {
        currRow++;
        const VString& fullmap = arg_reader.getStringRef(0);

        try {
            if (fullmap.isNull())
            {
                result_writer.setBool(vbool_null);
                result_writer.next();
                continue;
            }

            const VString& value = arg_reader.getStringRef(1);
            VMapBlockReader map_reader(srvInterface, fullmap);

            bool found_value = false;
            for (uint32 valueIndex = 0; valueIndex < map_reader.get_count(); valueIndex++) {
                if (value.length() == map_reader.get_value_size(valueIndex))
                {
                    const char *currValue = map_reader.get_value(valueIndex);
                    if (currValue && strncmp(value.data(), currValue, value.length()) == 0)
                    {
                        found_value = true;
                        break;
                    }
                }
            }
            result_writer.setBool(found_value);

        } catch(std::exception& e) {
            result_writer.setBool(vbool_null);
            LogDebugUDBasic("Exception while processing partition row: [%llu] on map contains value: [%s]", currRow, e.what());
        } catch(...) {
            result_writer.setBool(vbool_null);
            LogDebugUDBasic("Unknown exception while processing partition row: [%llu] on map contains value.", currRow);
        }

        result_writer.next();
    } while (arg_reader.next());
}



MapContainsValueFactory::MapContainsValueFactory() {
    vol = IMMUTABLE;
}

// Tell Vertica that we take in a row with no columns, and return a row with 1 string
void MapContainsValueFactory::getPrototype(ServerInterface &srvInterface, ColumnTypes &arg_types, ColumnTypes &returnType)
{
    arg_types.addLongVarchar();
    arg_types.addVarchar();
    returnType.addBool();
}

// Tell Vertica what our return string length will be, given the input
// string length
void MapContainsValueFactory::getReturnType(ServerInterface &srvInterface,
                           const SizedColumnTypes &arg_types,
                           SizedColumnTypes &return_types)
{
    return_types.addBool(MAP_CONTAINS_KEY_OUTPUT_NAME);
}

ScalarFunction * MapContainsValueFactory::createScalarFunction(ServerInterface &srvInterface)
{ return vt_createFuncObj(srvInterface.allocator, MapContainsValue); }



MapBinContainsValueFactory::MapBinContainsValueFactory() {
    vol = IMMUTABLE;
}

// Tell Vertica that we take in a row with no columns, and return a row with 1 string
void MapBinContainsValueFactory::getPrototype(ServerInterface &srvInterface, ColumnTypes &arg_types, ColumnTypes &returnType)
{
    arg_types.addLongVarbinary();
    arg_types.addVarchar();
    returnType.addBool();
}
//// END mapcontainsvalue()


//// BEGIN mapversion()
void MapVersion::processBlock(ServerInterface &srvInterface,
                                     BlockReader &arg_reader,
                                     BlockWriter &result_writer)
{
    uint64 currRow = 0;
    do {
        currRow++;
        const VString& fullmap = arg_reader.getStringRef(0);

        try {
            if (fullmap.isNull())
            {
                result_writer.setInt(-1);
                result_writer.next();
                continue;
            }

            result_writer.setInt(V1ImmutableVMap<strncmp>::map_version(fullmap));

        } catch(std::exception& e) {
            result_writer.setInt(-1);
            LogDebugUDBasic("Exception while processing partition row: [%llu] on map value version: [%s]", currRow, e.what());
        } catch(...) {
            result_writer.setInt(-1);
            LogDebugUDBasic("Unknown exception while processing partition row: [%llu] on map value version.", currRow);
        }

        result_writer.next();
    } while (arg_reader.next());
}


MapVersionFactory::MapVersionFactory() {
    vol = IMMUTABLE;
}

// Tell Vertica that we take in a row with no columns, and return a row with 1 string
void MapVersionFactory::getPrototype(ServerInterface &srvInterface, ColumnTypes &arg_types, ColumnTypes &returnType)
{
    arg_types.addLongVarchar();
    returnType.addInt();
}

// Tell Vertica what our return string length will be, given the input
// string length
void MapVersionFactory::getReturnType(ServerInterface &srvInterface,
                           const SizedColumnTypes &arg_types,
                           SizedColumnTypes &return_types)
{
    return_types.addLongVarchar(VALUE_FOOTPRINT, MAP_VALUE_OUTPUT_NAME);
}

ScalarFunction * MapVersionFactory::createScalarFunction(ServerInterface &srvInterface)
{ return vt_createFuncObj(srvInterface.allocator, MapVersion); }


MapBinVersionFactory::MapBinVersionFactory() {
    vol = IMMUTABLE;
}

// Tell Vertica that we take in a row with no columns, and return a row with 1 string
void MapBinVersionFactory::getPrototype(ServerInterface &srvInterface,
                                               ColumnTypes &arg_types,
                                               ColumnTypes &returnType)
{
    arg_types.addLongVarbinary();
    returnType.addInt();
}
//// END mapversion()


//// BEGIN mapsize()
void MapSize::processBlock(ServerInterface &srvInterface,
                                     BlockReader &arg_reader,
                                     BlockWriter &result_writer)
{
    uint64 currRow = 0;
    do {
        currRow++;
        const VString& fullmap = arg_reader.getStringRef(0);

        try {
            if (fullmap.isNull() || (!V1ImmutableVMap<strncmp>::is_valid_map(fullmap))) {
                result_writer.setInt(-1);
                result_writer.next();
                continue;
            }

            V1ImmutableVMap<strncmp> vmap(fullmap);
            result_writer.setInt(vmap.size());

        } catch(std::exception& e) {
            result_writer.setInt(-1);
            LogDebugUDBasic("Exception while processing partition row: [%llu] on map value size: [%s]", currRow, e.what());
        } catch(...) {
            result_writer.setInt(-1);
            LogDebugUDBasic("Unknown exception while processing partition row: [%llu] on map value size.", currRow);
        }

        result_writer.next();
    } while (arg_reader.next());
}


MapSizeFactory::MapSizeFactory() {
    vol = IMMUTABLE;
}

// Tell Vertica that we take in a row with no columns, and return a row with 1 string
void MapSizeFactory::getPrototype(ServerInterface &srvInterface, ColumnTypes &arg_types, ColumnTypes &returnType)
{
    arg_types.addLongVarchar();
    returnType.addInt();
}

// Tell Vertica what our return string length will be, given the input
// string length
void MapSizeFactory::getReturnType(ServerInterface &srvInterface,
                           const SizedColumnTypes &arg_types,
                           SizedColumnTypes &return_types)
{
    return_types.addLongVarchar(VALUE_FOOTPRINT, MAP_VALUE_OUTPUT_NAME);
}

ScalarFunction * MapSizeFactory::createScalarFunction(ServerInterface &srvInterface)
{ return vt_createFuncObj(srvInterface.allocator, MapSize); }


MapBinSizeFactory::MapBinSizeFactory() {
    vol = IMMUTABLE;
}

// Tell Vertica that we take in a row with no columns, and return a row with 1 string
void MapBinSizeFactory::getPrototype(ServerInterface &srvInterface,
                                               ColumnTypes &arg_types,
                                               ColumnTypes &returnType)
{
    arg_types.addLongVarbinary();
    returnType.addInt();
}
//// END mapsize()


//// BEGIN mapkeys()
void MapKeys::processPartition(ServerInterface &srvInterface,
                              PartitionReader &arg_reader,
                              PartitionWriter &result_writer)
{
    uint64 currRow = 0;
    try {
        if (arg_reader.getNumCols() != 1)
            vt_report_error(0, "Function only accepts 1 argument, but [%zu] provided on map keys retrieval", arg_reader.getNumCols());

        do {
            currRow++;
            const VString& fullmap = arg_reader.getStringRef(0);

            // If input string is NULL, then output is NULL as well
            if (!fullmap.isNull())
            {
                VMapBlockReader map_reader(srvInterface, fullmap);
                for (size_t pairNum = 0; pairNum < map_reader.get_count(); pairNum++) {
                    VString& resultCol = result_writer.getStringRef(0);
                    const char *key = map_reader.get_key(pairNum);
                    if (key) {
                        resultCol.copy(key, map_reader.get_key_size(pairNum));
                    } else {
                        resultCol.setNull();
                    }
                    result_writer.next();
                }
            }
        } while (arg_reader.next());
    } catch(std::exception& e) {
        vt_report_error(0, "Exception while processing partition row: [%llu] on map keys retrieval: [%s]", currRow, e.what());
    } catch(...) {
        vt_report_error(0, "Unknown exception while processing partition row: [%llu] on map keys retrieval.", currRow);
    }
}



// Tell Vertica that we take in a row with 1 string, and return a row with 1 string
void MapKeysFactory::getPrototype(ServerInterface &srvInterface, ColumnTypes &arg_types, ColumnTypes &returnType)
{
    arg_types.addLongVarchar();

    returnType.addVarchar();
}

// Tell Vertica what our return string length will be, given the input
// string length
void MapKeysFactory::getReturnType(ServerInterface &srvInterface,
                           const SizedColumnTypes &arg_types,
                           SizedColumnTypes &return_types)
{
    // Error out if we're called with anything but 1 argument
    if (arg_types.getColumnCount() != 1)
        vt_report_error(0, "Function only accepts 1 argument, but [%zu] provided on map keys retrieval", arg_types.getColumnCount());

    int input_len = arg_types.getColumnType(0).getStringLength();

    // Our output size will never be more than the input size
    return_types.addVarchar(input_len, MAP_KEY_NAMES_OUTPUT_NAME);
}

TransformFunction * MapKeysFactory::createTransformFunction(ServerInterface &srvInterface)
{ return vt_createFuncObj(srvInterface.allocator, MapKeys); }



// Tell Vertica that we take in a row with 1 string, and return a row with 1 string
void MapBinKeysFactory::getPrototype(ServerInterface &srvInterface, ColumnTypes &arg_types, ColumnTypes &returnType)
{
    arg_types.addLongVarbinary();

    returnType.addVarchar();
}
//// END mapkeys()


//// BEGIN mapkeysinfo()
void MapKeysInfo::processPartition(ServerInterface &srvInterface,
                              PartitionReader &arg_reader,
                              PartitionWriter &result_writer)
{
    uint64 currRow = 0;
    try {
        if (arg_reader.getNumCols() != 1)
            vt_report_error(0, "Function only accepts 1 argument, but [%zu] provided on map keys information retrieval", arg_reader.getNumCols());

        do {
            currRow++;
            const VString& fullmap = arg_reader.getStringRef(0);

            // If input string is NULL, then output is NULL as well
            if (!fullmap.isNull())
            {
                VMapBlockReader map_reader(srvInterface, fullmap);
                for (size_t pairNum = 0; pairNum < map_reader.get_count(); pairNum++) {
                    VString& resultCol = result_writer.getStringRef(0);
                    const char *key = map_reader.get_key(pairNum);
                    if (key) {
                        resultCol.copy(key, map_reader.get_key_size(pairNum));
                    } else {
                        resultCol.setNull();
                    }
                    result_writer.setInt(1, map_reader.get_value_size(pairNum));
                    result_writer.setInt(2, ((map_reader.is_valid_map(srvInterface, pairNum, false)) ? MapOID : LongVarbinaryOID));
                    result_writer.setInt(3, currRow);
                    result_writer.setInt(4, pairNum);
                    result_writer.next();
                }
            }
        } while (arg_reader.next());
    } catch(std::exception& e) {
        vt_report_error(0, "Exception while processing partition row: [%llu] on map keys information retrieval: [%s]", currRow, e.what());
    } catch(...) {
        vt_report_error(0, "Unknown exception while processing partition row: [%llu] on map keys information retrieval.", currRow);
    }
}



// Tell Vertica that we take in a row with 1 string, and return a row with 1 string, 1 int, and 1 bool
void MapKeysInfoFactory::getPrototype(ServerInterface &srvInterface, ColumnTypes &arg_types, ColumnTypes &returnType)
{
    arg_types.addLongVarchar();

    returnType.addVarchar();
    returnType.addInt();
    returnType.addInt();
    returnType.addInt();
    returnType.addInt();
}

// Tell Vertica what our return string length will be, given the input
// string length
void MapKeysInfoFactory::getReturnType(ServerInterface &srvInterface,
                           const SizedColumnTypes &arg_types,
                           SizedColumnTypes &return_types)
{
    // Error out if we're called with anything but 1 argument
    if (arg_types.getColumnCount() != 1)
        vt_report_error(0, "Function only accepts 1 argument, but [%zu] provided on map keys information retrieval", arg_types.getColumnCount());

    int input_len = arg_types.getColumnType(0).getStringLength();

    // Our output size will never be more than the input size
    return_types.addVarchar(input_len, MAP_KEY_NAMES_OUTPUT_NAME);
    return_types.addInt(MAP_KEYS_LENGTH_OUTPUT_NAME);
    return_types.addInt(MAP_KEYS_TYPE_OUTPUT_NAME);
    return_types.addInt(MAP_KEYS_ROW_NUM_OUTPUT_NAME);
    return_types.addInt(MAP_KEYS_KEY_NUM_OUTPUT_NAME);
}

TransformFunction * MapKeysInfoFactory::createTransformFunction(ServerInterface &srvInterface)
{ return vt_createFuncObj(srvInterface.allocator, MapKeysInfo); }



// Tell Vertica that we take in a row with 1 string, and return a row with 1 string
void MapBinKeysInfoFactory::getPrototype(ServerInterface &srvInterface, ColumnTypes &arg_types, ColumnTypes &returnType)
{
    arg_types.addLongVarbinary();

    returnType.addVarchar();
    returnType.addInt();
    returnType.addInt();
    returnType.addInt();
    returnType.addInt();
}
//// END mapkeysinfo()


//// BEGIN mapvalues()
void MapValues::processPartition(ServerInterface &srvInterface,
                              PartitionReader &arg_reader,
                              PartitionWriter &result_writer)
{
    uint64 currRow = 0;
    try {
        if (arg_reader.getNumCols() != 1)
            vt_report_error(0, "Function only accepts 1 argument, but [%zu] provided on map values retrieval", arg_reader.getNumCols());

        do {
            currRow++;
            const VString& fullmap = arg_reader.getStringRef(0);

            // If input string is NULL, then output is NULL as well
            if (!fullmap.isNull())
            {
                VMapBlockReader map_reader(srvInterface, fullmap);
                for (size_t pairNum = 0; pairNum < map_reader.get_count(); pairNum++) {
                    VString& resultCol = result_writer.getStringRef(0);
                    const char *value = map_reader.get_value(pairNum);
                    if (value) {
                        resultCol.copy(value, map_reader.get_value_size(pairNum));
                    } else {
                        resultCol.setNull();
                    }
                    result_writer.next();
                }
            }
        } while (arg_reader.next());
    } catch(std::exception& e) {
        vt_report_error(0, "Exception while processing partition row: [%llu] on map values retrieval: [%s]", currRow, e.what());
    } catch(...) {
        vt_report_error(0, "Unknown exception while processing partition row: [%llu] on map values retrieval.", currRow);
    }
}



// Tell Vertica that we take in a row with 1 string, and return a row with 1 string
void MapValuesFactory::getPrototype(ServerInterface &srvInterface, ColumnTypes &arg_types, ColumnTypes &returnType)
{
    arg_types.addLongVarchar();

    returnType.addVarchar();
}

// Tell Vertica what our return string length will be, given the input
// string length
void MapValuesFactory::getReturnType(ServerInterface &srvInterface,
                           const SizedColumnTypes &arg_types,
                           SizedColumnTypes &return_types)
{
    // Error out if we're called with anything but 1 argument
    if (arg_types.getColumnCount() != 1)
        vt_report_error(0, "Function only accepts 1 argument, but [%zu] provided on map values retrieval", arg_types.getColumnCount());

    int input_len = arg_types.getColumnType(0).getStringLength();

    // Our output size will never be more than the input size
    return_types.addVarchar(input_len, MAP_VALUE_NAMES_OUTPUT_NAME);
}

TransformFunction * MapValuesFactory::createTransformFunction(ServerInterface &srvInterface)
{ return vt_createFuncObj(srvInterface.allocator, MapValues); }



// Tell Vertica that we take in a row with 1 string, and return a row with 1 string
void MapBinValuesFactory::getPrototype(ServerInterface &srvInterface, ColumnTypes &arg_types, ColumnTypes &returnType)
{
    arg_types.addLongVarbinary();

    returnType.addVarchar();
}
//// END mapvalues()

//// BEGIN mapvaluesorfield()
void MapValuesOrField::processPartition(ServerInterface &srvInterface,
                                        PartitionReader &arg_reader,
                                        PartitionWriter &result_writer)
{
    uint64 currRow = 0;
    try {
        if (arg_reader.getNumCols() != 1)
            vt_report_error(0, "Function only accepts 1 argument, but [%zu] provided on map values retrieval", arg_reader.getNumCols());

        do {
            currRow++;
            const VString& fullmap = arg_reader.getStringRef(0);

            // If input string is not a map, output that string
            if (fullmap.isNull() || (!V1ImmutableVMap<strncmp>::is_valid_map(fullmap)))
            {
                VString& resultCol = result_writer.getStringRef(0);
                resultCol.copy(fullmap);
                result_writer.next();
            }
            else
            {
                VMapBlockReader map_reader(srvInterface, fullmap);
                for (size_t pairNum = 0; pairNum < map_reader.get_count(); pairNum++) {
                    VString& resultCol = result_writer.getStringRef(0);
                    const char *value = map_reader.get_value(pairNum);
                    if (value) {
                        resultCol.copy(value, map_reader.get_value_size(pairNum));
                    } else {
                        resultCol.setNull();
                    }
                    result_writer.next();
                }
            }
        } while (arg_reader.next());
    } catch(std::exception& e) {
        vt_report_error(0, "Exception while processing partition row: [%llu] on map values retrieval: [%s]", currRow, e.what());
    } catch(...) {
        vt_report_error(0, "Unknown exception while processing partition row: [%llu] on map values retrieval.", currRow);
    }
}



// Tell Vertica that we take in a row with 1 string, and return a row with 1 string
void MapValuesOrFieldFactory::getPrototype(ServerInterface &srvInterface, ColumnTypes &arg_types, ColumnTypes &returnType)
{
    arg_types.addLongVarchar();

    returnType.addVarchar();
}

// Tell Vertica what our return string length will be, given the input
// string length
void MapValuesOrFieldFactory::getReturnType(ServerInterface &srvInterface,
                                            const SizedColumnTypes &arg_types,
                                            SizedColumnTypes &return_types)
{
    // Error out if we're called with anything but 1 argument
    if (arg_types.getColumnCount() != 1)
        vt_report_error(0, "Function only accepts 1 argument, but [%zu] provided on map values retrieval", arg_types.getColumnCount());

    int input_len = arg_types.getColumnType(0).getStringLength();

    // Our output size will never be more than the input size
    return_types.addVarchar(input_len, MAP_VALUE_NAMES_OUTPUT_NAME);
}

TransformFunction * MapValuesOrFieldFactory::createTransformFunction(ServerInterface &srvInterface)
{ return vt_createFuncObj(srvInterface.allocator, MapValuesOrField); }

//// END mapvaluesorfield()

//// BEGIN mapitems()
void MapItems::setup(ServerInterface &srvInterface, const SizedColumnTypes &arg_types)
{
    arg_types.getArgumentColumns(arg_cols);
}

void MapItems::processPartition(ServerInterface &srvInterface,
                              PartitionReader &arg_reader,
                              PartitionWriter &result_writer)
{
    uint64 currRow = 0;
    try {
        if (arg_reader.getNumCols() < 1)
            vt_report_error(0, "Function only must accept at least 1 Flex VMap argument, but none passed.");

        do {
            currRow++;
            const VString& fullmap = arg_reader.getStringRef(0);

            // If input string is NULL, then output is NULL as well
            if (!fullmap.isNull())
            {
                VMapBlockReader map_reader(srvInterface, fullmap);
                for (size_t pairNum = 0; pairNum < map_reader.get_count(); pairNum++) {
                    VString& resultCol = result_writer.getStringRef(0);
                    const char *key = map_reader.get_key(pairNum);
                    if (key) {
                        resultCol.copy(key, map_reader.get_key_size(pairNum));
                    } else {
                        resultCol.setNull();
                    }
                    resultCol = result_writer.getStringRef(1);
                    const char *value = map_reader.get_value(pairNum);
                    if (value) {
                        resultCol.copy(value, map_reader.get_value_size(pairNum));
                    } else {
                        resultCol.setNull();
                    }

                    // Write the remaining arguments to output
                    size_t colIdx = 2;
                    for (std::vector<size_t>::iterator currCol = arg_cols.begin() + 1; currCol < arg_cols.end(); currCol++) {
                        result_writer.copyFromInput(colIdx++, arg_reader, *currCol);
                    }

                    result_writer.next();
                }
            }
        } while (arg_reader.next());
    } catch(std::exception& e) {
        vt_report_error(0, "Exception while processing partition row: [%llu] on map items retrieval: [%s]", currRow, e.what());
    } catch(...) {
        vt_report_error(0, "Unknown exception while processing partition row: [%llu] on map items retrieval.", currRow);
    }
}



// Tell Vertica that we take in a row with 1 string, and return a row with 1 string
void MapItemsFactory::getPrototype(ServerInterface &srvInterface, ColumnTypes &arg_types, ColumnTypes &returnType)
{
    arg_types.addAny();

    returnType.addAny();
}

// Tell Vertica what our return string length will be, given the input
// string length
void MapItemsFactory::getReturnType(ServerInterface &srvInterface,
                           const SizedColumnTypes &arg_types,
                           SizedColumnTypes &return_types)
{
    // Error out if we're called with anything but 1 argument
    if (arg_types.getColumnCount() < 1) {
        vt_report_error(0, "Function only must accept at least 1 Flex VMap argument, but none passed.");
    }
    // First argument must be a varbinary or varchar
    if (!arg_types.getColumnType(0).isStringType()) {
        vt_report_error(0, "First argument to Function must be of type varbinary");
    }


    int input_len = arg_types.getColumnType(0).getStringLength();

    // Include the two columns to hold the pairs of the map explosion
    // (NB: Our output size will never be more than the input size)
    return_types.addVarchar(input_len, MAP_KEY_NAMES_OUTPUT_NAME);
    return_types.addVarchar(input_len, MAP_VALUE_NAMES_OUTPUT_NAME);

    // Handle output rows for added pass-through input rows
    std::vector<size_t> argCols;
    arg_types.getArgumentColumns(argCols);
    size_t colIdx = 0;
    for (std::vector<size_t>::iterator currCol = argCols.begin() + 1; currCol < argCols.end(); currCol++)
    {
        std::string inputColName = arg_types.getColumnName(colIdx + 1);
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

TransformFunction * MapItemsFactory::createTransformFunction(ServerInterface &srvInterface)
{ return vt_createFuncObj(srvInterface.allocator, MapItems); }



// Tell Vertica that we take in a row with 1 string, and return a row with 1 string
void MapBinItemsFactory::getPrototype(ServerInterface &srvInterface, ColumnTypes &arg_types, ColumnTypes &returnType)
{
    arg_types.addLongVarbinary();

    returnType.addAny();
}
//// END mapitems()


//// BEGIN emptymap()
void EmptyMap::processBlock(ServerInterface &srvInterface,
                           BlockReader &arg_reader,
                           BlockWriter &result_writer)
{
    uint64 currRow = 0;
    try {
        do {
            currRow++;
            result_writer.getStringRef().copy(empty_map_data_buf, empty_map_data_len);
            result_writer.next();
        } while (arg_reader.next());
    } catch(std::exception& e) {
        vt_report_error(0, "Exception while processing partition row: [%llu] on empty map construction: [%s]", currRow, e.what());
    } catch(...) {
        vt_report_error(0, "Unknown exception while processing partition row: [%llu] on empty map construction", currRow);
    }
}



EmptyMapFactory::EmptyMapFactory() {
    vol = IMMUTABLE;
}

// Tell Vertica that we take in a row with no columns, and return a row with 1 string
void EmptyMapFactory::getPrototype(ServerInterface &srvInterface, ColumnTypes &arg_types, ColumnTypes &returnType)
{
    returnType.addLongVarbinary();
}

// Tell Vertica what our return string length will be, given the input
// string length
void EmptyMapFactory::getReturnType(ServerInterface &srvInterface,
                           const SizedColumnTypes &arg_types,
                           SizedColumnTypes &return_types)
{
    return_types.addLongVarbinary(EmptyMap::empty_map_data_len, MAP_RAW_MAP_OUTPUT_NAME);  // We know we're only one byte; Vertica can up-cast as needed
}

ScalarFunction * EmptyMapFactory::createScalarFunction(ServerInterface &srvInterface)
{ return vt_createFuncObj(srvInterface.allocator, EmptyMap); }
//// END emptymap()


//// BEGIN mapaggregate()
void MapAggregate::processPartition(ServerInterface &srvInterface,
                              PartitionReader &arg_reader,
                              PartitionWriter &result_writer)
{
    if (arg_reader.getNumCols() != 2)
        vt_report_error(0, "Function accepts exactly 2 arguments, but [%zu] provided on map aggregation", arg_reader.getNumCols());

    uint64 currRow = 0;

    char* pair_buf = (char*)srvInterface.allocator->alloc(MAP_FOOTPRINT);
    VMapPairWriter map_writer(pair_buf, MAP_FOOTPRINT);
    size_t total_offset = 0;

    try {
        do {
            currRow++;
            const VString& key = arg_reader.getStringRef(0);
            const VString& value = arg_reader.getStringRef(1);

            // Don't allow empty keys, only empty values
            if (key.isNull() || key.length() <= 0)
                continue;

            map_writer.append(srvInterface, key.data(), key.length(), value.isNull(), ((value.isNull()) ? NULL : value.data()), ((value.isNull()) ? 0 : value.length()));

        } while (arg_reader.next());
    } catch(std::exception& e) {
        vt_report_error(0, "Exception while processing partition row: [%llu] on map keys retrieval: [%s]", currRow, e.what());
    } catch(...) {
        vt_report_error(0, "Unknown exception while processing partition row: [%llu] on map keys retrieval.", currRow);
    }

    VString& resultCol = result_writer.getStringRef(0);
    try {
        char* map_data_buf = (char*)srvInterface.allocator->alloc(MAP_FOOTPRINT);
        VMapPairReader map_reader(map_writer);
        VMapBlockWriter::convert_vmap(srvInterface, map_reader,
                map_data_buf, MAP_FOOTPRINT, total_offset);

        if (total_offset > 0)
            resultCol.copy(map_data_buf, total_offset);
        else
            resultCol.setNull();

        result_writer.next();

    } catch(std::exception& e) {
        resultCol.setNull();
        LogDebugUDBasic("Exception while finalizing map aggregation: [%s]", e.what());
    } catch(...) {
        resultCol.setNull();
        LogDebugUDBasic("Unknown exception while finalizing on map aggregation.");
    }
}


// Tell Vertica that we take in a row with 1 string, and return a row with 1 string
void MapAggregateFactory::getPrototype(ServerInterface &srvInterface, ColumnTypes &arg_types, ColumnTypes &returnType)
{
    arg_types.addVarchar();
    arg_types.addVarchar();
    returnType.addLongVarbinary();
}

// Tell Vertica what our return string length will be, given the input
// string length
void MapAggregateFactory::getReturnType(ServerInterface &srvInterface,
                           const SizedColumnTypes &arg_types,
                           SizedColumnTypes &return_types)
{
    // Error out if we're called with anything but 1 argument
    if (arg_types.getColumnCount() != 2)
        vt_report_error(0, "Function accepts exactly 2 arguments, but [%zu] provided on map aggregation", arg_types.getColumnCount());

    return_types.addLongVarbinary(MAP_FOOTPRINT, MAP_RAW_MAP_OUTPUT_NAME);
}

TransformFunction * MapAggregateFactory::createTransformFunction(ServerInterface &srvInterface)
{ return vt_createFuncObj(srvInterface.allocator, MapAggregate); }


// Tell Vertica that we take in a row with 1 string, and return a row with 1 string
void MapLongAggregateFactory::getPrototype(ServerInterface &srvInterface, ColumnTypes &arg_types, ColumnTypes &returnType)
{
    arg_types.addLongVarchar();
    arg_types.addLongVarchar();
    returnType.addLongVarbinary();
}
//// END mapaggregate()

void PickBestType::setup(ServerInterface &srvInterface, const SizedColumnTypes &argTypes) {
    ParamReader params = srvInterface.getParamReader();
    if (params.containsParameter("ErrorMax")) {
        errmax = params.getIntRef("ErrorMax");
    }
    if (params.containsParameter("EnforceErrorThreshold")) {
        enforceErrorThreshold = params.getBoolRef("EnforceErrorThreshold");
    }

    // Load all types we will try to parse the input values
    inValue = vt_allocArray(srvInterface.allocator, char,
                            argTypes.getColumnType(0).getStringLength() + 1 /*null terminator*/);
    verticaTypes = vt_allocArray(srvInterface.allocator, VerticaType,
                                 TYPE_GUESSING_NUM_DATA_TYPES);
    verticaTypes[INT8] = VerticaType(Int8OID, -1);
    verticaTypes[TIMESTAMP] = VerticaType(TimestampOID, -1);
    verticaTypes[TIMESTAMP_TZ] = VerticaType(TimestampTzOID, -1);
    verticaTypes[BOOL] = VerticaType(BoolOID, -1);
    verticaTypes[TIME] = VerticaType(TimeOID,-1);
    verticaTypes[TIME_TZ] = VerticaType(TimeTzOID,-1);
    verticaTypes[FLOAT8] = VerticaType(Float8OID, -1);
    verticaTypes[DATE] = VerticaType(DateOID, -1);
    verticaTypes[INTERVAL] = VerticaType(IntervalOID,
                                  VerticaType::makeIntervalTypeMod(MAX_SECONDS_PRECISION,
                                                                   INTERVAL_DAY2SECOND));
}

void PickBestType::initAggregate(ServerInterface &srvInterface, IntermediateAggs &aggs) {
    // Initialize all type guessing output values to zero
    VString &out = aggs.getStringRef(0);
    VIAssert(out.max_size == TYPE_GUESSING_RESULT_LEN);
    memset(out.data(), 0, TYPE_GUESSING_RESULT_LEN);
    EE::setLenSV(out.sv, TYPE_GUESSING_RESULT_LEN);
}

void PickBestType::aggregate(ServerInterface &srvInterface, 
                             BlockReader &argReader, 
                             IntermediateAggs &aggs) {
    DataTypeCounters &dtc = *reinterpret_cast<DataTypeCounters*>(aggs.getStringRef(0).data());
    Oid typ = dtc.typeOid;

    try {
        do {
            const VString &input = argReader.getStringRef(0);
            dtc.rowcnt++;
            if (input.isNull() || !input.length()) {
                dtc.nullcnt++;
                continue; // ignore nulls and empty strings
            }

            // Update maximum input length
            dtc.len = (dtc.len < input.length())? input.length() : dtc.len;

            // short circuit for long texts, which cannot really be anything other than string/maps
            if (input.length() > 50) {
                //LogDebugUDBasic("Long input optimization");
                if (V1ImmutableVMap<strncmp>::is_valid_map(input)) {
                    typ = MapOID;
                    dtc.counts[VMAP]++;
                } else {
                    dtc.counts[VARCHAR]++;
                    if (++dtc.errcnt > errmax) {
                        typ = VarcharOID;
                    }
                }
                continue;
            }

            // The StringParsers object expects NULL terminated character strings
            // Make a copy of the input value, and set the null terminator
            memcpy(inValue, input.data(), input.length());
            NullTerminatedString s(inValue, input.length(), true, true);
            if (!s.size()) {
                // ignore empty strings after trimming leading/trailing spaces
                dtc.nullcnt++;
                continue;
            }

            bool retry = false; // parse value once more?
            bool parseWithTZ = false; // parse time/timestamp with timezone?
            // When re-trying type parsing, we don't want to retry with the same type
            bool parseType[TYPE_GUESSING_NUM_DATA_TYPES]; // parse value to this type?
            memset(parseType, 1, sizeof(parseType));
            do {
                switch (typ) {
                case VUnspecOID:
                    // try types in order, most specific to least:
                    //LogDebugUDBasic("Unspec startup");
                {
                    Vertica::vint val;
                    if (parseType[INT8] &&
                        sp.parseInt(s.ptr(), s.size(), 0, val, verticaTypes[INT8])) {
                        typ = Int8OID;
                        dtc.counts[INT8]++;
                        // Remember the largest number of digits needed
                        // in case we later merge with Numeric
                        dtc.prec = (s.size() > dtc.prec)? s.size() : dtc.prec;
                        retry = false;
                        break;
                    }
                }
                {
                    Vertica::vbool val;
                    if (parseType[BOOL] &&
                        sp.parseBool(s.ptr(), s.size(), 0, val, verticaTypes[BOOL])) {
                        // Boolean overlaps with integer
                        // Example: 'true'::bool=='1'::bool
                        // Favor integers. Don't prescribe type for the next value parsing.
                        typ = VUnspecOID;
                        dtc.counts[BOOL]++;
                        retry = false;
                        break;
                    }
                }
                {
                    // Numeric before Time/TimeTz as a range of numeric values can be 
                    // successfully parsed as concatenated times, such as 
                    // '1234.56'::time=='12:34:00.56' (VER-45266)
                    int prec = 0, scale = 0; // precision and scale
                    if (parseType[NUMERIC] &&
                        parseNumeric(s.ptr(), s.size(), prec, scale)) {
                        typ = NumericOID;
                        dtc.counts[NUMERIC]++;
                        dtc.prec = (prec > dtc.prec)? prec : dtc.prec;
                        dtc.scale = (scale > dtc.scale)? scale : dtc.scale;
                        retry = false;
                        break;
                    }
                }
                {
                    Vertica::vfloat val;
                    if (parseType[FLOAT8] &&
                        sp.parseFloat(s.ptr(), s.size(), 0, val, verticaTypes[FLOAT8])) {
                        //LogDebugUDBasic("Found initial float");
                        typ = Float8OID;
                        dtc.counts[FLOAT8]++;
                        retry = false;
                        break;
                    }
                }
                {
                    if (parseType[VMAP] &&
                        s.size() >= (size_t) EmptyMap::empty_map_data_len &&
                        V1ImmutableVMap<strncmp>::is_valid_map(input)) {
                        typ = MapOID;
                        dtc.counts[VMAP]++;
                        retry = false;
                        break;
                    }
                }
                {
                    parseWithTZ = mayHaveTimezone(s.ptr(), s.size());
                    if (!parseWithTZ && parseType[TIMESTAMP]) {
                        Vertica::Timestamp val;
                        try {
                            val = Vertica::timestampInNoTzNameCheck(s.ptr(), verticaTypes[TIMESTAMP].getTypeMod(), false);
                        } catch(...) {
                            val = vint_null;
                        }
                        if (val != vint_null) {
                            if (kMod(val,usPerDay) != 0) {
                                // we can catch Date with this parse, differentiate
                                typ = TimestampOID;
                                dtc.counts[TIMESTAMP]++;
                            } else {
                                typ = DateOID;
                                dtc.counts[DATE]++;
                            }
                            retry = false;
                            break;
                        }
                    }
                }
                {
                    if (parseWithTZ && parseType[TIMESTAMP_TZ]) {
                        Vertica::TimestampTz val;
                        try {
                            val = Vertica::timestamptzInNoTzNameCheck(s.ptr(), verticaTypes[TIMESTAMP_TZ].getTypeMod(), false);
                        } catch(...) {
                            val = vint_null;
                        }
                        if (val != vint_null) {
                            typ = TimestampTzOID;
                            dtc.counts[TIMESTAMP_TZ]++;
                            retry = false;
                            break;
                        }
                    }
                }
                {
                    if (!parseWithTZ && parseType[TIME]) {
                        Vertica::TimeADT val;
                        try {
                            val = Vertica::timeInNoTzNameCheck(s.ptr(), verticaTypes[TIME].getTypeMod(), false);
                        } catch(...) {
                            val = vint_null;
                        }
                        if (val != vint_null) {
                            // Timestamp[Tz] values could parse to Time[Tz]
                            // (the date portion is ignored).
                            // Don't prescribe the type for the next value parsing
                            typ = VUnspecOID;
                            dtc.counts[TIME]++;
                            retry = false;
                            break;
                        }
                    }
                }
                {
                    if (parseWithTZ && parseType[TIME_TZ]) {
                        Vertica::TimeTzADT val;
                        try {
                            val = Vertica::timetzInNoTzNameCheck(s.ptr(), verticaTypes[TIME_TZ].getTypeMod(), false);
                        } catch(...) {
                            val = vint_null;
                        }
                        if (val != vint_null) {
                            // Timestamp[Tz] values could parse to Time[Tz]
                            // (the date portion is ignored).
                            // Don't prescribe the type for the next value parsing
                            typ = VUnspecOID;
                            dtc.counts[TIME_TZ]++;
                            retry = false;
                            break;
                        }
                    }
                }
                {
                    if (parseType[INTERVAL]) {
                        Vertica::Interval val;
                        try {
                            val = Vertica::intervalIn(s.ptr(), verticaTypes[INTERVAL].getTypeMod(), false);
                        } catch(...) {
                            val = vint_null;
                        }
                        if (val != vint_null) {
                            // Interval overlaps with numeric
                            // Example: '345.67'::interval=='345 16:04:48'
                            // Favor numeric. So don't prescribe type for the next parsing
                            typ = VUnspecOID;
                            dtc.counts[INTERVAL]++;
                            retry = false;
                            break;
                        }
                    }
                }

                // The value doesn't look like any type we care about. Count it as string
                dtc.counts[VARCHAR]++;
                retry = false;
                if (++dtc.errcnt > errmax) {
                    // We tried unsuccessfully parsing into a specific type enough times
                    // Go with string from now on
                    typ = VarcharOID;
                }
                break;

                case DateOID: // these type parsers overlap, always use broader
                case TimestampOID:
                case TimestampTzOID: {
                    parseWithTZ = mayHaveTimezone(s.ptr(), s.size());
                    if (!parseWithTZ) {
                        Vertica::Timestamp val;
                        try {
                            val = Vertica::timestampInNoTzNameCheck(s.ptr(), verticaTypes[TIMESTAMP].getTypeMod(), false);
                        } catch(...) {
                            val = vint_null;
                        }
                        if (val != vint_null) {
                            if (kMod(val,usPerDay) != 0) {
                                // we can catch Date with this parse, differentiate
                                typ = TimestampOID;
                                dtc.counts[TIMESTAMP]++;
                            } else {
                                typ = DateOID;
                                dtc.counts[DATE]++;
                            }
                            retry = false;
                            break;
                        }
                    } else {
                        Vertica::TimestampTz val;
                        try {
                            val = Vertica::timestamptzInNoTzNameCheck(s.ptr(), verticaTypes[TIMESTAMP_TZ].getTypeMod(), false);
                        } catch(...) {
                            val = vint_null;
                        }
                        if (val != vint_null) {
                            typ = TimestampTzOID;
                            dtc.counts[TIMESTAMP_TZ]++;
                            retry = false;
                            break;
                        }
                    }
                    // Failed to parse to Timestamp[Tz]
                    if (++dtc.errcnt > errmax) {
                        typ = VarcharOID;
                        dtc.counts[VARCHAR]++;
                    } else {
                        // Try parsing with another type
                        typ = VUnspecOID;
                        retry = true;
                        parseType[TIMESTAMP] = false;
                        parseType[TIMESTAMP_TZ] = false;
                    }
                    break;
                }

                case Int8OID: {
                    Vertica::vint val;
                    if (!sp.parseInt(s.ptr(), s.size(),0,val, verticaTypes[INT8])) {
                        parseType[INT8] = false;
                        // Try with numeric
                        int prec = 0, scale = 0; // precision and scale
                        if (!parseNumeric(s.ptr(), s.size(), prec, scale)) {
                            parseType[NUMERIC] = false;
                            // Try with float
                            Vertica::vfloat val2;
                            if (sp.parseFloat(s.ptr(), s.size(), 0, val2, verticaTypes[FLOAT8])) {
                                dtc.counts[FLOAT8]++;
                                typ = Float8OID;
                                break;
                            }
                            parseType[FLOAT8] = false;

                            if (++dtc.errcnt > errmax) {
                                typ = VarcharOID;
                                dtc.counts[VARCHAR]++;
                            } else {
                                // Try parsing with another type
                                retry = true;
                                typ = VUnspecOID;
                            }
                        } else {
                            typ = NumericOID;
                            dtc.counts[NUMERIC]++;
                            dtc.prec = (prec > dtc.prec)? prec : dtc.prec;
                            dtc.scale = (scale > dtc.scale)? scale : dtc.scale;
                        }
                    } else {
                        dtc.counts[INT8]++;
                    }
                    break;
                }

                case Float8OID: {
                    Vertica::vfloat val;
                    if (!sp.parseFloat(s.ptr(), s.size(), 0, val, verticaTypes[FLOAT8])) {
                        //LogDebugUDBasic("Mismatched second float");
                        if (++dtc.errcnt > errmax) {
                            typ = VarcharOID;
                            dtc.counts[VARCHAR]++;
                        } else {
                            // Try parsing with another type
                            typ = VUnspecOID;
                            retry = true;
                            parseType[FLOAT8] = false;
                        }
                    } else {
                        dtc.counts[FLOAT8]++;
                    }
                    break;
                }
              
                case IntervalOID: {
                    Vertica::Interval val;
                    try {
                        val = Vertica::intervalIn(s.ptr(), verticaTypes[INTERVAL].getTypeMod(), false);
                    } catch(...) {
                        val = vint_null;
                    }
                    if (val != vint_null) {
                        dtc.counts[INTERVAL]++;
                    } else {
                        if (++dtc.errcnt > errmax) {
                            typ = VarcharOID;
                            dtc.counts[VARCHAR]++;
                        } else {
                            // Try parsing with another type
                            typ = VUnspecOID;
                            retry = true;
                            parseType[INTERVAL] = false;
                        }
                    }
                    break;
                }

                case BoolOID: {
                    Vertica::vbool val;
                    if (!sp.parseBool(s.ptr(), s.size(), 0, val, verticaTypes[BOOL])) {
                        if (++dtc.errcnt > errmax) {
                            typ = VarcharOID;
                            dtc.counts[VARCHAR]++;
                        } else {
                            // Try parsing with another type
                            typ = VUnspecOID;
                            retry = true;
                            parseType[BOOL] = false;
                        }
                    } else {
                        dtc.counts[BOOL]++;
                    }
                    break;
                }

                case NumericOID: {
                    int prec = 0, scale = 0; // precision and scale
                    if (!parseNumeric(s.ptr(), s.size(), prec, scale)) {
                        // Try with float
                        Vertica::vfloat val;
                        if (sp.parseFloat(s.ptr(), s.size(), 0, val, verticaTypes[FLOAT8])) {
                            typ = Float8OID;
                            dtc.counts[FLOAT8]++;
                            break;
                        }
                        if (++dtc.errcnt > errmax) {
                            typ = VarcharOID;
                            dtc.counts[VARCHAR]++;
                        } else {
                            // Try parsing with another type
                            typ = VUnspecOID;
                            retry = true;
                            parseType[NUMERIC] = false;
                        }
                    } else { 
                        dtc.counts[NUMERIC]++;
                        dtc.prec = (prec > dtc.prec)? prec : dtc.prec;
                        dtc.scale = (scale > dtc.scale)? scale : dtc.scale;
                        typ = NumericOID;
                    }
                    break;
                }

                case MapOID: {
                    if (!V1ImmutableVMap<strncmp>::is_valid_map(input)) {
                        if (++dtc.errcnt > errmax) {
                            typ = VarcharOID;
                            dtc.counts[VARCHAR]++;
                        } else {
                            // Try parsing with another type
                            typ = VUnspecOID;
                            retry = true;
                            parseType[VMAP] = false;
                        }
                    } else { 
                        dtc.counts[VMAP]++;
                    }
                    break;
                }

                default: {
                    dtc.errcnt++;
                    typ = VarcharOID;
                    dtc.counts[VARCHAR]++;
                    break;
                }
                }
            } while (retry);
        } while (argReader.next());
    } catch(std::exception& e) {
        // Standard exception. Quit.
        vt_report_error(0, "Exception while processing aggregate: [%s]", e.what());
    }

    dtc.typeOid = typ;
}

bool PickBestType::mayHaveTimezone(const char* s, uint32 len) const {
    if (!len) {
        return false;
    }

    // skip spaces
    uint32 i = len - 1;
    while (i && isspace(s[i])) {
        i--;
    }

    // Check for alphanumeric characters in the last bytes
    if (isalpha(s[i])) {
        // Check for Time AM/PM strings
        // Find the beginning of the alphabetic character sequence
        // and check if we match a Meridian label
        uint32 end = i + 1; // exclusive
        while (i && !isspace(s[--i])) {
            ;
        }
        if (isspace(s[i])) {
            if (strncasecmp(&s[i + 1], "pm", end - i) == 0 ||
                strncasecmp(&s[i + 1], "am", end - i) == 0) {
                return false;
            }
        }
        return true;
    }

    // Check if the ending is like: [+-][0-9]+
    if (!isdigit(s[i])) {
        return false;
    }

    // Skip digits and spaces
    while (i && (isdigit(s[i]) || isspace(s[i]))) {
        i--;
    }

    // There should be a +/- sign here to qualify
    if (s[i] == '+') {
        return true;
    } else if (s[i] == '-') {
        // This could be a date 'yy-mm-dd' without timezone
        // Check there is at least two more '-' signs or none
        int count = 0;
        while (i) {
            if (s[--i] == '-') {
                count++;
            }
        }
        if (!count || count > 1) {
            return true;
        }
    }
    return false;
}

void PickBestType::combine(ServerInterface &srvInterface, 
                           IntermediateAggs &aggs, 
                           MultipleIntermediateAggs &aggsOther) {
    DataTypeCounters &dtc1 = *reinterpret_cast<DataTypeCounters*>(aggs.getStringRef(0).data());
    do {
        const DataTypeCounters &dtc2 = *reinterpret_cast<const DataTypeCounters*>(aggsOther.getStringRef(0).data());
        // Accumulate counts. Don't pick the best type yet.
        // We choose the best type in terminate().
        dtc1 += dtc2;
    } while (aggsOther.next());
}

void PickBestType::terminate(ServerInterface &srvInterface, 
                             BlockWriter &resWriter, 
                             IntermediateAggs &aggs) {
    DataTypeCounters &dtc = *reinterpret_cast<DataTypeCounters*>(aggs.getStringRef(0).data());

    // Do data type merging
    // For instance, if there are INT8 and FLOAT8 values,
    // INT8 values can coerce to FLOAT8 without data loss
    {
        // Int8->Numeric
        if (dtc.counts[INT8] && dtc.counts[NUMERIC]) {
            dtc.counts[NUMERIC] += dtc.counts[INT8];
            dtc.counts[INT8] = 0;
            // dtc.prec has the largest numeric precision required as well as
            // the largest number of digits of integers we processed
        }

        // Int8->Float8
        if (dtc.counts[INT8] && dtc.counts[FLOAT8]) {
            dtc.counts[FLOAT8] += dtc.counts[INT8];
            dtc.counts[INT8] = 0;
        }

        // Numeric->Float8
        if (dtc.counts[NUMERIC] && dtc.counts[FLOAT8]) {
            dtc.counts[FLOAT8] += dtc.counts[NUMERIC];
            dtc.counts[NUMERIC] = 0;
        }
        
        // Date->Timestamp
        if (dtc.counts[DATE] && dtc.counts[TIMESTAMP]) {
            dtc.counts[TIMESTAMP] += dtc.counts[DATE];
            dtc.counts[DATE] = 0;
        }

        // Date->TimestampTz
        if (dtc.counts[DATE] && dtc.counts[TIMESTAMP_TZ]) {
            dtc.counts[TIMESTAMP_TZ] += dtc.counts[DATE];
            dtc.counts[DATE] = 0;
        }

        // Timestamp->TimestampTz
        if (dtc.counts[TIMESTAMP] && dtc.counts[TIMESTAMP_TZ]) {
            dtc.counts[TIMESTAMP_TZ] += dtc.counts[TIMESTAMP];
            dtc.counts[TIMESTAMP] = 0;
        }

        // TimeTz->Time
        if (dtc.counts[TIME] && dtc.counts[TIME_TZ]) {
            dtc.counts[TIME_TZ] += dtc.counts[TIME];
            dtc.counts[TIME] = 0;
        }
    }

    // Increase Numeric prec/scale for future values
    if (dtc.counts[NUMERIC]) {
        dtc.prec += NUMERIC_PREC_INCREMENT_FACTOR;
        if (dtc.prec > 1024) {
            dtc.prec = 1024;
        }
        dtc.scale += NUMERIC_SCALE_INCREMENT_FACTOR;
        if (dtc.scale > dtc.prec) {
            dtc.scale = dtc.prec;
        }
    }

    dtc.errcnt = 0;
    uint64 largestCount = 0;
    if (dtc.nullcnt < dtc.rowcnt) {
        // Find index of the largest count. It will be the candidate to best data type
        // NB: We (arbitrarily) break ties by choosing the data type with the smaller Oid
        int bestTypeIdx = INT8; // index of the largest count
        largestCount = dtc.counts[INT8];
        for (int i = INT8 + 1; i < LAST_UNUSED; i++) {
            const uint64 currCount = dtc.counts[i];
            if (currCount > largestCount) {
                bestTypeIdx = i;
                largestCount = currCount;
            }
        }
        largestCount += dtc.nullcnt; // NULL values can coerce to the best type
        // Go with the best type found so far
        dtc.typeOid = DataTypeIndexToTypeOid[bestTypeIdx];
        // Compute error count
        if (dtc.typeOid != VarcharOID) {
            // Add up type counts other than the best type's
            // We assume those can't coerce to the best type
            for (int i = INT8; i < LAST_UNUSED; i++) {
                if (i != bestTypeIdx) {
                    dtc.errcnt += dtc.counts[i];
                }
            }
        }
    } else {
        // Most values are NULL. Go conservatively with string
        dtc.typeOid = VarcharOID;
        largestCount = dtc.rowcnt;
    }

    // Apply error threshold if user wants so
    if (enforceErrorThreshold && (largestCount / (vfloat) dtc.rowcnt) <
        (1 - TYPE_GUESSING_ERROR_PCT_THRESHOLD)) {
        // Go with string data type. If we have VMap values, go with binary string type
        if (dtc.counts[VMAP]) {
            dtc.typeOid = MapOID;
        } else {
            dtc.typeOid = VarcharOID;
        }
        dtc.errcnt = 0;
    }

    VString &result = resWriter.getStringRef();
    VIAssert(result.max_size == TYPE_GUESSING_RESULT_LEN);
    memcpy(result.data(), &dtc, TYPE_GUESSING_RESULT_LEN);
    EE::setLenSV(result.sv, TYPE_GUESSING_RESULT_LEN);
}

const uint16 PickBestTypeMerge::MIN_PRECISION_FOR_INT8 = 18;
const uint16 PickBestTypeMerge::MIN_STRING_LEN = 20;
const uint16 PickBestTypeMerge::MAX_SHORT_STRING_LEN = 65000;
const uint16 PickBestType::NUMERIC_PREC_INCREMENT_FACTOR = 3;
const uint16 PickBestType::NUMERIC_SCALE_INCREMENT_FACTOR = 1;
const std::string MapPutFactory::KEYS_PARAM_NAME("keys");
const uint32 MapPutFactory::MAX_KEYS_PARAM_SIZE = 1000000;
const uint16 MapPut::MAX_CONVBUF_LENGTH = 1024 + 3 /*0. and '\0'*/;
const uint8 MapPut::MIN_VINT_LENGTH = 20;
const uint8 MapPut::MIN_VFLOAT_LENGTH = 22;

bool PickBestTypeMerge::DataType::canSafelyParseTo(const DataType &dt2) {
    if (oid == dt2.oid || dt2.isStringType()) {
        // Anything can be converted to string or to self
        return true;
    }

    switch(dt2.oid) {
    case Int8OID: {
        return false;
    }

    case BoolOID: 
    case DateOID:
    case TimeOID: {
        return false;
    }

    case TimestampOID: {
        if (oid == DateOID) {
            return true;
        }
        return false;
    }

    case TimestampTzOID: {
        if (oid == DateOID || oid == TimestampOID) {
            return true;
        }
        return false;
    }

    case TimeTzOID: {
        if (oid == TimeOID) {
            return true;
        }
        return false;
    }

    case NumericOID: {
        if (oid == Int8OID) {
            return true;
        }
        return false;
    }

    case Float8OID: {
        if (oid == Int8OID || oid == NumericOID) {
            return true;
        }
        return false;
    }

    default:
        return false;
    }
}

Oid PickBestTypeMerge::DataType::getTypeOid(std::string typeStr) {
    // Lower case string to ease comparison
    std::transform(typeStr.begin(), typeStr.end(), typeStr.begin(), ::tolower);
    const char *str = typeStr.c_str();

    if (strcmp(str, "varchar") == 0) {
        return VarcharOID;
    }
    if (strcmp(str, "long varchar") == 0) {
        return LongVarcharOID;
    }
    if (strcmp(str, "varbinary") == 0) {
        return VarbinaryOID;
    }
    if (strcmp(str, "long varbinary") == 0) {
        return LongVarbinaryOID;
    }
    if (strcmp(str, "integer") == 0 || strcmp(str, "int8") == 0) {
        return Int8OID;
    }
    if (strcmp(str, "timestamp") == 0) {
        return TimestampOID;
    }
    if (strcmp(str, "date") == 0) {
        return DateOID;
    }
    if (strcmp(str, "timestamptz") == 0) {
        return TimestampTzOID;
    }
    if (strcmp(str, "boolean") == 0 || strcmp(str, "bool") == 0) {
        return BoolOID;
    }
    if (strcmp(str, "time") == 0) {
        return TimeOID;
    }
    if (strcmp(str, "timetz") == 0) {
        return TimeTzOID;
    }
    if (strcmp(str, "numeric") == 0) {
        return NumericOID;
    }
    if (strcmp(str, "float") == 0 || strcmp(str, "float8") == 0) {
        return Float8OID;
    }
    if (strcmp(str, "interval") == 0) {
        return IntervalOID;
    }

    return VUnspecOID;
}

void PickBestTypeMerge::setTypeMod(VerticaType &vt, const DataType &dt) {
    if (dt.oid == NumericOID) {
        vt.setTypeMod(VerticaType::makeNumericTypeMod(dt.prec, dt.scale));
    } else if (dt.oid == IntervalOID) {
        vt.setTypeMod(VerticaType::makeIntervalTypeMod(dt.prec, dt.scale));
    } else if (dt.isStringType()) {
        vt.setTypeMod(VerticaType::makeStringTypeMod(dt.len));
    }
}

std::string PickBestTypeMerge::expandTypeParameters(const DataType &dt1, const DataType &dt2) {
    // Expand type parameters
    DataType outdt(dt1);
    outdt.expandType(dt2);

    // Output type
    VerticaType vt(dt1.oid, -1);

    // Adjust TypeMod
    setTypeMod(vt, outdt);

    // SDK's VerticaType::getPrettyPrintStr() returns synonyms for these types:
    //   v_catalog.types: Integer, SDK: Int8
    //   v_catalog.types: Float,   SDK: Float8
    //   v_catalog.types: Boolean, SDK: Bool
    // To avoid breaking tests, uniform them to types style,
    // which is what we used in older releases
    std::string result = vt.getPrettyPrintStr();
    if (result.compare("Int8") == 0) {
        result = "Integer";
    } else if (result.compare("Float8") == 0) {
        result = "Float";
    } else if (result.compare("Bool") == 0) {
        result = "Boolean";
    }
    return result;
}

std::string PickBestTypeMerge::downgradeToString(const DataType &dt1, const DataType &dt2) {
    uint32 len = std::max(dt1.len, dt2.len);
    VerticaType vt(VarcharOID, -1);

    // Set string length
    if (len < MIN_STRING_LEN) {
        len = MIN_STRING_LEN;
    }
    vt.setTypeMod(VerticaType::makeStringTypeMod(len));

    // Choose between short string and long string
    if (len > MAX_SHORT_STRING_LEN || dt1.isLongStringType() || dt2.isLongStringType()) {
        vt.setTypeOid(LongVarcharOID);
    }

    // Choose between character string and binary string
    if (dt1.isBinaryStringType() || dt2.isBinaryStringType()) {
        if (vt.getTypeOid() == LongVarcharOID) {
            vt.setTypeOid(LongVarbinaryOID);
        } else {
            vt.setTypeOid(VarbinaryOID);
        }
    }

    return vt.getPrettyPrintStr();
}

std::string PickBestTypeMerge::getBestType(const VString &oldType, const vint &oldFreq,
                                           const VString &newType, const vint &newFreq,
                                           const vint &newErrorCount) {
    if (oldType.equal(&newType)) {
        // Same data type and parameters (if any). Nothing to do
        return oldType.str();
    }

    DataType dt1, dt2; // old and new data types
    dt1.setTypeParameters(oldType);
    dt2.setTypeParameters(newType);
    const vint rowCount = oldFreq + newFreq;

    // Pick the more specific data type that generates an error rate under the threshold
    // Resource to string type as the last option
    if (!dt1.isStringType()) {
        if (dt2.canSafelyParseTo(dt1)) {
            return expandTypeParameters(dt1, dt2);
        }
        // You could still go with the old type if the error rate doesn't go over the threshold.
        // NB: This error rate is over-estimated. Some *new* values may parse
        //     correctly to the old type, specifically those counted in newErrorCount
        if ((newFreq / (vfloat) rowCount) <= TYPE_GUESSING_ERROR_PCT_THRESHOLD) {
            return expandTypeParameters(dt1, dt2);
        }
    }

    if (!dt2.isStringType()) {
        if (dt1.canSafelyParseTo(dt2)) {
            return expandTypeParameters(dt2, dt1);
        }
        // You could still go with the new type if the error rate doesn't go over the threshold
        // NB: This error rate is over-estimated. Some *old* values may parse
        //     correctly to the new type.
        if ((oldFreq / (vfloat) rowCount) <= TYPE_GUESSING_ERROR_PCT_THRESHOLD) {
            return expandTypeParameters(dt2, dt1);
        }
    }

    // Both types are string, or neither type could be parsed into 
    // the other one while maintaining a low error rate
    // Downgrade to string
    return downgradeToString(dt1, dt2);
}

void PickBestTypeMerge::DataType::setTypeParameters(const VString &typeStr) {
    name = "";
    oid = VUnspecOID;
    std::string typeWithParams = typeStr.str();
    size_t sPar = typeWithParams.find("(");

    if (sPar == std::string::npos) {
        // No length or precision/scale
        name = typeWithParams;
        oid = getTypeOid(typeWithParams);
        return;
    }
    name = typeWithParams.substr(0, sPar);
    oid = getTypeOid(name);

    // Look for precision/scale
    size_t ePar = typeWithParams.rfind(")");
    size_t cSep = typeWithParams.find(",", sPar);
    if (cSep == std::string::npos) {
        // No precision/scale
        std::string lenStr = typeWithParams.substr(sPar + 1, ePar - sPar - 1);
        len = atoi(lenStr.c_str());
        return;
    }

    // Get precision and scale
    std::string precStr = typeWithParams.substr(sPar + 1, cSep - sPar - 1);
    std::string scaleStr = typeWithParams.substr(cSep + 1, ePar - cSep - 1);
    prec = atoi(precStr.c_str());
    scale = atoi(scaleStr.c_str());
}

void PickBestTypeMerge::processBlock(ServerInterface &srvInterface,
                                     BlockReader &arg_reader,
                                     BlockWriter &result_writer) {
    try {
        do {
            const VString &oldType = arg_reader.getStringRef(0);
            const VString &newType = arg_reader.getStringRef(2);
            {
                // Input comes from a full outer join. NULL values are  missing matches
                // In such cases, pick the non-NULL entries
                const bool isNull1 = oldType.isNull();
                const bool isNull2 = newType.isNull();
                if (isNull1) {
                    VIAssert(!isNull2);
                    result_writer.getStringRef().copy(newType);
                    result_writer.next();
                    continue;
                } else if (isNull2) {
                    VIAssert(!isNull1);
                    result_writer.getStringRef().copy(oldType);
                    result_writer.next();
                    continue;
                }
            }

            // This is a key match. Pick the best data type
            const vint &oldFreq = arg_reader.getIntRef(1);
            const vint &newFreq = arg_reader.getIntRef(3);
            const vint &newErrCount = arg_reader.getIntRef(4);
            result_writer.getStringRef().copy(getBestType(oldType, oldFreq,
                                                          newType, newFreq, newErrCount));
            result_writer.next();
        } while (arg_reader.next());
    } catch(std::exception& e) {
        // Standard exception. Quit
        vt_report_error(0, "%s", e.what());
    }
}


void SetMapKeysFactory::getReturnType(ServerInterface &srvInterface,
                                      const SizedColumnTypes &argTypes,
                                      SizedColumnTypes &returnTypes) {
    // Check callers gave us string arguments only
    const size_t numKeys = argTypes.getColumnCount();
    if (!numKeys) {
        vt_report_error(0, "Function expects at least one argument, but none provided");
    }
    // Set output length as the largest possible length
    // based on the input (string) arguments
    size_t argLen = 0;
    for (size_t i = 0; i < numKeys; ++i) {
        const VerticaType& vt = argTypes.getColumnType(i);
        if (vt.getTypeOid() == UnknownOID) {
            // This is a string. Use typmod to get its length
            argLen += (vt.getTypeMod() - VARHDRSZ);
        } else if (vt.isStringType()) {
            argLen += vt.getStringLength();
        } else {
            vt_report_error(0, "Expected string type argument [%zu], but got [%s]",
                            (i + 1), vt.getPrettyPrintStr().c_str());
        }
    }

    outLen = sizeof(uint32) + // number of keys
        sizeof(uint32) * numKeys + // string length space
        argLen; // string space
    returnTypes.addLongVarbinary(outLen, "keys");
}

void SetMapKeys::processBlock(ServerInterface &srvInterface, BlockReader &arg_reader,
                              BlockWriter &result_writer) {
    const size_t numKeys = arg_reader.getNumCols();

    do {
        // Get pre-allocated string buffer to emit output
        VString& outKeys = result_writer.getStringRef();
        outKeys.setNull();

        char *outBuf = outKeys.data();
        uint32 bytes = 0; // Bytes written so far in output buffer

        // write out num_keys
        *reinterpret_cast<uint32*>(outBuf) = numKeys;
        outBuf += sizeof(uint32);
        bytes += sizeof(uint32);

        // Copy key lengths and strings in the output buffer
        // We should have the exact amount of space to copy all of them
        keyMap.clear(); // Reset key map
        for (size_t i = 0; i < numKeys; ++i) {
            const VString& currKey = arg_reader.getStringRef(i);
            if (currKey.isNull()) {
                // NULL keys are not allowed
                vt_report_error(0, "Key with NULL value found in argument [%zu]", (i + 1));
            }
            const vsize currKeyLen = currKey.length();

            // Check for duplicate keys
            {
                ImmutableStringPtr keyString(currKey.data(), currKeyLen);
                ColLkup::iterator it = keyMap.find(keyString);
                if (it != keyMap.end()) {
                    // Duplicate key found
                    std::string dupKey(currKey.data(),
                                       std::min(currKeyLen, (uint32) LOG_TRUNCATE_LENGTH));
                    vt_report_error(0, "Duplicate key found [%s] in argument [%zu]",
                                    dupKey.c_str(), (i + 1));
                } else {
                    keyMap[keyString] = 1;
                }
            }

            // We should not overrun the output buffer
            VIAssert((bytes + sizeof(uint32) + currKeyLen) <= outKeys.max_size);

            *reinterpret_cast<uint32*>(outBuf) = currKeyLen; // key length
            outBuf += sizeof(uint32);
            memcpy(outBuf, currKey.data(), currKeyLen); // key string
            outBuf += currKeyLen;
            bytes += sizeof(uint32) + currKeyLen;
        }

        EE::setLenSV(outKeys.sv, bytes);

        result_writer.next();
    } while (arg_reader.next());
}

uint32 MapPutFactory::estimateOutVMapLen(const KeyBuffer &keyBuffer,
                                         const SizedColumnTypes &argTypes) {
    // Compute length of all keys
    uint32 totalKeyLen = 0;
    {
        const char *ptr = reinterpret_cast<const char*>(&keyBuffer.keys[0]);
        for (uint32 i = 0; i < keyBuffer.len; i++) {
            const KeyValue *keyValue = reinterpret_cast<const KeyValue*>(ptr);
            totalKeyLen += keyValue->len;
            ptr += sizeof(KeyValue) + keyValue->len;
        }
    }

    // Compute maximum length of all values
    uint32 totalValueLen = 0;
    const size_t numArgs = argTypes.getColumnCount();
    VIAssert(keyBuffer.len == (numArgs - 1));
    for (size_t i = 1; i < numArgs; i++) {
        const VerticaType& vt = argTypes.getColumnType(i);
        if (vt.isStringType()) {
            totalValueLen += vt.getStringLength();
            continue;
        }

        // Non-string data types. OK to overestimate

        // For date/time types, maximum lengths are taken from Vertica's Doc
        // https://my.vertica.com/docs/7.2.x/HTML/Content/Authoring/SQLReferenceManual/DataTypes/Date-Time/DateTimeDataTypes.htm
        // We convert date/time to strings without formatting to generally get shorter strings
        const BaseDataOID typeOid = vt.getTypeOid();
        switch (typeOid) {
        case UnknownOID: {
            // This is a string. Use typmod to get its length
            totalValueLen += (vt.getTypeMod() - VARHDRSZ);
            break;
        }
        case NumericOID: {
            totalValueLen += vt.getNumericPrecision() + 1 /*decimal separator*/;
            break;
        }
        case BoolOID: {
            totalValueLen++;
            break;
        }
        case Int8OID:
        case Float8OID:{
            totalValueLen += MapPut::MIN_VFLOAT_LENGTH;
            break;
        }
            // 
        case DateOID: {
            totalValueLen += strlen("YYYY-MM-DD");
            break;
        }
        case IntervalOID: {
            totalValueLen += strlen("+-106751991 days  04:00:54.775807");
            break;
        }
        case TimestampOID: {
            totalValueLen += strlen("294277-01-09 04:00:54:775806 AD");
            break;
        }
        case TimestampTzOID: {
            // Time zone names can be 
            totalValueLen += strlen("294277-01-09 04:00:54:775806 AD UTC");
            break;
        }
        case TimeOID: {
            totalValueLen += strlen("23:59:60.999999");
            break;
        }
        case TimeTzOID: {
            totalValueLen += strlen("23:59:59.999999-14");
            break;
        }
        default: {
            // Other types, arbitrarily size it to something reasonable
            totalValueLen += 20;
        }
        }
    }
    // Assume no keys are overwritten on the input VMap
    return (argTypes.getColumnType(0).getStringLength() + // VMap argument
            // New VMap
            MAP_BLOCK_HEADER_SIZE + // already counted but OK to overestimate a little bit
            sizeof(uint32) + // len of value portion
            sizeof(uint32) + // number of values
            (sizeof(uint32) * (numArgs - 1)) + // value offsets
            totalValueLen + // actual values
            sizeof(uint32) + // number of keys
            (sizeof(uint32) * (numArgs - 1)) + // key offsets
            totalKeyLen // actual keys
        );
}

void MapPutFactory::getReturnType(ServerInterface &srvInterface,
                                  const SizedColumnTypes &argTypes,
                                  SizedColumnTypes &returnTypes) {
    // Validate arguments the user gave us
    const size_t numArgs = argTypes.getColumnCount();
    if (numArgs < 2) {
        vt_report_error(0, "Function expects at least two arguments, "
                        "but got [%zu]", numArgs);
    }

    // First argument must be a VMap (LongVarbinary)
    const VerticaType& vt = argTypes.getColumnType(0);
    if (vt.getTypeOid() != LongVarbinaryOID) {
        vt_report_error(0, "Function expects a Long Varbinary type (VMap) in the first "
                        "argument, but got [%s]", vt.getPrettyPrintStr().c_str());
    }

    // Check we got the same number of keys and values
    ParamReader params = srvInterface.getParamReader();
    const VString &keys = params.getStringRef(KEYS_PARAM_NAME);
    VIAssert(keys.length() > sizeof(KeyBuffer));
    const KeyBuffer &keyBuffer = *reinterpret_cast<const KeyBuffer*>(keys.data());
    if (keyBuffer.len != (numArgs - 1)) {
        vt_report_error(0, "Number of keys [%d] and values [%d] must match",
                        keyBuffer.len, (numArgs - 1));
    }

    outVMapLen = vt.getStringLength() + estimateOutVMapLen(keyBuffer, argTypes);
    returnTypes.addLongVarbinary(outVMapLen, "vmap");
}

void MapPut::setup(ServerInterface &srvInterface, const SizedColumnTypes &argTypes) {
    // Get a copy of keys parameter so we don't need to get one every time we process blocks
    ParamReader params = srvInterface.getParamReader();
    const VString &keys = params.getStringRef(MapPutFactory::KEYS_PARAM_NAME);
    keysParamLen = keys.length();
    keysParam = vt_allocArray(srvInterface.allocator, char, keysParamLen);
    memcpy(keysParam, keys.data(), keysParamLen);

    // Sanity check on the number of keys and values we were given
    const MapPutFactory::KeyBuffer &keyBuffer = *reinterpret_cast<const MapPutFactory::KeyBuffer*>(keysParam);
    // We better get at least one value. First arg is VMap
    VIAssert(argTypes.getColumnCount() > 1);
    if ((argTypes.getColumnCount() - 1) != keyBuffer.len) {
        vt_report_error(0, "Number of keys [%d] and values [%d] must match",
                        keyBuffer.len, argTypes.getColumnCount());
    }

    // Fill up map of keys
    MapPutFactory::KeyBufferReader keyBufReader(keyBuffer);
    do {
        const MapPutFactory::KeyValue &kv = keyBufReader.getKeyValueRef();
        keyMap[ImmutableStringPtr(kv.key, kv.len)] = 1;
    } while (keyBufReader.next());

    // Pre-allocate a buffer for new key/value pairs
    mapBuf = vt_allocArray(srvInterface.allocator, char, mapBufLen);
    // Pre-allocate a buffer for numeric-to-string conversions
    conversionBuf = vt_allocArray(srvInterface.allocator, char, MAX_CONVBUF_LENGTH);
}

void MapPut::processBlock(ServerInterface &srvInterface, BlockReader &arg_reader,
                          BlockWriter &result_writer) {
    const SizedColumnTypes &argTypes = arg_reader.getTypeMetaData();
    const MapPutFactory::KeyBuffer &keyBuffer = *reinterpret_cast<const MapPutFactory::KeyBuffer*>(keysParam);
 
    try {
        do {
            const VString &inVMap = arg_reader.getStringRef(0); // first arg is VMap
            VString &outVMap = result_writer.getStringRef();
            outVMap.setNull();

            // Output NULL on NULL or invalid VMap
            if (inVMap.isNull() || !VMapBlockReader::is_valid_map(srvInterface, inVMap, false)) {
                result_writer.next();
                continue;
            }

            VMapPairWriter vmWriter(mapBuf, mapBufLen);
            uint32 valIndex = 1; // values start from the second argument
            MapPutFactory::KeyBufferReader keyBufReader(keyBuffer);

            // Write new key/value pairs in the auxiliary VMap buffer
            do {
                const MapPutFactory::KeyValue &keyValue = keyBufReader.getKeyValueRef();
                const VerticaType &vt = argTypes.getColumnType(valIndex);
                const BaseDataOID typeOid = vt.getTypeOid();

                if (vt.isStringType()) {
                    const VString &value = arg_reader.getStringRef(valIndex);
                    const bool isNull = value.isNull();
                    int32 valueLen = 0;
                    if (!isNull) {
                        valueLen = value.length();
                    }
                    if (!vmWriter.append(srvInterface,
                                         keyValue.key, keyValue.len,
                                         isNull,
                                         value.data(), valueLen)) {
                        // Insufficient space to write key/value pair
                        vt_report_error(0, "Output VMap too big");
                    }
                    continue;
                }

                // Value is not of string type
                // We need to convert its internal representation to a string value
                *conversionBuf = '\0';
                switch(typeOid) {
                case BoolOID: {
                    const vbool &a = arg_reader.getBoolRef(valIndex);
                    char buf = '\0';
                    int32 n = 0;
                    if (a != vbool_null) {
                        buf = (a == vbool_true)? 't' : 'f';
                        n = 1;
                    }
                    if (!vmWriter.append(srvInterface, keyValue.key, keyValue.len,
                                         (a == vbool_null), &buf, n)) {
                        // Insufficient space to write key/value pair
                        vt_report_error(0, "Output VMap too big");
                    }
                    break;
                }

                case Int8OID: {
                    const vint &a = arg_reader.getIntRef(valIndex);
                    int32 n = vintToChar(a, conversionBuf, MAX_CONVBUF_LENGTH);
                    if (!vmWriter.append(srvInterface, keyValue.key, keyValue.len,
                                         (a == vint_null), conversionBuf, n)) {
                        // Insufficient space to write key/value pair
                        vt_report_error(0, "Output VMap too big");
                    }
                    break;
                }
                case Float8OID: {
                    const vfloat &a = arg_reader.getFloatRef(valIndex);
                    int32 n = vfloatToChar(a, conversionBuf, MAX_CONVBUF_LENGTH);
                    if (!vmWriter.append(srvInterface, keyValue.key, keyValue.len,
                                         vfloatIsNull(&a), conversionBuf, n)) {
                        // Insufficient space to write key/value pair
                        vt_report_error(0, "Output VMap too big");
                    }
                    break;
                }
                case NumericOID: {
                    const VNumeric &a = arg_reader.getNumericRef(valIndex);
                    const bool isNull = a.isNull();
                    int32 n = 0;
                    if (!isNull) {
                        a.toString(conversionBuf, MAX_CONVBUF_LENGTH);
                        trimTrailingZeros(conversionBuf);
                        n = strlen(conversionBuf);
                    }
                    if (!vmWriter.append(srvInterface, keyValue.key, keyValue.len,
                                         isNull, conversionBuf, n)) {
                        // Insufficient space to write key/value pair
                        vt_report_error(0, "Output VMap too big");
                    }
                    break;
                }
                case DateOID: {
                    const DateADT &a = arg_reader.getDateRef(valIndex);
                    int32 n = dateToChar(a, conversionBuf, MAX_CONVBUF_LENGTH, USE_ISO_DATES, false);
                    if (!vmWriter.append(srvInterface, keyValue.key, keyValue.len,
                                         (a == vint_null), conversionBuf, n)) {
                        // Insufficient space to write key/value pair
                        vt_report_error(0, "Output VMap too big");
                    }
                    break;
                }
                case TimeOID: {
                    const TimeADT &a = arg_reader.getTimeRef(valIndex);
                    int32 n = timeToChar(a, conversionBuf, MAX_CONVBUF_LENGTH, false);
                    if (!vmWriter.append(srvInterface, keyValue.key, keyValue.len,
                                         (a == vint_null), conversionBuf, n)) {
                        // Insufficient space to write key/value pair
                        vt_report_error(0, "Output VMap too big");
                    }
                    break;
                }
                case TimeTzOID: {
                    const TimeTzADT &a = arg_reader.getTimeTzRef(valIndex);
                    int32 n = timetzToChar(a, conversionBuf, MAX_CONVBUF_LENGTH, false);
                    if (!vmWriter.append(srvInterface, keyValue.key, keyValue.len,
                                         (a == vint_null), conversionBuf, n)) {
                        // Insufficient space to write key/value pair
                        vt_report_error(0, "Output VMap too big");
                    }
                    break;
                }
                case TimestampOID: {
                    const Timestamp &a = arg_reader.getTimestampRef(valIndex);
                    int32 n = timestampToChar(a, conversionBuf, MAX_CONVBUF_LENGTH, USE_ISO_DATES, false);
                    if (!vmWriter.append(srvInterface, keyValue.key, keyValue.len,
                                         (a == vint_null), conversionBuf, n)) {
                        // Insufficient space to write key/value pair
                        vt_report_error(0, "Output VMap too big");
                    }
                    break;
                }
                case TimestampTzOID: {
                    const TimestampTz &a = arg_reader.getTimestampTzRef(valIndex);
                    int32 n = timestamptzToChar(a, conversionBuf, MAX_CONVBUF_LENGTH, USE_ISO_DATES, false);
                    if (!vmWriter.append(srvInterface, keyValue.key, keyValue.len,
                                         (a == vint_null), conversionBuf, n)) {
                        // Insufficient space to write key/value pair
                        vt_report_error(0, "Output VMap too big");
                    }
                    break;
                }
                case IntervalOID: {
                    const Interval &a = arg_reader.getIntervalRef(valIndex);
                    int32 n = intervalToChar(a, vt.getTypeMod(), conversionBuf, MAX_CONVBUF_LENGTH, USE_ISO_DATES, false);
                    if (!vmWriter.append(srvInterface, keyValue.key, keyValue.len,
                                         (a == vint_null), conversionBuf, n)) {
                        // Insufficient space to write key/value pair
                        vt_report_error(0, "Output VMap too big");
                    }
                    break;
                }
                default: {
                    // Write NULL for UD types for now
                    if (!vmWriter.append(srvInterface, keyValue.key, keyValue.len,
                                         true, NULL, 0)) {
                        // Insufficient space to write key/value pair
                        vt_report_error(0, "Output VMap too big");
                    }
                }
                }
            } while (keyBufReader.next() && ++valIndex < arg_reader.getNumCols());

            // Write input VMap key/value pairs. Skip keys already in new VMap
            VMapBlockReader inVMapBlockReader(srvInterface, inVMap);
            for (uint32 i = 0; i < inVMapBlockReader.get_count(); i++) {
                const char *key = inVMapBlockReader.get_key(i);
                const uint32 keyLen = inVMapBlockReader.get_key_size(i);

                ImmutableStringPtr keyString(key, keyLen);
                KeyLkup::iterator it = keyMap.find(keyString);
                if (it != keyMap.end()) {
                    // Key already in new VMap. Skip this old key/value pair
                    continue;
                }

                const uint32 valSize = inVMapBlockReader.get_value_size(i);
                if (!vmWriter.append(srvInterface, key, keyLen,
                                     (valSize == 0),
                                     inVMapBlockReader.get_value(i), valSize)) {
                    // Insufficient space to write key/value pair
                    vt_report_error(0, "Output VMap too big");
                }
            }

            VMapPairReader outVMapPairReader(vmWriter);
            std::vector<VMapPair> &outVMapPairs = outVMapPairReader.get_pairs();

            // Write out the output VMap
            char *outData = outVMap.data(); // pre-allocated output buffer
            size_t outVMapSize = 0; // size of output VMap
            if (!VMapBlockWriter::convert_vmap(srvInterface, outVMapPairs, outData,
                                               outVMap.max_size, outVMapSize)) {
                vt_report_error(0, "Output VMap too big");
            }
            EE::setLenSV(outVMap.sv, outVMapSize);
            result_writer.next();
        } while (arg_reader.next());
    } catch(std::exception& e) {
        // Standard exception. Quit.
        vt_report_error(0, "Exception while processing block: [%s]", e.what());
    }
}

// Register each function factory
RegisterFactory(MapLookupFactory);
RegisterFactory(MapBinLookupFactory);
RegisterFactory(MapToStringFactory);
RegisterFactory(MapBinToStringFactory);
RegisterFactory(MapContainsKeyFactory);
RegisterFactory(MapBinContainsKeyFactory);
RegisterFactory(MapContainsValueFactory);
RegisterFactory(MapBinContainsValueFactory);
RegisterFactory(MapVersionFactory);
RegisterFactory(MapBinVersionFactory);
RegisterFactory(MapSizeFactory);
RegisterFactory(MapBinSizeFactory);
RegisterFactory(MapKeysFactory);
RegisterFactory(MapBinKeysFactory);
RegisterFactory(MapKeysInfoFactory);
RegisterFactory(MapBinKeysInfoFactory);
RegisterFactory(MapValuesFactory);
RegisterFactory(MapValuesOrFieldFactory);
RegisterFactory(MapBinValuesFactory);
RegisterFactory(MapItemsFactory);
RegisterFactory(MapBinItemsFactory);
RegisterFactory(EmptyMapFactory);
RegisterFactory(MapAggregateFactory);
RegisterFactory(MapLongAggregateFactory);
RegisterFactory(PickBestTypeFactory);
RegisterFactory(PickBestTypeResultFactory);
RegisterFactory(PickBestTypeMergeFactory);
RegisterFactory(MapPutFactory);
RegisterFactory(SetMapKeysFactory);

} /// END namespace flextable
