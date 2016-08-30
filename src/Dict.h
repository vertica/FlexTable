/**
 * Copyright (c) 2005 - 2015 Hewlett Packard Enterprise Development LP
 *
 * Creation Date: December 10, 2013
 */
/**
 * Description: UDx plugins for Flex Table SQL functions.
 */


#ifndef DICT_H_
#define DICT_H_

#include "Vertica.h"
#include "VerticaUDx.h"
#include "StringParsers.h"
#include "BasicsUDxShared.h"
#include "VMap.h"

using namespace Vertica;



namespace flextable
{


//////////////////////////// Single column map for 7.0


//// BEGIN maplookup()
class MapLookup : public ScalarFunction
{
public:
    MapLookup(uint32 outSize = 0) : case_sensitive(vbool_false), out_size(outSize) { }
    virtual void setup(ServerInterface &srvInterface, const SizedColumnTypes &argTypes);
    virtual void processBlock(ServerInterface &srvInterface,
                               BlockReader &arg_reader,
                               BlockWriter &result_writer);
private:
    vbool case_sensitive;
    uint32 out_size;
};

class MapLookupFactory : public ScalarFunctionFactory
{
public:
    MapLookupFactory();

    virtual void getPrototype(ServerInterface &srvInterface, ColumnTypes &argTypes, ColumnTypes &returnType);

    virtual void getReturnType(ServerInterface &srvInterface,
                               const SizedColumnTypes &argTypes,
                               SizedColumnTypes &returnTypes);
    virtual void getParameterType(ServerInterface &srvInterface,
                                  SizedColumnTypes &argTypes);

    virtual ScalarFunction * createScalarFunction(ServerInterface &srvInterface);

    virtual void getPerInstanceResources(ServerInterface &srvInterface, VResources &res) {
        // This function does not use file handles
        res.nFileHandles = 0;
    }

    static uint32 getOutSize(ServerInterface &srvInterface, const SizedColumnTypes &arg_types) {
        uint32 out_size = 0;
        ParamReader args = srvInterface.getParamReader();
        if (args.containsParameter("buffer_size")) {
            out_size = args.getIntRef("buffer_size");
        }
        if (out_size == 0) {
            out_size = arg_types.getColumnType(0).getStringLength();   // NB: This allows for the whole map to be the value, so it will never be quite this big, given indexing structures and key name storage
        }
        return out_size;
    }
};

class MapBinLookupFactory : public MapLookupFactory
{
public:
    MapBinLookupFactory();

    virtual void getPrototype(ServerInterface &srvInterface, ColumnTypes &argTypes, ColumnTypes &returnType);
};
//// END maplookup()


//// BEGIN maptostring()
class MapToString : public ScalarFunction
{
public:
    MapToString() : canonical_json(vbool_true) {}
    virtual void setup(ServerInterface &srvInterface, const SizedColumnTypes &argTypes);
    virtual void processBlock(ServerInterface &srvInterface,
                               BlockReader &arg_reader,
                               BlockWriter &result_writer);

private:
    vbool canonical_json;
};

class MapToStringFactory : public ScalarFunctionFactory
{
public:
    MapToStringFactory();

    virtual void getPrototype(ServerInterface &srvInterface, ColumnTypes &argTypes, ColumnTypes &returnType);

    virtual void getReturnType(ServerInterface &srvInterface,
                               const SizedColumnTypes &argTypes,
                               SizedColumnTypes &returnTypes);
    virtual void getParameterType(ServerInterface &srvInterface,
                                  SizedColumnTypes &argTypes);

    virtual ScalarFunction * createScalarFunction(ServerInterface &srvInterface);

    virtual void getPerInstanceResources(ServerInterface &srvInterface, VResources &res) {
        // This function does not use file handles
        res.nFileHandles = 0;
    }
};

class MapBinToStringFactory : public MapToStringFactory
{
public:
    MapBinToStringFactory();

    virtual void getPrototype(ServerInterface &srvInterface, ColumnTypes &argTypes, ColumnTypes &returnType);
};
//// END maptostring()


//// BEGIN mapcontainskey()
class MapContainsKey : public ScalarFunction
{
public:
    virtual void processBlock(ServerInterface &srvInterface,
                               BlockReader &arg_reader,
                               BlockWriter &result_writer);
};

class MapContainsKeyFactory : public ScalarFunctionFactory
{
public:
    MapContainsKeyFactory();

    virtual void getPrototype(ServerInterface &srvInterface, ColumnTypes &argTypes, ColumnTypes &returnType);

    virtual void getReturnType(ServerInterface &srvInterface,
                               const SizedColumnTypes &argTypes,
                               SizedColumnTypes &returnTypes);

    virtual ScalarFunction * createScalarFunction(ServerInterface &srvInterface);

    virtual void getPerInstanceResources(ServerInterface &srvInterface, VResources &res) {
        // This function does not use file handles
        res.nFileHandles = 0;
    }
};

class MapBinContainsKeyFactory : public MapContainsKeyFactory
{
public:
    MapBinContainsKeyFactory();

    virtual void getPrototype(ServerInterface &srvInterface, ColumnTypes &argTypes, ColumnTypes &returnType);
};
//// END mapcontainskey()


//// BEGIN mapcontainsvalue()
class MapContainsValue : public ScalarFunction
{
public:
    virtual void processBlock(ServerInterface &srvInterface,
                               BlockReader &arg_reader,
                               BlockWriter &result_writer);
};

class MapContainsValueFactory : public ScalarFunctionFactory
{
public:
    MapContainsValueFactory();

    virtual void getPrototype(ServerInterface &srvInterface, ColumnTypes &argTypes, ColumnTypes &returnType);

    virtual void getReturnType(ServerInterface &srvInterface,
                               const SizedColumnTypes &argTypes,
                               SizedColumnTypes &returnTypes);

    virtual ScalarFunction * createScalarFunction(ServerInterface &srvInterface);

    virtual void getPerInstanceResources(ServerInterface &srvInterface, VResources &res) {
        // This function does not use file handles
        res.nFileHandles = 0;
    }
};

class MapBinContainsValueFactory : public MapContainsValueFactory
{
public:
    MapBinContainsValueFactory();

    virtual void getPrototype(ServerInterface &srvInterface, ColumnTypes &argTypes, ColumnTypes &returnType);
};
//// END mapcontainsvalue()


//// BEGIN mapversion()
class MapVersion : public ScalarFunction
{
public:
    virtual void processBlock(ServerInterface &srvInterface,
                               BlockReader &arg_reader,
                               BlockWriter &result_writer);
};

class MapVersionFactory : public ScalarFunctionFactory
{
public:
    MapVersionFactory();

    virtual void getPrototype(ServerInterface &srvInterface, ColumnTypes &argTypes, ColumnTypes &returnType);

    virtual void getReturnType(ServerInterface &srvInterface,
                               const SizedColumnTypes &argTypes,
                               SizedColumnTypes &returnTypes);

    virtual ScalarFunction * createScalarFunction(ServerInterface &srvInterface);

    virtual void getPerInstanceResources(ServerInterface &srvInterface, VResources &res) {
        // This function does not use file handles
        res.nFileHandles = 0;
    }
};

class MapBinVersionFactory : public MapVersionFactory
{
public:
    MapBinVersionFactory();

    virtual void getPrototype(ServerInterface &srvInterface, ColumnTypes &argTypes, ColumnTypes &returnType);
};
//// END mapversion()


//// BEGIN mapsize()
class MapSize : public ScalarFunction
{
public:
    virtual void processBlock(ServerInterface &srvInterface,
                               BlockReader &arg_reader,
                               BlockWriter &result_writer);
};

class MapSizeFactory : public ScalarFunctionFactory
{
public:
    MapSizeFactory();

    virtual void getPrototype(ServerInterface &srvInterface, ColumnTypes &argTypes, ColumnTypes &returnType);

    virtual void getReturnType(ServerInterface &srvInterface,
                               const SizedColumnTypes &argTypes,
                               SizedColumnTypes &returnTypes);

    virtual ScalarFunction * createScalarFunction(ServerInterface &srvInterface);

    virtual void getPerInstanceResources(ServerInterface &srvInterface, VResources &res) {
        // This function does not use file handles
        res.nFileHandles = 0;
    }
};

class MapBinSizeFactory : public MapSizeFactory
{
public:
    MapBinSizeFactory();

    virtual void getPrototype(ServerInterface &srvInterface, ColumnTypes &argTypes, ColumnTypes &returnType);
};
//// END mapsize()


//// BEGIN mapkeys()
class MapKeys : public TransformFunction
{
    virtual void processPartition(ServerInterface &srvInterface,
                                  PartitionReader &arg_reader,
                                  PartitionWriter &result_writer);
};

class MapKeysFactory : public TransformFunctionFactory
{
    virtual void getPrototype(ServerInterface &srvInterface, ColumnTypes &argTypes, ColumnTypes &returnType);

    virtual void getReturnType(ServerInterface &srvInterface,
                               const SizedColumnTypes &argTypes,
                               SizedColumnTypes &returnTypes);

    virtual TransformFunction * createTransformFunction(ServerInterface &srvInterface);
};

class MapBinKeysFactory : public MapKeysFactory
{
    virtual void getPrototype(ServerInterface &srvInterface, ColumnTypes &argTypes, ColumnTypes &returnType);
};
//// END mapkeys()


//// BEGIN mapkeysinfo()
class MapKeysInfo : public TransformFunction
{
    virtual void processPartition(ServerInterface &srvInterface,
                                  PartitionReader &arg_reader,
                                  PartitionWriter &result_writer);
};

class MapKeysInfoFactory : public TransformFunctionFactory
{
    virtual void getPrototype(ServerInterface &srvInterface, ColumnTypes &argTypes, ColumnTypes &returnType);

    virtual void getReturnType(ServerInterface &srvInterface,
                               const SizedColumnTypes &argTypes,
                               SizedColumnTypes &returnTypes);

    virtual TransformFunction * createTransformFunction(ServerInterface &srvInterface);
};

class MapBinKeysInfoFactory : public MapKeysInfoFactory
{
    virtual void getPrototype(ServerInterface &srvInterface, ColumnTypes &argTypes, ColumnTypes &returnType);
};
//// END mapkeysinfo()


//// BEGIN mapvalues()
class MapValues : public TransformFunction
{
    virtual void processPartition(ServerInterface &srvInterface,
                                  PartitionReader &arg_reader,
                                  PartitionWriter &result_writer);
};

class MapValuesFactory : public TransformFunctionFactory
{
    virtual void getPrototype(ServerInterface &srvInterface, ColumnTypes &argTypes, ColumnTypes &returnType);

    virtual void getReturnType(ServerInterface &srvInterface,
                               const SizedColumnTypes &argTypes,
                               SizedColumnTypes &returnTypes);

    virtual TransformFunction * createTransformFunction(ServerInterface &srvInterface);
};

class MapBinValuesFactory : public MapValuesFactory
{
    virtual void getPrototype(ServerInterface &srvInterface, ColumnTypes &argTypes, ColumnTypes &returnType);
};
//// END mapvalues()

//// BEGIN mapvaluesorfield()
class MapValuesOrField : public TransformFunction
{
    virtual void processPartition(ServerInterface &srvInterface,
                                  PartitionReader &arg_reader,
                                  PartitionWriter &result_writer);
};

class MapValuesOrFieldFactory : public TransformFunctionFactory
{
    virtual void getPrototype(ServerInterface &srvInterface, ColumnTypes &argTypes, ColumnTypes &returnType);

    virtual void getReturnType(ServerInterface &srvInterface,
                               const SizedColumnTypes &argTypes,
                               SizedColumnTypes &returnTypes);

    virtual TransformFunction * createTransformFunction(ServerInterface &srvInterface);
};
//// END mapvaluesorfield()


//// BEGIN mapitems()

/*
 * Simple implementation of the Flexible Tables "MapItemS()" function.
 *
 * This transform function looks at its input and if it's a VMap, it iterates
 *  over all of the key/value pairs within the map and outputs each pair in
 *  its own row under the column headings "keys" and "values".
 */
class MapItems : public TransformFunction
{
    virtual void setup(ServerInterface &srvInterface, const SizedColumnTypes &argTypes);
    virtual void processPartition(ServerInterface &srvInterface,
                                  PartitionReader &arg_reader,
                                  PartitionWriter &result_writer);
private:
    /*
     * The only non-TransformFunction member.
     *
     * Data member to store the passed arguments so we don't have to look them
     *  up again for each partition.
     */
    std::vector<size_t> arg_cols;
};

class MapItemsFactory : public TransformFunctionFactory
{
    virtual void getPrototype(ServerInterface &srvInterface, ColumnTypes &argTypes, ColumnTypes &returnType);

    virtual void getReturnType(ServerInterface &srvInterface,
                               const SizedColumnTypes &argTypes,
                               SizedColumnTypes &returnTypes);

    virtual TransformFunction * createTransformFunction(ServerInterface &srvInterface);
};

class MapBinItemsFactory : public MapItemsFactory
{
    virtual void getPrototype(ServerInterface &srvInterface, ColumnTypes &argTypes, ColumnTypes &returnType);
};
//// END mapitems()


//// BEGIN emptymap()
class EmptyMap : public ScalarFunction
{
private:
    char *empty_map_data_buf; // Empty VMap

public:
    EmptyMap() : empty_map_data_buf(NULL) { }

/// Constant to store empty map data. Must be updated along with new map formats
///   to be as large as the largest possible empty map.
    static const ssize_t empty_map_data_len = 16;

    void setup(ServerInterface &srvInterface, const SizedColumnTypes &argTypes) {
        // Initialize empty VMap buffer
        std::vector<VMapPair> vpairs; // Empty set of map pairs
        empty_map_data_buf = (char *) srvInterface.allocator->alloc(empty_map_data_len);
        size_t total_offset = 0;
        VMapBlockWriter::convert_vmap(srvInterface, vpairs, empty_map_data_buf,
                                      empty_map_data_len, total_offset);
    }

    virtual void processBlock(ServerInterface &srvInterface,
                               BlockReader &arg_reader,
                               BlockWriter &result_writer);
};

class EmptyMapFactory : public ScalarFunctionFactory
{
public:
    EmptyMapFactory();

    virtual void getPrototype(ServerInterface &srvInterface, ColumnTypes &argTypes, ColumnTypes &returnType);

    virtual void getReturnType(ServerInterface &srvInterface,
                               const SizedColumnTypes &argTypes,
                               SizedColumnTypes &returnTypes);

    virtual ScalarFunction * createScalarFunction(ServerInterface &srvInterface);

    virtual void getPerInstanceResources(ServerInterface &srvInterface, VResources &res) {
        // This function does not use file handles
        res.nFileHandles = 0;
    }
};
//// END emptymap()


//// BEGIN mapaggregate()
class MapAggregate : public TransformFunction
{
    virtual void processPartition(ServerInterface &srvInterface,
                                  PartitionReader &arg_reader,
                                  PartitionWriter &result_writer);
};

class MapAggregateFactory : public TransformFunctionFactory
{
    virtual void getPrototype(ServerInterface &srvInterface, ColumnTypes &argTypes, ColumnTypes &returnType);

    virtual void getReturnType(ServerInterface &srvInterface,
                               const SizedColumnTypes &argTypes,
                               SizedColumnTypes &returnTypes);

    virtual TransformFunction * createTransformFunction(ServerInterface &srvInterface);
};

class MapLongAggregateFactory : public MapAggregateFactory
{
    virtual void getPrototype(ServerInterface &srvInterface, ColumnTypes &argTypes, ColumnTypes &returnType);
};
//// END mapaggregate()


// PickBestType UD aggregate function output is a DataTypeCounters instance
// in an opaque buffer
#define TYPE_GUESSING_RESULT_LEN sizeof(PickBestType::DataTypeCounters)

// Maximum number of parse tries before we decide to go with string type
#define TYPE_GUESSING_MAX_PARSE_TRIES 10000

// Maximum number of parse errors is 20% of the total number of processed values
#define TYPE_GUESSING_ERROR_PCT_THRESHOLD .20

// Total number of data type we try to guess
// 0: Int8
// 1: Timestamp
// 2: TimestampTz
// 3: Bool
// 4: Time
// 5: TimeTz
// 6: Float8
// 7: Date
// 8: Interval
// 9: Varchar
// 10: Numeric
// 11: VMap
#define TYPE_GUESSING_NUM_DATA_TYPES 12

/**
 * Pick the best data type given a column of string values
 * The output includes the best data type Oid and its parameters (such as length, precision/scale)
 * Since a UD aggregate can only output a single value,
 * We output an opaque buffer containing an instance of PickBestType::DataTypeCounters.
 */
class PickBestType: public AggregateFunction {
    uint64 errmax; // maximum parse tries
    bool enforceErrorThreshold; // default to string if number of errors exceeds threshold?
    char* inValue; // A copy of the input value we receive in aggregate()
                   // We want to modify the input copy during when parsing it to specific types

    VerticaType *verticaTypes; // All the Vertica data types we will try in type guessing

    // A mapping between DataTypeIndex and Type Oids
    // Mainly used to get type Oids for DataTypeCounters::counts items
    Oid DataTypeIndexToTypeOid[TYPE_GUESSING_NUM_DATA_TYPES];

    /**
     * Parse a character string into a Numeric(18, s)
     *
     * We choose precision=18 following the Vertica's recommendation.
     * Numeric(18, s) has similar performance to int.
     *
     * @param str input string
     * @param len string length
     * @param p precision
     * @param s scale
     *
     * @return TRUE if value parses well into a Numeric type, and (p,s) are within (18, scale)
     */
    bool parseNumeric(const char *str, int len, int &p, int &s) {
        Vertica::int64 words = 0;
        Vertica::int64 *pout = &words;
        int nwords = sizeof(words) / sizeof(Vertica::int64);
        // Ignore leading '$' sign
        const bool success = (*str == '$')? Basics::BigInt::charToNumeric(str+1, len-1, false, pout, nwords, p, s, false) : Basics::BigInt::charToNumeric(str, len, false, pout, nwords, p, s, false);
        if (success) {
            // Check if precision and scale are within 15 digits
            if ((p + s) <= 15) {
                return true;
            }
        }
        return false;
    }

    /**
     * Check if the input string terminates with characters that may represent a timezone
     * The goal is to quickly determine if the string could possibly include a timezone
     * It is a light test. False positives are likely to happen
     */
    bool mayHaveTimezone(const char* s, uint32 len) const;

    // Factors to enlarge numeric precision and scale
    // in provision of future values
    static const uint16 NUMERIC_PREC_INCREMENT_FACTOR;
    static const uint16 NUMERIC_SCALE_INCREMENT_FACTOR;

public:
    // Indexes to access DataTypeCounters::counts
    enum DataTypeIndex{INT8 = 0,
                       TIMESTAMP = 1,
                       TIMESTAMP_TZ = 2,
                       BOOL = 3,
                       TIME = 4,
                       TIME_TZ = 5,
                       FLOAT8 = 6,
                       DATE = 7,
                       INTERVAL = 8,
                       VARCHAR = 9,
                       NUMERIC = 10,
                       VMAP = 11,
                       LAST_UNUSED // must be last for routines that iterate through the enums
    };

    // Data structure to keep data type counts and stats of values seen during aggregation
    struct DataTypeCounters {
        DataTypeCounters() : typeOid(VUnspecOID), len(0), prec(0), scale(0),
                             errcnt(0), rowcnt(0), nullcnt(0) {
            memset(counts, 0, sizeof(counts));
        }

        Oid typeOid; // best data type so far
        uint32 len; // largest character/binary string length
        uint16 prec; // largest Numeric precision
        uint16 scale; // largest Numeric scale
        uint64 errcnt; // number of parse errors
        uint64 rowcnt; // total number of rows processed, including NULL values
        uint64 nullcnt; // total number of NULL and empty string values
        uint64 counts[TYPE_GUESSING_NUM_DATA_TYPES]; // counters. See NUM_DATA_TYPES comment for indexes

        // Add counts and set len, prec, scale to the larger values
        // TypeOid is left unchanged
        DataTypeCounters &operator+=(const DataTypeCounters &rhs) {
            len = std::max(len, rhs.len);
            prec = std::max(prec, rhs.prec);
            scale = std::max(scale, rhs.scale);
            errcnt += rhs.errcnt;
            rowcnt += rhs.rowcnt;
            nullcnt += rhs.nullcnt;
            for (int i = INT8; i < LAST_UNUSED; i++) {
                counts[i] += rhs.counts[i];
            }
            return *this;
        }

        std::string toString() const {
            std::ostringstream oss;
            oss << "[len=" << len
                << ", prec=" << prec
                << ", scale=" << scale
                << ", errcnt=" << errcnt
                << ", rowcnt=" << rowcnt
                << ", nullcnt=" << nullcnt
                << ", counts={" << counts[INT8];
            for (int i = INT8 + 1; i < LAST_UNUSED; i++) {
                oss << ", " << counts[i];
            }
            oss << "}]";
            return oss.str();
        }
    };

    PickBestType() : errmax(TYPE_GUESSING_MAX_PARSE_TRIES), inValue(NULL), verticaTypes(NULL) {
        DataTypeIndexToTypeOid[INT8] = Int8OID;
        DataTypeIndexToTypeOid[TIMESTAMP] = TimestampOID;
        DataTypeIndexToTypeOid[TIMESTAMP_TZ] = TimestampTzOID;
        DataTypeIndexToTypeOid[BOOL] = BoolOID;
        DataTypeIndexToTypeOid[TIME] = TimeOID;
        DataTypeIndexToTypeOid[TIME_TZ] = TimeTzOID;
        DataTypeIndexToTypeOid[FLOAT8] = Float8OID;
        DataTypeIndexToTypeOid[DATE] = DateOID;
        DataTypeIndexToTypeOid[INTERVAL] = IntervalOID;
        DataTypeIndexToTypeOid[VARCHAR] = VarcharOID;
        DataTypeIndexToTypeOid[NUMERIC] = NumericOID;
        DataTypeIndexToTypeOid[VMAP] = MapOID;
    }

    StringParsers sp;
    void initAggregate(ServerInterface &srvInterface, IntermediateAggs &aggs);
    void aggregate(ServerInterface &srvInterface, 
                   BlockReader &argReader, 
                   IntermediateAggs &aggs);
    void combine(ServerInterface &srvInterface, 
                 IntermediateAggs &aggs, 
                 MultipleIntermediateAggs &aggsOther);
    void terminate(ServerInterface &srvInterface, 
                   BlockWriter &resWriter, 
                   IntermediateAggs &aggs);
    void setup(ServerInterface &srvInterface, const SizedColumnTypes &argTypes);

    InlineAggregate()
};

class PickBestTypeFactory : public AggregateFunctionFactory {
    void getIntermediateTypes(ServerInterface &srvInterface, const SizedColumnTypes &inputTypes,
                              SizedColumnTypes &intermediateTypeMetaData) {
        intermediateTypeMetaData.addVarbinary(TYPE_GUESSING_RESULT_LEN);
    }

    void getPrototype(ServerInterface &srvfloaterface, ColumnTypes &argTypes,
                      ColumnTypes &returnType) {
        argTypes.addVarchar();
        returnType.addVarbinary();
    }

    void getParameterType(ServerInterface &srvInterface,
                          SizedColumnTypes &parameterTypes) {
        parameterTypes.addInt("_minimizeCallCount");
        SizedColumnTypes::Properties props(false /*visible*/, false /*required*/,
                                           false /*canBeNull*/, "Maximum number of parse tries");
        parameterTypes.addInt("ErrorMax", props);
        props.comment = "Default to string when the parse error count exceeds the threshold";
        parameterTypes.addBool("EnforceErrorThreshold", props);
    }
    
    void getReturnType(ServerInterface &srvfloaterface, 
                       const SizedColumnTypes &inputTypes, 
                       SizedColumnTypes &outputTypes) {
        outputTypes.addVarbinary(TYPE_GUESSING_RESULT_LEN);
    }

    AggregateFunction *createAggregateFunction(ServerInterface &srvfloaterface) {
        return vt_createFuncObject<PickBestType>(srvfloaterface.allocator);
    }
};

/**
 * Extract the result values output by the PickBestType UD aggregate function
 */
class PickBestTypeResult: public TransformFunction {
    void processPartition(ServerInterface &srvInterface,
                          PartitionReader &arg_reader,
                          PartitionWriter &result_writer) {
        do {
            const PickBestType::DataTypeCounters &dtc = *reinterpret_cast<const PickBestType::DataTypeCounters*>(arg_reader.getStringRef(0).data());
            result_writer.setInt(0, dtc.typeOid);
            result_writer.setInt(1, dtc.len);
            result_writer.setInt(2, dtc.prec);
            result_writer.setInt(3, dtc.scale);
            result_writer.setInt(4, dtc.errcnt);
            result_writer.setInt(5, dtc.rowcnt);
            result_writer.next();
        } while (arg_reader.next());
    }
};

class PickBestTypeResultFactory: public TransformFunctionFactory {
    void getPrototype(ServerInterface &srvInterface, ColumnTypes &argTypes,
                      ColumnTypes &returnType) {
        argTypes.addVarbinary(); // VMap
        returnType.addInt(); // type Oid
        returnType.addInt(); // len
        returnType.addInt(); // prec
        returnType.addInt(); // scale
        returnType.addInt(); // errcnt
        returnType.addInt(); // rowcnt
    }

    void getReturnType(ServerInterface &srvInterface, const SizedColumnTypes &argTypes,
                       SizedColumnTypes &returnTypes) {
        returnTypes.addInt("toid"); // type Oid
        returnTypes.addInt("len"); // len
        returnTypes.addInt("prec"); // prec
        returnTypes.addInt("scale"); // scale
        returnTypes.addInt("errcnt"); // errcnt
        returnTypes.addInt("rowcnt"); // rowcnt
    }

    TransformFunction *createTransformFunction(ServerInterface &srvInterface) {
        return vt_createFuncObject<PickBestTypeResult>(srvInterface.allocator);
    }
};

/**
 * Pick the better type between and old and new type guesses
 * The UDx keeps the more general type between the two of them,
 * and may enlarge the type parameters to avoid loosing information
 *
 * Example: between INT (old) and NUMERIC(10,4) (new) types,
 *          the result is NUMERIC(18,4)
 *
 * The algorithm may pick a less general type if they cannot be coerced to each other
 * and the error percentage is still within the threshold
 * Example: INT 1000 rows (old guess) and VARCHAR 20 rows (new guess)
 *          The result will be INT with 2% error (< TYPE_GUESSING_ERROR_PCT_THRESHOLD)
 *
 * In some cases, none of the two types can be generalized to any type better than string,
 * or the error introduced by keeping one of the type is over threshold
 * In those cases, the algorithm defaults to a string type
 * Example: DATE (1000 rows) and BOOL (700 rows) results in VARCHAR(20)
 */
class PickBestTypeMerge: public ScalarFunction {
public:
    struct DataType {
        DataType() : name(), oid(VUnspecOID), len(0), prec(0), scale(0) { }
        DataType(Oid oid) : name(), oid(oid), len(0), prec(0), scale(0) { }
        DataType(Oid oid, uint32 len, uint16 prec, uint16 scale) :
            name(), oid(oid), len(len), prec(prec), scale(scale) { }
        std::string name;
        Oid oid;
        uint32 len;
        uint16 prec;
        uint16 scale;

        // Check if this type represents a string type, including VUnspecOID type
        // which is technically a string, too.
        bool isStringType() const {
            return(oid == VarcharOID   || oid == CharOID ||
                   oid == VarbinaryOID || oid == BinaryOID ||
                   oid == LongVarcharOID || oid == LongVarbinaryOID ||
                   oid == VUnspecOID);
        }

        bool isLongStringType() const {
            return (oid == LongVarcharOID || oid == LongVarbinaryOID);
        }

        bool isBinaryStringType() const {
            return (oid == VarbinaryOID || oid == LongVarbinaryOID);
        }

        /**
         * Expand this data type parameters with another one's.
         * For Interval type, it just sets prec/scale for Interval Day to Second.
         */
        void expandType(const DataType &dt2) {
            if (oid == NumericOID) {
                prec = std::max(prec, dt2.prec);
                if (dt2.oid == Int8OID) {
                    // Ensure integers will fit in the numeric column
                    prec = std::max(prec, MIN_PRECISION_FOR_INT8);
                }
            } else if (oid == IntervalOID) {
                prec = MAX_SECONDS_PRECISION;
                scale = INTERVAL_DAY2SECOND;
            } else if (isStringType()) {
                len = std::max(len, dt2.len);
                if (len < MIN_STRING_LEN) {
                    len = MIN_STRING_LEN;
                }
            }
        }

        // Set this type parameters (Oid, length, precision, scale)
        // from a type in string representation
        void setTypeParameters(const VString &typeStr);

        /**
         * Get the type Oid from a string containing a data type
         * Not necessarily comprehensive. Just the types the UDagg pickbesttype can return
         */
        static Oid getTypeOid(std::string typeStr);

        /**
         * Check if this type can be parsed to another type without data loss
         * The check is based only on type Oids
         *
         * Callers are responsile to adjust typeMod to actually guarantee no data loss
         */
        bool canSafelyParseTo(const DataType &dt2);
    };

    // Minimum numeric precision to ensure you can store INT8 without data loss
    static const uint16 MIN_PRECISION_FOR_INT8;

    // Minimum string length
    static const uint16 MIN_STRING_LEN;

    // Maximum length of regular string types
    static const uint16 MAX_SHORT_STRING_LEN;

    PickBestTypeMerge() { }

    void processBlock(ServerInterface &srvInterface,
                      BlockReader &arg_reader,
                      BlockWriter &result_writer);

private:
    /**
     * Pick the beter type of the two input types, or resource to string
     * if no better type is possible to keep error rate under threshold.
     */
    std::string getBestType(const VString &oldType, const vint &oldFreq,
                            const VString &newType, const vint &newFreq,
                            const vint &newErrorCount);

    /**
     * Expand first data type parameters with second type's
     *
     * @return String representation of data type plus parameters
     */
    std::string expandTypeParameters(const DataType &dt1, const DataType &dt2);

    /**
     * Set the typeMod of a VerticaType with the parameters of a DataType
     */
    void setTypeMod(VerticaType &vt, const DataType &dt);

    /**
     * Combine parameters of two string data types, picking the largest length
     * It selects a binary string if either input is of binary string type
     *
     * @return String representation of data type plus parameters
     */
    std::string downgradeToString(const DataType &dt1, const DataType &dt2);
};

class PickBestTypeMergeFactory: public ScalarFunctionFactory {
public:
    PickBestTypeMergeFactory() {
        vol = IMMUTABLE;
        strict = RETURN_NULL_ON_NULL_INPUT;
    }

    void getPrototype(ServerInterface &srvInterface, ColumnTypes &argTypes,
                      ColumnTypes &returnType) {
        argTypes.addVarchar(); // old type guess
        argTypes.addInt(); // old frequency
        argTypes.addVarchar(); // new type guess
        argTypes.addInt(); // new frequency
        argTypes.addInt(); // new errcnt
        returnType.addVarchar(); // best type
    }

    void getReturnType(ServerInterface &srvInterface, const SizedColumnTypes &argTypes,
                       SizedColumnTypes &returnTypes) {
        returnTypes.addVarchar(64, "best_type");
    }

    ScalarFunction *createScalarFunction(ServerInterface &srvInterface) {
        return vt_createFuncObject<PickBestTypeMerge>(srvInterface.allocator);
    }
};

/**
 * Helper class to set VMap keys from key strings passed by users
 * It is meant to be used with Flex's MapPut() API rather than standalone.
 *
 * Example: Output a VMap with key/value pairs: {a=1, b=2, c=3}
 *   SELECT MapPut(EmptyMap(), 1, 2, 3 USING PARAMETERS keys=SetMapKeys('a', 'b', 'c'));
 *
 * @param key1[,key2,...] The function takes any number of string type arguments
 *
 * @throw The function throws on: non-string type arguments, duplicate keys
 *
 * It returns an opaque buffer with key strings and their lengths
 */
class SetMapKeys : public ScalarFunction {
private:
    ColLkup keyMap; // Map to check for key duplicates

public:
    SetMapKeys() : keyMap() { }

    void processBlock(ServerInterface &srvInterface, BlockReader &arg_reader,
                      BlockWriter &result_writer);
};

class SetMapKeysFactory : public ScalarFunctionFactory {
private:
    uint32 outLen;

public:
    SetMapKeysFactory() : outLen(0) {
        vol = IMMUTABLE;
    }

    void getPrototype(ServerInterface &srvInterface, ColumnTypes &argTypes,
                      ColumnTypes &returnType) {
        argTypes.addAny(); // Arbitraty number of key name strings
        // Opaque block with key strings and lengths
        // See getReturnType() function comments for a description of the output buffer
        returnType.addLongVarbinary();
    }

    /**
     * The function returns an opaque buffer with key lengths and key strings.
     *
     * The output buffer stores first the total number of keys, then keys follow
     *   uint32 num_keys
     *   KeyValue instances sequentially
     *
     * struct KeyValue {
     *    uint32 key_length;
     *    char key[];
     * };
     */
    void getReturnType(ServerInterface &srvInterface, const SizedColumnTypes &argTypes,
                       SizedColumnTypes &returnTypes);

    ScalarFunction *createScalarFunction(ServerInterface &srvInterface) {
        return vt_createFuncObject<SetMapKeys>(srvInterface.allocator);
    }

    void getPerInstanceResources(ServerInterface &srvInterface, VResources &res) {
        res.scratchMemory += outLen + outLen /*key map*/;
    }
};

/**
 * Put into an existing VMap vm a list of key/value pairs
 * Keys must be set using the auxiliary function SetMapKeys(), and can only be constant strings
 * If vm has already any of the input keys, their values will be replaced by the new ones
 */
class MapPut : public ScalarFunction {
public:
    // Length of conversionBuf
    // Sufficiently large to store the largest numeric value with precision=1024
    // String representations of the largest int (MIN_VINT_LENGTH) and
    // float (MIN_VFLOAT_LENGTH with default format) values are much smaller
    static const uint16 MAX_CONVBUF_LENGTH;

    // Minimum buffer lengths for int/float conversion to string
    static const uint8 MIN_VINT_LENGTH;
    static const uint8 MIN_VFLOAT_LENGTH;

    MapPut(uint32 mapBufLen) :
        keysParam(NULL), keysParamLen(0), mapBuf(NULL), mapBufLen(mapBufLen),
        conversionBuf(NULL), keyMap(KeyLkup()) { }

    void setup(ServerInterface &srvInterface, const SizedColumnTypes &argTypes);

    void processBlock(ServerInterface &srvInterface, BlockReader &arg_reader,
                      BlockWriter &result_writer);

private:
    char *keysParam; // Copy of KeyBuffer provided by "keys" parameter
    uint32 keysParamLen; // keys buffer size
    char *mapBuf; // auxiliaty VMap buffer to store new key/value pairs
    uint32 mapBufLen; // length of mapBuf. Should be as large as the output VMap
    char *conversionBuf; // pre-allocated buffer to convert bool, int, float, etc. values to strings
    typedef std::map<ImmutableStringPtr, size_t> KeyLkup;
    KeyLkup keyMap; // pointers to keys in keysParam buffer

    /**
     * Convert a Vertica int value to a null-terminated string
     *
     * @param res buffer to store the result
     *            Callers should give a buffer at least (MAX_VINT_LENGTH+1) bytes long
     * @param max_size res buffer length
     *
     * @return length of string not including the '\0' terminating character
     */
    inline int32 vintToChar(const vint &a, char *res, int32 max_size) {
        if (a == vint_null) {
            return 0;
        }
        VIAssert(max_size > MIN_VINT_LENGTH);
        int n = sprintf(res, "%lld", a);
        return n;
    }

    static void trimTrailingZeros(char *str) {
        int         len = strlen(str);

        /* chop off trailing zeros... but leave at least 1 fractional digit */
        while (*(str + len - 1) == '0' && *(str + len - 2) != '.') {
            len--;
            *(str + len) = '\0';
        }
    }

    /**
     * Convert a Vertica float value to a null-terminated string
     *
     * @param res buffer to store the result
     *            Callers should give a buffer at least (MAX_VFLOAT_LENGTH+1) bytes long
     * @param max_size res buffer length
     *
     * @return length of string not including the '\0' terminating character
     */
    inline int32 vfloatToChar(const vfloat &a, char *res, int32 max_size) {
        if (vfloatIsNull(&a)) {
            return 0;
        }
        VIAssert(max_size > MIN_VFLOAT_LENGTH);
        int32 n = 0;
        if (vfloatIsNaN(&a)) {
            strcpy(res, "NaN");
            n = strlen("NaN");
        } else {
            n = sprintf(res, "%.15g", a);
            trimTrailingZeros(res);
            char *p = strstr(res, "inf");
            if (p) {
                // inf | -inf
                strcpy(p, "Infinity");
            }
            n = strlen(res);
        }
        return n;
    }
};

class MapPutFactory : public ScalarFunctionFactory {
private:
    uint32 outVMapLen;

public:
    static const std::string KEYS_PARAM_NAME; // keys parameter name
    static const uint32 MAX_KEYS_PARAM_SIZE; // keys parameter name

    MapPutFactory() : outVMapLen(0) {
        vol = IMMUTABLE;
    }

    struct KeyValue {
        KeyValue() : len(0) { }
        uint32 len;
        char key[];
    };

    struct KeyBuffer {
        KeyBuffer() : len(0) { }
        uint32 len; // number of keys
        KeyValue keys[];
    };

    // Helper class to read keys off a KeyBuffer
    class KeyBufferReader {
    public:
        // Constructor
        // Callers must ensure that the KeyBuffer reference outlives this object
        KeyBufferReader(const KeyBuffer &kb) : keysPtr(NULL), index(0), kb(kb),
                                               emptyKeyValue(KeyValue()) {
            if (kb.len) {
                keysPtr = reinterpret_cast<const char *>(&kb.keys[0]);
                index = 1;
            }
        }

        bool next() {
            if (index < kb.len) {
                // Move keysPtr to the next KeyValue object
                const KeyValue *keyValue = reinterpret_cast<const KeyValue *>(keysPtr);
                keysPtr += sizeof(KeyValue) + keyValue->len;
                index++;
                return true;
            } else {
                return false;
            }
        }

        const KeyValue &getKeyValueRef() const {
            if (keysPtr) {
                return *reinterpret_cast<const KeyValue *>(keysPtr);
            } else {
                return emptyKeyValue;
            }
        }

    private:
        KeyBufferReader() : keysPtr(NULL), index(0), kb(KeyBuffer()),
                            emptyKeyValue(KeyValue()) { }
        const char *keysPtr;
        uint32 index; // number of KeyValue instances already read
        const KeyBuffer &kb;
        const KeyValue emptyKeyValue;
    };

    void getPrototype(ServerInterface &srvInterface, ColumnTypes &argTypes,
                      ColumnTypes &returnType) {
        argTypes.addAny();
        returnType.addLongVarbinary();
    }

    /**
     * Estimate the length of the output VMap MapPut will return
     *
     * Callers must ensure we have the same number of keys and values
     */
    uint32 estimateOutVMapLen(const KeyBuffer &keyBuffer, const SizedColumnTypes &argTypes);

    void getReturnType(ServerInterface &srvInterface, const SizedColumnTypes &argTypes,
                       SizedColumnTypes &returnTypes);

    void getParameterType(ServerInterface &srvInterface, SizedColumnTypes &argTypes) {
        SizedColumnTypes::Properties props(true /*visible*/, true /*required*/,
                                           false /*canBeNull*/, "VMap keys");
        argTypes.addLongVarbinary(MAX_KEYS_PARAM_SIZE, KEYS_PARAM_NAME, props);
    }

    ScalarFunction *createScalarFunction(ServerInterface &srvInterface) {
        return vt_createFuncObject<MapPut>(srvInterface.allocator, outVMapLen);
    }
};

} /// END namespace flextable

/// END define DICT_H_
#endif

