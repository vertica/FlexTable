/**
 * Copyright (c) 2005 - 2015 Hewlett Packard Enterprise Development LP
 *
 */
#ifndef NAVRO

#ifndef AVRO_H_
#define AVRO_H_

#include "VMap.h"

#include "Vertica.h"
#include "ContinuousUDParser.h"
#include "StringParsers.h"
#include "CRReader.hh"
#include "avro/Generic.hh"
#include "AvroDatumParsers.h"
#include "FlexParsersSupport.h"

namespace flextable {

/**
 * Parser for Flex Table tables with data files in Avro format.
 */
class FlexTableAvroParser: public ContinuousUDParser {
public:
    FlexTableAvroParser(
        std::vector<std::string> formatStrings = std::vector<std::string>(),
        bool rejectOnMaterializedTypeError = false,
        bool flatten_maps = false,
        bool flatten_arrays = true, bool flatten_records = false,  bool enforcelength = false);

    ~FlexTableAvroParser();
        
    //enum to set a return state from run() method
    enum CRState { ERROR_CRAETING_READER, 
                   ERROR_READING_FILE, 
                   ERROR_SYNC_MISMATCH, 
                   ERROR_SNAPPY_LENGTH, 
                   ERROR_SNAPPY_MEMORY, 
                   ERROR_SNAPPY_UNCOMPRESS,
                   ERROR_CRC32,
                   ERROR_CODEC,
                   NOT_FLATTEN_REAL_TABLE, 
                   OK };

private:

    enum CRState state;
    // Keep a copy of the information about each column.
    // Note that Vertica doesn't let us safely keep a reference to
    // the internal copy of this data structure that it shows us.
    // But keeping a copy is fine.
    SizedColumnTypes colInfo;

    size_t currentRecordSize;

    // Source of parsed avro data
    CRReader<avro::GenericDatum> * dataReader;
    avro::ValidSchema dataSchema;

    //stack to store avro names and use them as keys for vmap
    std::stack<std::string> baseName;

    /*
     * An instance of the class containing the methods that we're
     * using to parse strings to the various relevant data types
     * VFormattedStringParsers sp;
     */
    StringParsers sp;

    /*
     * Real column name lookup
     */
    ColLkup real_col_lookup;

    // 7.0/Crane map structure
    // The column number of the map data column (__raw__) and buffers to hold
    // the final map and intermediate pairs
    int32 map_col_index;
    char* map_data_buf;
    ssize_t map_data_buf_len;
    char* pair_buf;

    bool is_flextable;

    std::vector<std::string> formatStrings;
    std::vector<bool> parsedRealCols;

    bool reject_on_materialized_type_error;
    bool flatten_maps;
    bool flatten_arrays;
    bool flatten_records;
    bool enforcelength;

    // Auxiliary buffer to normalize keys and store null-terminated values
    BufferWithAllocator<char> aux_buf;

    unsigned int rowNum;
    int num_parsed_real_values;

    std::vector<std::string> real_column_names;
    std::string rejected_data;
    /**
     * Given a field in string form (a pointer to the first character and
     * a length), submit that field to Vertica.
     * `colNum` is the column number from the input file; how many fields
     * it is into the current record.
     *
     * Ordinarily, this method will probably want to do some parsing and
     * conversion of the input string.  For now, though, this example
     * only supports char and varchar columns, and only outputs strings,
     * so no parsing is necessary.
     */
    bool handle_field(size_t colNum, char* start, size_t len,
                      bool hasPadding = false);

    void rejectRecord(const std::string &reason);
    void constructRejectMessage(GenericDatum d);
    void constructRejectMessage_NameKey_(std::string recordType);

    //classes that take an avro complex field and parse them
    bool resolveRecord(VMapPairWriter& map_writer,GenericRecord record);
    bool resolveEnum(VMapPairWriter& map_writer,avro::GenericEnum avroEnum, bool flatten,
                     bool isArrayEnum);
    bool resolveArray(VMapPairWriter& map_writer,avro::GenericArray avroArray);
    bool resolveMap(VMapPairWriter& map_writer, avro::GenericMap avroMap);

    bool resolveRealFieldIntoTable(std::string columnName, std::string value);
    //convert_map received and also process materialized columns
    bool construct_map(VMapPairWriter& map_writer_nested, size_t& total_offset);
    bool construct_append_nested_map(VMapPairWriter& map_writer,GenericDatum d);

    void clearBaseNameStack();
    void handleErrorState(int status);

    void cleanup();
        
public:
    virtual void run();

    virtual void initialize(ServerInterface &srvInterface,
                            SizedColumnTypes &returnType);

    virtual void deinitialize(ServerInterface &srvInterface,
                              SizedColumnTypes &returnType);

    VTAllocator *getAllocator();


};

class FAvroParserFactory: public ParserFactory {
public:
    virtual void plan(ServerInterface &srvInterface,
                      PerColumnParamReader &perColumnParamReader, PlanContext &planCtxt);

    virtual UDParser* prepare(ServerInterface &srvInterface,
                              PerColumnParamReader &perColumnParamReader, PlanContext &planCtxt,
                              const SizedColumnTypes &returnType);

    virtual void getParserReturnType(ServerInterface &srvInterface,
                                     PerColumnParamReader &perColumnParamReader,
                                     PlanContext &planCtxt, const SizedColumnTypes &argTypes,
                                     SizedColumnTypes &returnType);

    virtual void getParameterType(ServerInterface &srvInterface,
                                  SizedColumnTypes &parameterTypes);
};
} /// END namespace flextable


/// END define AVRO_H_
#endif

#endif // NAVRO
