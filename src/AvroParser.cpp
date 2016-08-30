/**
 * Copyright (c) 2005 - 2015 Hewlett Packard Enterprise Development LP
 *
 * Creation Date: February  2014
 */
/**
 * Flex Table Avro parser
 *
 */
#ifndef NAVRO

#include "FlexTable.h"
#include "Vertica.h"
#include "ContinuousUDParser.h"
#include "StringParsers.h"
#include "CRReader.hh"
#include "AvroParser.h"
#include "AvroDatumParsers.h"

using namespace Vertica;

#include <string>
#include <vector>
#include <sstream>
#include <fstream>
#include <iostream>
#include <stack>
#include <ctime>

using namespace std;

namespace flextable {
/// BEGIN class FlexTableAvroParser

FlexTableAvroParser::FlexTableAvroParser(
    std::vector<std::string> formatStrings,
    bool rejectOnMaterializedTypeError,
    bool flattenMaps,
    bool flattenArrays,
    bool flattenRecords,
    bool enforce_length) :
    map_col_index(-1), map_data_buf(NULL), map_data_buf_len(-1),
    pair_buf(NULL), is_flextable(false), formatStrings(formatStrings),
    reject_on_materialized_type_error(rejectOnMaterializedTypeError),
    flatten_maps(flattenMaps), flatten_arrays(flattenArrays),
    flatten_records(flattenRecords),enforcelength(enforce_length), aux_buf() {
    state = OK;
}

FlexTableAvroParser::~FlexTableAvroParser() {
}

//for real table insertion

bool FlexTableAvroParser::handle_field(size_t colNum, char* start, size_t len, bool hasPadding) {
    // Empty columns are null.
    if (len == 0) {
        writer->setNull(colNum);
        return true;
    } else {
    	const Vertica::VerticaType &destColType = colInfo.getColumnType(colNum);
        ssize_t maxDataLen = len;
        if (destColType.isStringType()) {
	    ssize_t destColLength = (ssize_t)destColType.getStringLength();
	    if(enforcelength && (destColLength >= 0 && len > (size_t)destColLength)){
                return false;
            }

            maxDataLen = std::min(((ssize_t)destColType.getStringLength()), maxDataLen);
        }
        NullTerminatedString str(start, maxDataLen, false, hasPadding);
        try {
            return parseStringToType(str.ptr(), str.size(), colNum, colInfo.getColumnType(colNum), writer, sp);
        } catch (std::exception& e) {
            log(
                "Exception while processing partition: [%s] while type converting Avro materialized column #[%lu], skipping this materialized column for this row.",
                e.what(), colNum);
        } catch (...) {
            log(
                "Unknown exception while type converting Avro materialized column #[%lu], skipping this materialized column for for this row.",
                colNum);
        }
    }

    writer->setNull(colNum);
    return false;
}

void FlexTableAvroParser::rejectRecord(const std::string &reason) {
    RejectedRecord rr(reason, rejected_data.c_str(), rejected_data.size(), std::string(1, '\n'));
    crej.reject(rr);
}

/*
 * Creates a new CRReader to correctly read and parse avro files
 * For each row (avro datum) read it will parse the corresponding type of data (record, array, enum, etc.)
 * and will insert the parsed data into the Flex VMap and at the end it will process any materialized columns
 */
void FlexTableAvroParser::run() {
    if (!is_flextable && (!flatten_records || !flatten_arrays || !flatten_maps)) {
        state = NOT_FLATTEN_REAL_TABLE;
        return;
    }
    try {
        dataReader = new CRReader<avro::GenericDatum> (cr);
    } catch (std::exception& e) {
        //could not create the reader
        state = ERROR_CRAETING_READER;
        return;
    }
    avro::GenericDatum datum(dataReader->dataSchema());
    rowNum = 0;
    //counter to name the primitive types col# as primitive types does not have names
    int primitiveType = 0;

    try {
        //read until we end with the file (status 0)
        //if status == -1 error reading the file
        while (true) {
            num_parsed_real_values = 0;
            int status = dataReader->read(datum);
            if (status == 0) {
                //done reading file
                cleanup();
                break;
            }
            else if (status < 0) {
                handleErrorState(status);
                cleanup();
                return;
            }
            rowNum++;
            bool rejected = false;
            bool resolved = false;
            parsedRealCols.resize(colInfo.getColumnCount(), false);

            if (is_flextable) {
                VMapPairWriter map_writer(pair_buf, map_data_buf_len);
                //call corresponding "resolve" functions depending on datum type
                if (datum.type() == AVRO_RECORD) {
                    GenericRecord rec = datum.value<GenericRecord> ();
                    resolved = resolveRecord(map_writer, rec);
                } else if (datum.type() == AVRO_ENUM) {
                    GenericEnum avroEnum = datum.value<GenericEnum> ();
                    resolved = resolveEnum(map_writer, avroEnum, true, false);
                } else if (datum.type() == AVRO_ARRAY) {
                    GenericArray avroArray = datum.value<GenericArray> ();
                    resolved = resolveArray(map_writer, avroArray);
                } else if (datum.type() == AVRO_MAP) {
                    GenericMap avroMap = datum.value<GenericMap> ();
                    resolved = resolveMap(map_writer, avroMap);
                } else {
                    //just a primitive type, set name as ucol# as they dont have name by themselves
                    //and as we are on the outer level, we dont need to concatenate keys of nested maps
                    std::string primitiveKeyName = "";
                    std::ostringstream ss;
                    ss << "ucol" << primitiveType;
                    primitiveKeyName = ss.str();
                    primitiveType++;

                    //virtual column value
                    std::string value = parseAvroFieldToString(datum);
                    if (map_writer.append(getServerInterface(), primitiveKeyName.c_str(), primitiveKeyName.length(),
                                          false, value.c_str(), value.length())) {
                        resolved = true;
                    } else {
                        rejected = true;
                        std::stringstream rejectMessage;
                        rejectMessage << "Error:  Row [" << rowNum << "] failed to append value for column: [" << primitiveKeyName << "]";
                        if(value.length() > 1000){
                            rejectMessage << " with value: [" << value.substr(0,1000) << "].";    
                        }
                        else{
                            rejectMessage << " with value: [" << value << "].";
                        }
                        rejected_data = value;
                        LogDebugUDBasic("%s", rejectMessage.str().c_str());
                        rejectRecord(rejectMessage.str());
                        resolved = false;
                    }
                }
                if (!resolved || rejected) {
                    clearBaseNameStack();
                    continue;
                }
                //once we finish with the record, check for materialized columns
                size_t total_offset = 0;
                if (!construct_map(map_writer, total_offset)) {
                    rejected = true;
                    continue;
                }
            } else {
                VMapPairWriter map_writer(pair_buf, 0);
                if (datum.type() == AVRO_RECORD) {
                    GenericRecord rec = datum.value<GenericRecord> ();
                    resolved = resolveRecord(map_writer, rec);
                } else if (datum.type() == AVRO_ENUM) {
                    GenericEnum avroEnum = datum.value<GenericEnum> ();
                    resolved = resolveEnum(map_writer, avroEnum, true, false);
                } else if (datum.type() == AVRO_ARRAY) {
                    GenericArray avroArray = datum.value<GenericArray> ();
                    resolved = resolveArray(map_writer, avroArray);
                } else if (datum.type() == AVRO_MAP) {
                    GenericMap avroMap = datum.value<GenericMap> ();
                    resolved = resolveMap(map_writer, avroMap);
                } else {
                    //just a primitive type, set name as ucol# as they dont have name by themselves
                    //and as we are on the outer level, we dont need to concatenate keys of nested maps
                    std::string primitiveKeyName = "";
                    std::ostringstream ss;
                    ss << "ucol" << primitiveType;
                    primitiveKeyName = ss.str();
                    primitiveType++;

                    //virtual column value
                    std::string value = parseAvroFieldToString(datum);
                    resolved = resolveRealFieldIntoTable(primitiveKeyName, value);
                }
                if (!resolved) {
                    clearBaseNameStack();
                    continue;
                }
            }
            if (!rejected) {
                // Make sure our output column is clean (especially in the case of previously-rejected rows)
                for (size_t realColNum = 0; realColNum < writer->getNumCols(); realColNum++) {
                    if (!(parsedRealCols[realColNum]
                          || (0 == strcmp(colInfo.getColumnName(realColNum).c_str(),
                                          RAW_COLUMN_NAME)))) {
                        writer->setNull(realColNum);
                    }
                }
                writer->next();
                recordsAcceptedInBatch++;
            }

            clearBaseNameStack();
        }
    } catch(std::exception& e) {
        log("Exception while processing partition: [%s] while emitting AVRO row [%u], skipping further data for this row.", e.what(), rowNum);
    } catch(...) {
        log("Unknown exception while emitting AVRO row [%u], skipping futher data for this row.", rowNum);
    }
    cleanup();
}

void FlexTableAvroParser::handleErrorState(int status) {
    switch (status) {
    case -1:
        state = ERROR_READING_FILE;
        break;
    case -2:
        state = ERROR_SYNC_MISMATCH;
        break;
    case -3:
        state = ERROR_SNAPPY_LENGTH;
        break;
    case -4:
        state = ERROR_SNAPPY_MEMORY;
        break;
    case -5:
        state = ERROR_SNAPPY_UNCOMPRESS;
        break;
    case -6:
        state = ERROR_CRC32;
        break;
    case -7:
        state = ERROR_CODEC;
        break;

    }
}
//Method that clears the key names stack

void FlexTableAvroParser::clearBaseNameStack() {
    while (!baseName.empty()) {
        baseName.pop();
    }
}

//Converts a vmap (convert_vmap) and process materialized columns

bool FlexTableAvroParser::construct_map(VMapPairWriter& map_writer_nested, size_t& total_offset) {
    VMapPairReader map_reader(map_writer_nested);
    // Capture dump-column data
    if (map_col_index != -1) {
        VMapBlockWriter::convert_vmap(getServerInterface(), map_reader,
                                      map_data_buf, map_data_buf_len, total_offset);
        VString& map_data_col = writer->getStringRef(map_col_index);
        map_data_col.copy(map_data_buf, total_offset);
    }

    bool rejected = false;
    // Process any materialized columns
    const std::vector<VMapPair> &vpairs = map_reader.get_pairs();
    for (size_t virtualColNum = 0; virtualColNum < vpairs.size(); virtualColNum++) {
        const size_t keyLen = vpairs[virtualColNum].key_length();
        aux_buf.allocate(keyLen + 1);
        normalize_key(vpairs[virtualColNum].key_str(), keyLen, aux_buf.loc);
        ImmutableStringPtr keyString(aux_buf.loc, keyLen);
        ColLkup::iterator it = real_col_lookup.find(keyString);

        if (it != real_col_lookup.end()) {
            // Copy value into the auxiliary buffer so we can append
            // a null terminator to it
            const size_t valueLen = vpairs[virtualColNum].value_length();
            aux_buf.allocate(valueLen + 1);
            memcpy(aux_buf.loc, vpairs[virtualColNum].value_str(), valueLen);

            if (handle_field(it->second, aux_buf.loc, valueLen, true)) {
                parsedRealCols[it->second] = true;
            } else if (reject_on_materialized_type_error) {
                rejected = true;
                std::ostringstream rejectMessage;
                rejectMessage << "Row [" << rowNum
                              << "] rejected due to materialized type error on column: ["
                              << std::string(vpairs[virtualColNum].key_str(), keyLen) << "]";
                rejectMessage << " with value: [" << std::string(
                    vpairs[virtualColNum].value_str(), valueLen) << "].";
                rejected_data = std::string(vpairs[virtualColNum].value_str(), valueLen);
                LogDebugUDBasic("%s", rejectMessage.str().c_str());
                rejectRecord(rejectMessage.str());
                break;
            }
        }
    }
    return !rejected;
}

bool FlexTableAvroParser::construct_append_nested_map(
    VMapPairWriter& map_writer, GenericDatum d) {
    bool resolved = false;
    //nested record, construct new vmap
    VMapPairWriter map_writer_not_flatten(
        map_writer.get_buf() + map_writer.get_buf_used(),
        map_writer.get_buf_total_size() - map_writer.get_buf_used());
    //resolve record using the created vmap for nested case
    if (d.type() == AVRO_RECORD) {
        GenericRecord rec = d.value<GenericRecord> ();
        resolved = resolveRecord(map_writer_not_flatten, rec);
    } else if (d.type() == AVRO_ARRAY) {
        GenericArray avroArray = d.value<GenericArray> ();
        resolved = resolveArray(map_writer_not_flatten, avroArray);
    } else if (d.type() == AVRO_MAP) {
        GenericMap avroMap = d.value<GenericMap> ();
        resolved = resolveMap(map_writer_not_flatten, avroMap);
    }
    if (!resolved) {
        return false;
    }
    size_t total_offset = 0;

    if (!construct_map(map_writer_not_flatten, total_offset)) {
        //rejected
        return false;
    }
    char* results_buf_loc = map_data_buf;
    //once resolved append the nested vmap to the parents vmap
    if (map_writer.append(getServerInterface(), baseName.top().c_str(),
                          baseName.top().length(), false, results_buf_loc, total_offset)) {
        num_parsed_real_values++;
        //nested vmap append OK
    } else {
        rejected_data = "";
        std::stringstream rejectMessage;
        rejectMessage << "Error:  Row [" << rowNum << "] failed to append nested map for column: [" << baseName.top() << "].";
        LogDebugUDBasic("%s", rejectMessage.str().c_str());
        rejectRecord(rejectMessage.str());
        return false;
    }
    return true;
}

//Resolves insertion into real table, searches column in real table
//and try to insert the value

bool FlexTableAvroParser::resolveRealFieldIntoTable(std::string columnName, std::string value) {
    size_t posCol = std::find(real_column_names.begin(), real_column_names.end(), columnName) - real_column_names.begin();
    if (posCol < real_column_names.size()) {
        //real table contains column name
        // Copy value into the auxiliary buffer so we can append
        // a null terminator to it
        const size_t valueLen = value.length();
        aux_buf.allocate(valueLen + 1);
        memcpy(aux_buf.loc, value.c_str(), valueLen);
        
        if (handle_field(posCol, aux_buf.loc, valueLen, true)) {
            num_parsed_real_values++;
            parsedRealCols[posCol] = true;
        } else {
            rejected_data = value;
            std::ostringstream rejectMessage;
            rejectMessage << "Row [" << rowNum
                          << "] rejected, error parsing column: ["
                          << columnName << "] with value ["
                          << value << "]";

            LogDebugUDBasic("%s", rejectMessage.str().c_str());
            rejectRecord(rejectMessage.str());
            return false;
        }
    }
    //////        else {
    //////            //real table does not contain column name
    //////            rejected_data = value;
    //////            std::ostringstream rejectMessage;
    //////            rejectMessage << "Row [" << rowNum
    //////                    << "] rejected, table does not contain column: ["
    //////                    << columnName << "]";
    //////
    //////            LogDebugUDBasic("%s", rejectMessage.str().c_str());
    //////            rejectRecord(rejectMessage.str());
    //////            return false;
    //////        }
    return true;
}

void FlexTableAvroParser::constructRejectMessage(GenericDatum d){
    std::stringstream rejectMessage;
    std::string value = parseAvroFieldToString(d);
    rejected_data = value;
    rejectMessage << "Error:  Row [" << rowNum << "] failed to append value for column: [" << colInfo.getColumnName(num_parsed_real_values) << "]";
    if(value.length() > 1000){
        rejectMessage << " with value: [" << value.substr(0,1000) << "].";    
    }
    else{
        rejectMessage << " with value: [" << value << "].";
    }
    LogDebugUDBasic("%s", rejectMessage.str().c_str());
    rejectRecord(rejectMessage.str());  
}

void FlexTableAvroParser::constructRejectMessage_NameKey_(std::string recordType){
    std::stringstream rejectMessage;
    rejected_data = recordType;
    rejectMessage << "Error:  Row [" << rowNum << "] failed to append value for column: [" << colInfo.getColumnName(num_parsed_real_values) << "]";
    rejectMessage << " with value: [" << recordType << "].";
    LogDebugUDBasic("%s", rejectMessage.str().c_str());
    rejectRecord(rejectMessage.str());  
}

//Parses the Avro record received and insert the key(record field name) - value(record field value) pairs into the vmap

bool FlexTableAvroParser::resolveRecord(VMapPairWriter& map_writer, GenericRecord record) {
    bool resolved = true;
    std::string recordType = "";
    std::string typeSt = "";

    //append the __name__ artificial key for record
    //key - "__name__", value - name of the Record
    if (is_flextable && record.schema()->hasName()) {
        recordType = record.schema()->name().simpleName();
        if (baseName.empty() || !flatten_records) {
            typeSt = "__name__";
        } else if (flatten_records) {
            typeSt = baseName.top() + ".__name__";
        }

        if (!map_writer.append(getServerInterface(), typeSt.c_str(),
                                   typeSt.length(), (recordType.empty()), recordType.c_str(),
                                   recordType.length())) {
            rejected_data = recordType;
            std::stringstream rejectMessage;
            rejectMessage << "Error:  Row [" << rowNum << "] failed to append value for column: [" << typeSt << "]";
            rejectMessage << " with value: [" << recordType << "].";
            LogDebugUDBasic("%s", rejectMessage.str().c_str());
            rejectRecord(rejectMessage.str());
            return false;
        } 
    }

    //iterate record fields
    for (size_t col = 0; col < record.fieldCount(); col++) {
        std::string columnName = "";
        GenericDatum d = record.fieldAt(col);
        if (d.type() == AVRO_RECORD || d.type() == AVRO_ARRAY || d.type() == AVRO_MAP) {
            //Pushing the name of the record here before resolving the nested field
            if (baseName.empty() || !flatten_records) {
                baseName.push(record.schema()->nameAt(col));
            } else if (flatten_records) {
                baseName.push(baseName.top() + "." + record.schema()->nameAt(col));
            }
        }
        if (d.type() == AVRO_RECORD) {
            if (!flatten_records) {
                if (!construct_append_nested_map(map_writer, d)) {
                    baseName.pop();
                    return false;
                }
                resolved = true;
            } else {
                //not flatten, use the original vmap (map_writer) and resolve the record
                GenericRecord rec = d.value<GenericRecord> ();
                resolved = resolveRecord(map_writer, rec);
            }
            baseName.pop();
        } else if (d.type() == AVRO_ENUM) {
            GenericEnum avroEnum = d.value<GenericEnum> ();
            resolved = resolveEnum(map_writer, avroEnum, flatten_records, false);
        } else if (d.type() == AVRO_ARRAY) {
            if (!flatten_arrays) {
                if (!construct_append_nested_map(map_writer, d)) {
                    baseName.pop();
                    return false;
                }
                resolved = true;
            } else {
                GenericArray avroArray = d.value<GenericArray> ();
                resolved = resolveArray(map_writer, avroArray);
            }
            baseName.pop();
        } else if (d.type() == AVRO_MAP) {
            if (!flatten_maps) {
                if (!construct_append_nested_map(map_writer, d)) {
                    baseName.pop();
                    return false;
                }
                resolved = true;
            } else {
                GenericMap avroMap = d.value<GenericMap> ();
                resolved = resolveMap(map_writer, avroMap);
            }
            baseName.pop();
        } else {
            //primitive
            if (is_flextable) {
                if (baseName.empty() || !flatten_records) {
                    columnName = record.schema()->nameAt(col);
                } else if (flatten_records) {
                    columnName = baseName.top() + "."
                            + record.schema()->nameAt(col);
                }
                //virtual column value
                std::string value = parseAvroFieldToString(d);
                if (map_writer.append(getServerInterface(), columnName.c_str(),
                        columnName.length(), false, value.c_str(), value.length())) {
                    resolved = true;
                } else {
                    rejected_data = value;
                    std::stringstream rejectMessage;
                    rejectMessage << "Error:  Row [" << rowNum << "] failed to append value for column: [" << columnName << "]";
                    if(value.length() > 1000){
                rejectMessage << " with value: [" << value.substr(0,1000) << "].";    
                    }
            else{
                        rejectMessage << " with value: [" << value << "].";
                    }
                    LogDebugUDBasic("%s", rejectMessage.str().c_str());
                    rejectRecord(rejectMessage.str());
                    return false;
                }
            } else {
                //real table
                if (!parseAvroFieldToType(d, num_parsed_real_values, colInfo.getColumnType(num_parsed_real_values), writer,enforcelength)) {
                    constructRejectMessage(d);
                    return false;
                }
                parsedRealCols[num_parsed_real_values] = true;
                num_parsed_real_values++;
                resolved = true;
            }
        }
        if (!resolved)
            return false;
    }
    return resolved;
}

//Parse avro enum field, will get enum name as key and enum symbol as value

bool FlexTableAvroParser::resolveEnum(VMapPairWriter& map_writer,
                                      GenericEnum avroEnum, bool flatten, bool isArrayEnum) {
    bool resolved = true;
    std::string fieldName = "";
    if (avroEnum.schema()->hasName()) {
        if (baseName.empty()) {
            fieldName = avroEnum.schema()->name().simpleName();
        } else if (flatten) {
            fieldName = baseName.top() + "."
                + avroEnum.schema()->name().simpleName();
        } else if (!flatten) {
            if (isArrayEnum) {
                fieldName = baseName.top() + "."
                    + avroEnum.schema()->name().simpleName();
            } else {
                fieldName = avroEnum.schema()->name().simpleName();
            }
        }
    }

    std::string enumSymbol = avroEnum.symbol();
    if (is_flextable) {
        if (map_writer.append(getServerInterface(), fieldName.c_str(),
                              fieldName.length(), false, enumSymbol.c_str(), enumSymbol.length())) {
            resolved = true;
        } else {
            rejected_data = enumSymbol;
            std::stringstream rejectMessage;
            rejectMessage << "Error:  Row [" << rowNum << "] failed to append value for column: [" << fieldName << "]";
            if(enumSymbol.length() > 1000){
                rejectMessage << " with value: [" << enumSymbol.substr(0,1000) << "].";    
            }
            else{
                rejectMessage << " with value: [" << enumSymbol << "].";
            }
            LogDebugUDBasic("%s", rejectMessage.str().c_str());
            rejectRecord(rejectMessage.str());
            return false;
        }
    } else {
        if (!resolveRealFieldIntoTable(fieldName, enumSymbol)) {
            return false;
        }
    }

    return true;
}

bool FlexTableAvroParser::resolveArray(VMapPairWriter& map_writer,
                                       GenericArray avroArray) {
    bool resolved = true;

    for (size_t indexArray = 0; indexArray < avroArray.value().size(); indexArray++) {
        //get index
        std::ostringstream ss;
        ss << indexArray;
        std::string indexString = ss.str();
        //virtual column name
        if (baseName.empty() || !flatten_arrays) {
            baseName.push(indexString);
        } else if (flatten_arrays) {
            //get last index of . and concat the index of the array field into the key names
            unsigned found = baseName.top().find_last_of(".");
            std::ostringstream ss2;
            ss2 << found;
            std::string str2 = ss2.str();
            if (found > baseName.top().size()) {
                baseName.push(
                    indexString + "." + baseName.top().substr(found + 1));
            } else {
                baseName.push(
                    baseName.top().substr(0, found) + "." + indexString
                    + "." + baseName.top().substr(found + 1,
                                                  baseName.top().size()));
            }
        }

        GenericDatum d = avroArray.value().at(indexArray);
        if (d.type() == AVRO_RECORD) {
            if (!flatten_records) {
                if (!construct_append_nested_map(map_writer, d)) {
                    baseName.pop();
                    return false;
                }
                resolved = true;
            } else {
                GenericRecord rec = d.value<GenericRecord> ();
                resolved = resolveRecord(map_writer, rec);
            }
        } else if (d.type() == AVRO_ENUM) {
            GenericEnum avroEnum = d.value<GenericEnum> ();
            resolved = resolveEnum(map_writer, avroEnum, flatten_arrays, true);
        } else if (d.type() == AVRO_ARRAY) {
            if (!flatten_arrays) {
                if (!construct_append_nested_map(map_writer, d)) {
                    baseName.pop();
                    return false;
                }
                resolved = true;
            } else {
                GenericArray avro_Array = d.value<GenericArray> ();
                resolved = resolveArray(map_writer, avro_Array);
            }

        } else if (d.type() == AVRO_MAP) {
            if (!flatten_maps) {
                if (!construct_append_nested_map(map_writer, d)) {
                    baseName.pop();
                    return false;
                }
                resolved = true;
            } else {
                GenericMap avroMap = d.value<GenericMap> ();
                resolved = resolveMap(map_writer, avroMap);
            }
        } else {
            if (is_flextable) {
                //virtual column value
                std::string value = parseAvroFieldToString(d);
                if (map_writer.append(getServerInterface(), baseName.top().c_str(),
                        baseName.top().length(), false, value.c_str(), value.length())) {
                    resolved = true;
                } else {
                    rejected_data = value;
                    std::stringstream rejectMessage;
                    rejectMessage << "Error:  Row [" << rowNum << "] failed to append value for column: [" << baseName.top() << "]";
                    if(value.length() > 1000){
                rejectMessage << " with value: [" << value.substr(0,1000) << "].";    
                    }
            else{
                        rejectMessage << " with value: [" << value << "].";
                    }
                    LogDebugUDBasic("%s", rejectMessage.str().c_str());
                    rejectRecord(rejectMessage.str());
                    baseName.pop();
                    return false;
                }
            } else {
                if (!parseAvroFieldToType(d, num_parsed_real_values, colInfo.getColumnType(num_parsed_real_values), writer,enforcelength)) {
                    baseName.pop();
                    constructRejectMessage(d);
                    return false;
                }
                parsedRealCols[num_parsed_real_values] = true;
                num_parsed_real_values++;
                resolved = true;
            }

        }
        baseName.pop();
        if (!resolved) {
            return false;
        }
    }
    return true;
}

bool FlexTableAvroParser::resolveMap(VMapPairWriter& map_writer,
                                     GenericMap avroMap) {
    bool resolved = true;

    //get key-value of avro map and iterate them
    std::vector < std::pair<std::string, GenericDatum> > avroVectorPairMap
        = avroMap.value();
    for (size_t j = 0; j < avroVectorPairMap.size(); j++) {
        std::pair < std::string, GenericDatum > pairDatum
            = avroVectorPairMap.at(j);
        GenericDatum d = pairDatum.second;
        std::string mapKey = pairDatum.first;
        //add key of avro map as key name for virtual column
        if (baseName.empty() || !flatten_maps) {
            baseName.push(mapKey);
        } else if (flatten_maps) {
            baseName.push(baseName.top() + "." + mapKey);
        }

        if (d.type() == AVRO_RECORD) {
            if (!flatten_records) {
                if (!construct_append_nested_map(map_writer, d)) {
                    baseName.pop();
                    return false;
                }
                resolved = true;
            } else {
                GenericRecord rec = d.value<GenericRecord> ();
                resolved = resolveRecord(map_writer, rec);
            }
        } else if (d.type() == AVRO_ENUM) {
            GenericEnum avroEnum = d.value<GenericEnum> ();
            resolved = resolveEnum(map_writer, avroEnum, flatten_maps, true);
        } else if (d.type() == AVRO_ARRAY) {
            if (!flatten_arrays) {
                if (!construct_append_nested_map(map_writer, d)) {
                    baseName.pop();
                    return false;
                }
                resolved = true;
            } else {
                GenericArray avroArray = d.value<GenericArray> ();
                resolved = resolveArray(map_writer, avroArray);
            }
        } else if (d.type() == AVRO_MAP) {
            if (!flatten_maps) {
                if (!construct_append_nested_map(map_writer, d)) {
                    baseName.pop();
                    return false;
                }
                resolved = true;
            } else {
                GenericMap avroMap = d.value<GenericMap> ();
                resolved = resolveMap(map_writer, avroMap);
            }
        } else {
            if (is_flextable) {
                std::string value = parseAvroFieldToString(d);
                if (map_writer.append(getServerInterface(), baseName.top().c_str(),
                        baseName.top().length(), false, value.c_str(),
                        value.length())) {
                    resolved = true;
                } else {
                    rejected_data = value;
                    std::stringstream rejectMessage;
                    rejectMessage << "Error:  Row [" << rowNum << "] failed to append value for column: [" << baseName.top() << "]";
                    if(value.length() > 1000){
                        rejectMessage << " with value: [" << value.substr(0,1000) << "].";    
                    }
                    else{
                        rejectMessage << " with value: [" << value << "].";
                    }
                    LogDebugUDBasic("%s", rejectMessage.str().c_str());
                    rejectRecord(rejectMessage.str());
                    baseName.pop();
                    return false;
                }
            } else {
                if (!parseAvroFieldToType(d, num_parsed_real_values, colInfo.getColumnType(num_parsed_real_values), writer, enforcelength)) {
                    baseName.pop();
                    constructRejectMessage(d);
                    return false;
                }
                parsedRealCols[num_parsed_real_values] = true;
                num_parsed_real_values++;
                resolved = true;
            }

        }
        baseName.pop();
        if (!resolved) {
            return false;
        }
    }
    return true;
}

void FlexTableAvroParser::initialize(ServerInterface &srvInterface,
                                     SizedColumnTypes &returnType) {
    aux_buf.initialize(&srvInterface, BASE_RESERVE_SIZE);

    delete dataReader;
    dataReader = NULL;
    colInfo = returnType;
    if (formatStrings.size() != returnType.getColumnCount()) {
        formatStrings.resize(returnType.getColumnCount(), "");
    }

    for (uint32 colNum = 0; colNum < colInfo.getColumnCount(); colNum++) {
        const std::string &str = colInfo.getColumnName(colNum);
        aux_buf.allocate(str.size() + 1);
        normalize_key(str.c_str(), str.size(), aux_buf.loc);
        real_col_lookup[ImmutableStringPtr(aux_buf.loc, str.size())]
            = colNum;
        aux_buf.loc += str.size() + 1;
        real_column_names.push_back(str);
    }

    // Find the flextable __raw__ column
    for (uint32 colNum = 0; colNum < colInfo.getColumnCount(); colNum++) {
        if ((0
             == strcmp(colInfo.getColumnName(colNum).c_str(),
                       RAW_COLUMN_NAME))
            && colInfo.getColumnType(colNum).isStringType()) {
            map_col_index = colNum;
            map_data_buf_len = colInfo.getColumnType(colNum).getStringLength();
            break;
        }
    }

    //if (map_data_buf_len <= 0) {
    //	map_data_buf_len = MAP_FOOTPRINT;
    //}

    //map_data_buf = (char*) srvInterface.allocator->alloc(map_data_buf_len);
    //parse_context->set_buf((char*) srvInterface.allocator->alloc(map_data_buf_len),map_data_buf_len);

    if (map_data_buf_len > 0) {
        //it is a flex table
        map_data_buf = (char*) srvInterface.allocator->alloc(map_data_buf_len);
        pair_buf = (char*) srvInterface.allocator->alloc(map_data_buf_len);
        is_flextable = true;
    }
}

void FlexTableAvroParser::cleanup() {
    if (dataReader) {
        dataReader->close();
        delete dataReader;
    }
    dataReader = NULL;
}

void FlexTableAvroParser::deinitialize(ServerInterface &srvInterface,
                                       SizedColumnTypes &returnType) {
    cleanup();
    if (state == ERROR_CRAETING_READER) {
        vt_report_error(0, "Error: Avro file schema not supported");
    }
    if (state == ERROR_READING_FILE) {
        vt_report_error(0, "Exception while parsing the Avro file. Incorrect/not supported Avro file/schema");
    }
    if (state == NOT_FLATTEN_REAL_TABLE) {
        vt_report_error(0, "Error: COPY into real table, flatten_records, flatten_arrays and flatten_maps must be true (default options: records and maps flattened, arrays not flattened)");
    }
    if (state == ERROR_SYNC_MISMATCH) {
        vt_report_error(0, "Exception while parsing the Avro file. Sync mismatch");
    }
    if (state == ERROR_SNAPPY_LENGTH) {
        vt_report_error(0, "[Snappy] Exception while parsing the Avro file. Could not get the snappy uncompressed length for file");
    }
    if (state == ERROR_SNAPPY_MEMORY) {
        vt_report_error(0, "[Snappy] Exception while parsing the Avro file. Could not allocate memory for uncompressed file data");
    }
    if (state == ERROR_SNAPPY_UNCOMPRESS) {
        vt_report_error(0, "[Snappy] Exception while parsing the Avro file. Error uncompressing the avro snappy compressed file");
    }
    if (state == ERROR_CRC32) {
        vt_report_error(0, "[Snappy] Exception while parsing the Avro file. CRC32 check failure uncompressing block with Snappy");
    }
    if (state == ERROR_CODEC) {
        vt_report_error(0, "Exception while parsing the Avro file. Codec not supported");
    }
}

// Gets allocator.

VTAllocator * FlexTableAvroParser::getAllocator() {
    return getServerInterface().allocator;
}
/// END class FlexTableAvroParser


/// BEGIN class FAvroParserFactory

void FAvroParserFactory::plan(ServerInterface &srvInterface,
                              PerColumnParamReader &perColumnParamReader, PlanContext &planCtxt) {
}

UDParser* FAvroParserFactory::prepare(ServerInterface &srvInterface,
                                      PerColumnParamReader &perColumnParamReader,
                                      PlanContext &planCtxt, const SizedColumnTypes &returnType) {
    // Defaults
    std::vector < std::string > formatStrings;
    bool rejectOnMaterializedTypeError(false);
    bool flatten_maps(true);
    bool flatten_arrays(false);
    bool flatten_records(true);
    bool enforcelength(false);

    // Args
    ParamReader args(srvInterface.getParamReader());
    if (args.containsParameter("reject_on_materialized_type_error")) {
        if (args.getBoolRef("reject_on_materialized_type_error") == vbool_null) {
            vt_report_error(2,
                            "Invalid parameter reject_on_materialized_type_error NULL");
        }
        rejectOnMaterializedTypeError = args.getBoolRef(
            "reject_on_materialized_type_error");
    }

    if (args.containsParameter("flatten_maps")) {
        if (args.getBoolRef("flatten_maps") == vbool_null) {
            vt_report_error(2, "Invalid parameter flatten_maps NULL");
        }
        flatten_maps = args.getBoolRef("flatten_maps");
    }
    if (args.containsParameter("flatten_arrays")) {
        if (args.getBoolRef("flatten_arrays") == vbool_null) {
            vt_report_error(2, "Invalid parameter flatten_arrays NULL");
        }
        flatten_arrays = args.getBoolRef("flatten_arrays");
    }
    if (args.containsParameter("flatten_records")) {
        if (args.getBoolRef("flatten_records") == vbool_null) {
            vt_report_error(2, "Invalid parameter flatten_records NULL");
        }
        flatten_records = args.getBoolRef("flatten_records");
    }
    if (args.containsParameter("enforce_length")) {
        if (args.getBoolRef("enforce_length") == vbool_null) {
            vt_report_error(2, "Invalid parameter enforce_length NULL");
        }
        enforcelength = args.getBoolRef("enforce_length");
    }

    // Extract the "format" argument.
    // Default to the global setting, but let any per-column settings override for that column.
    if (args.containsParameter("format")) {
        formatStrings.resize(returnType.getColumnCount(),
                             args.getStringRef("format").str());
    } else {
        formatStrings.resize(returnType.getColumnCount(), "");
    }

    for (size_t i = 0; i < returnType.getColumnCount(); i++) {
        const std::string & cname(returnType.getColumnName(i));
        if (perColumnParamReader.containsColumn(cname)) {
            ParamReader &colArgs = perColumnParamReader.getColumnParamReader(
                cname);
            if (colArgs.containsParameter("format")) {
                formatStrings[i] = colArgs.getStringRef("format").str();
            }
        }
    }

    return vt_createFuncObj(srvInterface.allocator, FlexTableAvroParser,
                            formatStrings, rejectOnMaterializedTypeError, flatten_maps,
                            flatten_arrays, flatten_records, enforcelength);
}

void FAvroParserFactory::getParserReturnType(ServerInterface &srvInterface,
                                             PerColumnParamReader &perColumnParamReader,
                                             PlanContext &planCtxt,
                                             const SizedColumnTypes &argTypes,
                                             SizedColumnTypes &returnType) {
    returnType = argTypes;
}

void FAvroParserFactory::getParameterType(ServerInterface &srvInterface,
                                          SizedColumnTypes &parameterTypes) {
    parameterTypes.addVarchar(256, "format");
    parameterTypes.addBool("reject_on_materialized_type_error");
    parameterTypes.addBool("flatten_maps");
    parameterTypes.addBool("flatten_arrays");
    parameterTypes.addBool("flatten_records");
    parameterTypes.addBool("enforce_length");

}
/// END class FAvroParserFactory

// Register the parser factory
RegisterFactory(FAvroParserFactory);
} /// END namespace flextable

#endif // NAVRO
