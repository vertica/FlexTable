/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#ifndef NAVRO

#ifndef avro_CRReader_hh__
#define avro_CRReader_hh__

#include "Config.hh"
#include "Encoder.hh"
#include "buffer/Buffer.hh"
#include "ValidSchema.hh"
#include "Specific.hh"
#include "Stream.hh"
#include "CoroutineHelpers.h"
#include "Vertica.h"
#include "CRStream.hh"

#include <map>
#include <string>
#include <vector>

#include "boost/array.hpp"
#include "boost/utility.hpp"

#include <byteswap.h>
#include <stdlib.h>
#include <stdio.h>
#include <sstream>
#include <memory>
#include <iostream>
#include <boost/shared_array.hpp>

namespace avro {

enum Codec {
  NULL_CODEC,
  SNAPPY_CODEC
};
/**
 * The sync value.
 */
typedef boost::array<uint8_t, 16> DataFileSync;

/**
 * The type independent portion of rader.
 */
class AVRO_DECL CRReaderBase: boost::noncopyable {
	//const std::string filename_;
	//const ContinuousReader &cr_;
	const std::auto_ptr<InputStream> stream_;
	const DecoderPtr decoder_;
	int64_t objectCount_;

	ValidSchema readerSchema_;
	ValidSchema dataSchema_;
	DecoderPtr dataDecoder_;
	std::auto_ptr<InputStream> dataStream_;
	typedef std::map<std::string, std::vector<uint8_t> > Metadata;

	Metadata metadata_;
	DataFileSync sync_;
        
        int has_data;
	bool use_external_schema;
        
	void readHeader();
    
	void readHeaderExternalSchema();

	int readDataBlock();
        
        // for compressed buffer
        std::vector<char> compressed_;
        Codec codec_;
        bool is_kafka;
        boost::shared_array<char> charPtr_t;

    
public:
	/**
	 * Returns the current decoder for this reader.
	 */
	Decoder& decoder() {
		return *dataDecoder_;
	}

        int64_t objectCount(){ return objectCount_; }
	/**
	 * Returns true if and only if there is more to read.
	 */
	int hasMore();

	/**
	 * Decrements the number of objects yet to read.
	 */
	void decr() {
		--objectCount_;
	}

	/**
	 * Constructs the reader for the given file and the reader is
	 * expected to use the schema that is used with data.
	 * This function should be called exactly once after constructing
	 * the CRReaderBase object.
	 */
	CRReaderBase(ContinuousReader& cr);
       
	/**
	 * Constructs the reader for the given message (Kafka usage)
         * and the reader is expected to use the schema that is used with data.
	 * This function should be called exactly once after constructing
	 * the CRReaderBase object
         * cr object is passed but not used, as InputStream is created from
         * the message.
	 */
        CRReaderBase(const char *message, uint length, bool use_external);
        
	/**
	 * Initializes the reader so that the reader and writer schemas
	 * are the same.
	 */
	void init();

	/**
	 * Initializes the reader to read objects according to the given
	 * schema. This gives an opportinity for the reader to see the schema
	 * in the data file before deciding the right schema to use for reading.
	 * This must be called exactly once after constructing the
	 * CRReaderBase object.
	 */
	void init(const ValidSchema& readerSchema);

	/**
	 * Returns the schema for this object.
	 */
	const ValidSchema& readerSchema() {
		return readerSchema_;
	}

	/**
	 * Returns the schema stored with the data file.
	 */
	const ValidSchema& dataSchema() {
		return dataSchema_;
	}
   
	void setDataSchema(ValidSchema vs) {
		dataSchema_ = vs;
	}
     
        void setCodec(Codec c) {
		codec_ = c;
	}
        
        void setIsKafka(bool isKafka){
            is_kafka = isKafka;
        }
	/**
	 * Closes the reader. No further operation is possible on this reader.
	 */
	void close();

	int get_has_data() {
		return has_data;
	}
};

/**
 * Reads the contents of data file one after another.
 */
template<typename T>
class CRReader: boost::noncopyable {
	std::auto_ptr<CRReaderBase> base_;
public:
	/**
	 * Constructs the reader for the given file and the reader is
	 * expected to use the given schema.
	 */
	CRReader(ContinuousReader& cr, const ValidSchema& readerSchema) :
		base_(new CRReaderBase(cr)) {
                base_->setIsKafka(false);
		base_->init(readerSchema);
	}

	/**
	 * Constructs the reader for the given file and the reader is
	 * expected to use the schema that is used with data.
	 */
	CRReader(ContinuousReader& cr) :
		base_(new CRReaderBase(cr)) {
                base_->setIsKafka(false);
		base_->init();
	}

	/**
	 * Constructs a reader using the reader base. This form of constructor
	 * allows the user to examine the schema of a given file and then
	 * decide to use the right type of data to be desrialize. Without this
	 * the user must know the type of data for the template _before_
	 * he knows the schema within the file.
	 * The schema present in the data file will be used for reading
	 * from this reader.
	 */
	CRReader(std::auto_ptr<CRReaderBase> base) :
		base_(base) {
                base_->setIsKafka(false);
		base_->init();
	}

	/**
	 * Constructs a reader using the reader base. This form of constructor
	 * allows the user to examine the schema of a given file and then
	 * decide to use the right type of data to be desrialize. Without this
	 * the user must know the type of data for the template _before_
	 * he knows the schema within the file.
	 * The argument readerSchema will be used for reading
	 * from this reader.
	 */
	CRReader(std::auto_ptr<CRReaderBase> base, const ValidSchema& readerSchema) :
		base_(base) {
                base_->setIsKafka(false);
		base_->init(readerSchema);
	}
        
	/**
	 * Constructs the reader for the given message and the reader is
	 * expected to use the schema that is used with data.
	 */
        CRReader(const char *message, uint length):
		base_(new CRReaderBase(message,length,false)) {
                base_->setIsKafka(true);
		base_->init();
	}
        
        CRReader(const char *message, uint length, const ValidSchema& readerSchema,
                bool use_snappy_codec) :
		base_(new CRReaderBase(message,length,true)) {
            base_->setIsKafka(true);
            if(use_snappy_codec){
                base_->setCodec(SNAPPY_CODEC);
            }
            else{ //default
                base_->setCodec(NULL_CODEC);
            }
                base_->setDataSchema(readerSchema);
		base_->init(readerSchema);
	}

	/**
	 * Reads the next entry from the data file.
	 * \return true if an object has been successfully read into \p datum and
	 * false if there are no more entries in the file.
	 */
	int read(T& datum) {
            try{
                int readData = base_->hasMore();
                if (readData > 0) {
                    base_->decr();
                    try {
                        avro::decode(base_->decoder(), datum);
                    } catch (std::exception& e) {
                        return -1;
                    }
                    return 1;
                }
                return readData;
            }
            catch(std::exception e){
                return -1;
            }
	}

	/**
	 * Returns the schema for this object.
	 */
	const ValidSchema& readerSchema() {
		return base_->readerSchema();
	}

	/**
	 * Returns the schema stored with the data file.
	 */
	const ValidSchema& dataSchema() {
		return base_->dataSchema();
	}
        
	/**
	 * Closes the reader. No further operation is possible on this reader.
	 */
	void close() {
		return base_->close();
	}

	int get_has_data() {
    	return base_->get_has_data();
	}
};

} // namespace avro
#endif

#endif // NAVRO
