/**
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

#include "CRReader.hh"
#include "avro/Compiler.hh"
#include "avro/Exception.hh"

#include <sstream>

#include <boost/random/mersenne_twister.hpp>
#include "snappy-c.h"
#include "boost/crc.hpp"

namespace avro {
using std::auto_ptr;
using std::ostringstream;
using std::istringstream;
using std::vector;
using std::copy;
using std::string;

using boost::array;

const string AVRO_SCHEMA_KEY("avro.schema");
const string AVRO_CODEC_KEY("avro.codec");
const string AVRO_NULL_CODEC("null");
const string AVRO_SNAPPY_CODEC("snappy");

const size_t minSyncInterval = 32;
const size_t maxSyncInterval = 1u << 30;
const size_t defaultSyncInterval = 16 * 1024;

typedef array<uint8_t, 4> Magic;
static Magic magic = { { 'O', 'b', 'j', '\x01' } };

CRReaderBase::CRReaderBase(ContinuousReader& cr) :
    stream_( auto_ptr<InputStream>( new CRInputStream(cr)  ) ),
    decoder_(binaryDecoder()), objectCount_(0),codec_(NULL_CODEC)
{
    readHeader();
}

/*
 * Method to be used for Kafka message
 */
CRReaderBase::CRReaderBase(const char *message, uint length, bool extern_schema) :
    stream_( auto_ptr<InputStream>( avro::memoryInputStream(reinterpret_cast<const uint8_t*> (message), length)  ) ), decoder_(binaryDecoder()), objectCount_(0), use_external_schema(extern_schema), codec_(NULL_CODEC)
{
    if(use_external_schema){
        readHeaderExternalSchema();
    }
    else{
        readHeader();
    }
}

void CRReaderBase::init()
{
    readerSchema_ = dataSchema_;
    dataDecoder_  = binaryDecoder();
    has_data = readDataBlock();

}

void CRReaderBase::init(const ValidSchema& readerSchema)
{
    readerSchema_ = readerSchema;
    dataDecoder_  = binaryDecoder();
    has_data = readDataBlock();
    if(is_kafka && objectCount_ == 0){
        //no rows? invalid schema?
        has_data = -1;
    }
}

static void drain(InputStream& in)
{
    const uint8_t *p = 0;
    size_t n = 0;
    while (in.next(&p, &n));    
}

char hex(unsigned int x)
{
    return x + (x < 10 ? '0' :  ('a' - 10));
}

std::ostream& operator << (std::ostream& os, const DataFileSync& s)
{
    for (size_t i = 0; i < s.size(); ++i) {
        os << hex(s[i] / 16)  << hex(s[i] % 16) << ' ';
    }
    os << std::endl;
    return os;
}


int CRReaderBase::hasMore()
{  
    if(is_kafka){
        if(objectCount_ == 0){
            return 0;
        }
        else if(objectCount_ > 1){
 	    return -8;
        }
    }
    if(objectCount_ < 0 ){
        return -1;
    }
    if (objectCount_ != 0) {
        return 1;
    }
    if(has_data < 1){
        return has_data;
    }
        
    dataDecoder_->init(*dataStream_);  
    drain(*dataStream_);
    DataFileSync s;
    decoder_->init(*stream_);
    avro::decode(*decoder_, s); 
    if (s != sync_) {
        return -2;
    }
    return readDataBlock();
}

class BoundedInputStream : public InputStream {
    InputStream& in_;
    size_t limit_;

    bool next(const uint8_t** data, size_t* len) {
        if (limit_ != 0 && in_.next(data, len)) {
            if (*len > limit_) {
                in_.backup(*len - limit_);
                *len = limit_;
            }
            limit_ -= *len;
            return true;
        }
        return false;
    }

    void backup(size_t len) {
        in_.backup(len);
        limit_ += len;
    }

    void skip(size_t len) {
        if (len > limit_) {
            len = limit_;
        }
        in_.skip(len);
        limit_ -= len;
    }

    size_t byteCount() const {
        return in_.byteCount();
    }

public:
    BoundedInputStream(InputStream& in, size_t limit) :
        in_(in), limit_(limit) { }
};

auto_ptr<InputStream> boundedInputStream(InputStream& in, size_t limit)
{
    return auto_ptr<InputStream>(new BoundedInputStream(in, limit));
}

int CRReaderBase::readDataBlock()
{ 
   try {

    decoder_->init(*stream_);

    const uint8_t* p = 0;
    size_t n = 0;
    if (! stream_->next(&p, &n)) {
        return 0;
    }  
    stream_->backup(n);
    avro::decode(*decoder_, objectCount_);
    
    if(objectCount_ < 0){
        return -1;
    }
    int64_t byteCount;
    avro::decode(*decoder_, byteCount);

    decoder_->init(*stream_);
    auto_ptr<InputStream> st = boundedInputStream(*stream_, static_cast<size_t>(byteCount));  
    if (codec_ == NULL_CODEC) {
        dataDecoder_->init(*st);
        dataStream_ = st;
    }
    else if (codec_ == SNAPPY_CODEC) {
        compressed_.clear();    
        const uint8_t* data;
        size_t len;
        while (st->next(&data, &len)) {
            compressed_.insert(compressed_.end(), data, data + len); 
        }
        size_t outlen;
        std::string str(compressed_.begin(),compressed_.end());         
        //calculate uncompressed size
        if(!is_kafka){
            if (snappy_uncompressed_length(str.c_str(), str.length()-4, &outlen) != SNAPPY_OK) {
                return -3;
            }
        }
        else if (!snappy_uncompressed_length(str.c_str(), str.length()-4, &outlen) ) {
            return -3;
        }
                
        charPtr_t = boost::shared_array<char>(new char[outlen]);
               
        //uncompress
        if (snappy_uncompress(str.c_str(), str.length()-4, charPtr_t.get(), &outlen) != SNAPPY_OK)
        {
            //error snappy uncompress
            objectCount_ = -1; //error decompressing, no more object to check -> error
            return -5;       
	}
              
        //crc32 check
        uint32_t crc_result;
        boost::crc_32_type  crc;
        crc.process_bytes( charPtr_t.get(), outlen );
        crc_result = __bswap_32(crc.checksum()); 
        
        if (memcmp(&crc_result, str.c_str()+str.length()-4, 4))
        {
            //error crc32 check
            return -6;    
	}
        
        std::auto_ptr<InputStream> in = auto_ptr<InputStream>( avro::memoryInputStream(reinterpret_cast<const uint8_t*> (charPtr_t.get()), outlen)  );
        dataDecoder_->init(*in);
        dataStream_ = in;       
    }
    else {
        //unknown not supported codec
        return -7;
    }
} catch (std::exception& e) {
        return -9;
        }
    return 1;
}

void CRReaderBase::close()
{
}

static string toString(const vector<uint8_t>& v)
{
    string result;
    result.resize(v.size());
    copy(v.begin(), v.end(), result.begin());
    return result;
}

static ValidSchema makeSchema(const vector<uint8_t>& v)
{
    istringstream iss(toString(v));
    ValidSchema vs;
    compileJsonSchema(iss, vs);
    return ValidSchema(vs);
}

void CRReaderBase::readHeader()
{
    decoder_->init(*stream_);      
    Magic m;
    avro::decode(*decoder_, m);
    if (magic != m) {
        throw Exception("Invalid data file. Magic does not match");
    }
    avro::decode(*decoder_, metadata_);         
    Metadata::const_iterator it = metadata_.find(AVRO_SCHEMA_KEY);       
    if (it == metadata_.end()) {
        throw Exception("No schema in metadata");
    }

    dataSchema_ = makeSchema(it->second);
    if (! readerSchema_.root()) {
        readerSchema_ = dataSchema();
    }

    it = metadata_.find(AVRO_CODEC_KEY);
    if (it != metadata_.end() && toString(it->second) == AVRO_SNAPPY_CODEC) {
        codec_ = SNAPPY_CODEC;
    } else {
        codec_ = NULL_CODEC;
        if (it != metadata_.end() && toString(it->second) != AVRO_NULL_CODEC) {
            throw Exception("Unknown codec in data file: " + toString(it->second));
        }
    }
    
    avro::decode(*decoder_, sync_);
}

void CRReaderBase::readHeaderExternalSchema()
{
    decoder_->init(*stream_);
    avro::decode(*decoder_, sync_);     
}
}   // namespace avro

#endif // NAVRO
