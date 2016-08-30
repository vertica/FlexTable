#ifndef NAVRO

#ifndef avro_DatumParsers_hh__
#define avro_DatumParsers_hh__

#include "Vertica.h"
#include "StringParsers.h"
#include "avro/Generic.hh"
#include "avro/Types.hh"
#include <boost/lexical_cast.hpp>
#include <string>
#include <boost/spirit/include/qi.hpp>
#include <boost/spirit/include/karma.hpp>
#include <utility>


std::string IntToString(int value)
{
   std::string output;
   boost::spirit::karma::generate(std::back_inserter(output), boost::spirit::int_, value);
   return output;
}

std::string LongToString(long value)
{
   std::string output;
   boost::spirit::karma::generate(std::back_inserter(output), boost::spirit::long_, value);
   return output;
}

std::string FloatToString(float value)
{
   std::string output;
   boost::spirit::karma::generate(std::back_inserter(output), boost::spirit::float_, value);
   return output;
}

std::string DoubleToString(double value)
{
   std::string output;
   boost::spirit::karma::generate(std::back_inserter(output), boost::spirit::double_, value);
   return output;
}
/**
 * Parse an avro bool to a boolean
 */
bool parseBool(const avro::GenericDatum& d, size_t colNum, vbool& target,
		const VerticaType& type) {
	if (d.type() != avro::AVRO_BOOL)
		return false;
	try{
		target = (d.value<bool> () ? vbool_true : vbool_false);
		return true;
	} catch (...) {
		log("Cannot parse bool column: %zu", colNum);
		return false;
	}
}

/**
 * Parse an avro int or long to an integer
 */
bool parseInt(const avro::GenericDatum& d, size_t colNum, vint& target,
		const VerticaType& type) {

	try{
		if (d.type() == avro::AVRO_INT)
			target = (long) d.value<int32_t> ();
		else if (d.type() == avro::AVRO_LONG)
			target = (long) d.value<int64_t> ();
		else
			return false;
		return true;
	} catch (...) {
		log("Cannot parse int column: %zu", colNum);
		return false;
	}
}

/**
 * Parse an avro float or double to a floating-point number
 */
bool parseFloat(const avro::GenericDatum& d, size_t colNum, vfloat& target,
		const VerticaType& type) {
	try{
		if (d.type() == avro::AVRO_FLOAT)
			target = (double) d.value<float> ();
		else if (d.type() == avro::AVRO_DOUBLE)
			target = d.value<double> ();
		else
			return false;
		return true;
	} catch (...) {
		log("Cannot parse float column: %zu", colNum);
		return false;
	}
}

/**
 * Parse an avro string to a VARCHAR field
 */
bool parseVarchar(const avro::GenericDatum& d, size_t colNum, VString& target,
		const VerticaType& type, bool enforcelength) {
	if (d.type() != avro::AVRO_STRING)
		return false;

	try {
	    std::string val = d.value<std::string> ();
	    ssize_t avro_field_size = val.length();
	    ssize_t column_size = (ssize_t)type.getStringLength();
        
            if(enforcelength && avro_field_size > column_size){
                return false;
            }

            ssize_t maxDataLen = std::min(column_size, avro_field_size);

	    target.copy(val.c_str(), maxDataLen);
	    return true;
       } catch (...) {
		log("Cannot parse column: %s", d.value<std::string> ().c_str());
		return false;
	}
}

/**
 * Parse an avro byte vector to a varbinary field
 */
bool parseVarbinary(const avro::GenericDatum& d, size_t colNum,
		VString& target, const VerticaType& type) {
	// Don't attempt to parse binary.
	// Just copy it.
	try{
		if (d.type() == avro::AVRO_BYTES) {
			std::vector < uint8_t > val = d.value<std::vector<uint8_t> > ();
			target.copy((char*) &val[0], val.size());
			return true;
		} else if (d.type() == avro::AVRO_FIXED) {
			avro::GenericFixed datumFixed = d.value<avro::GenericFixed> ();
			target.copy((char*) &datumFixed.value()[0], datumFixed.value().size());
			return true;
		} else {
			return false;
		}
	} catch (...) {
		log("Cannot parse Varbinary column: %zu", colNum);
		return false;
	}
}

bool parseNumeric(const avro::GenericDatum& d, size_t colNum, VNumeric& target,
		const VerticaType& type) {
	try{
		std::string val;
        	if(d.type() == avro::AVRO_FLOAT){
	    		val = FloatToString(d.value<float> ());
			}
			else if(d.type() == avro::AVRO_INT){
	    		val = IntToString(d.value<int32_t> ());
			}
			else if(d.type() == avro::AVRO_LONG){
	    		val = LongToString(d.value<int64_t> ());
			}
			else if(d.type() == avro::AVRO_DOUBLE){
	    		val = DoubleToString(d.value<double> ());    	       
			}
			else{
	    		return false;
			}
		std::vector<char> str(val.size() + 1);
		std::copy(val.begin(), val.end(), str.begin());
		str.push_back('\0');
		return VNumeric::charToNumeric(&str[0], type, target);
	} catch (...) {
		log("Cannot parse Numeric column: %zu", colNum);
		return false;
	}
}

/**
 * Parse an avro string to a Vertica Date, according to the specified format.
 * `format` is a format-string as passed to the default Vertica
 * string->date parsing function.
 */
bool parseDate(const avro::GenericDatum& d, size_t colNum, DateADT& target,
		const VerticaType& type) {
	if (d.type() != avro::AVRO_STRING)
		return false;

	try {
		std::string val = d.value<std::string> ();
		target = dateIn(val.c_str(), true);
		return true;
	} catch (...) {
		log("Invalid date format: %s", d.value<std::string> ().c_str());
		return false;
	}
}

/**
 * Parse an avro string to a Vertica Time, according to the specified format.
 * `format` is a format-string as passed to the default Vertica
 * string->date parsing function.
 */
bool parseTime(const avro::GenericDatum& d, size_t colNum, TimeADT& target,
		const VerticaType& type) {
	
	if (d.type() != avro::AVRO_STRING)
		return false;

	try {
		std::string val = d.value<std::string> ();
		target = timeIn(val.c_str(), type.getTypeMod(), true);
		return true;
	} catch (...) {
		log("Invalid Time format: %s", d.value<std::string> ().c_str());
		return false;
	}
}

/**
 * Parse an avro string to a Vertica Timestamp, according to the specified format.
 * `format` is a format-string as passed to the default Vertica
 * string->date parsing function.
 */
bool parseTimestamp(const avro::GenericDatum& d, size_t colNum,
		Timestamp& target, const VerticaType& type) {
	if (d.type() != avro::AVRO_STRING)
		return false;
	try {
		std::string val = d.value<std::string> ();
		target = timestampIn(val.c_str(), type.getTypeMod(), true);
		return true;
	} catch (...) {
		log("Invalid Timestamp format: %s", d.value<std::string> ().c_str());
		return false;
	}
}

/**
 * Parse an avro string to a Vertica TimeTz, according to the specified format.
 * `format` is a format-string as passed to the default Vertica
 * string->date parsing function.
 */
bool parseTimeTz(const avro::GenericDatum& d, size_t colNum, TimeTzADT& target,
		const VerticaType& type) {
	if (d.type() != avro::AVRO_STRING)
		return false;
	try {
		std::string val = d.value<std::string> ();
		target = timetzIn(val.c_str(), type.getTypeMod(), true);
		return true;
	} catch (...) {
		log("Invalid TimeTz format: %s", d.value<std::string> ().c_str());
		return false;
	}
}

/**
 * Parse an avro string to a Vertica TimestampTz, according to the specified format.
 * `format` is a format-string as passed to the default Vertica
 * string->date parsing function.
 */
bool parseTimestampTz(const avro::GenericDatum& d, size_t colNum,
		TimestampTz &target, const VerticaType &type) {
	if (d.type() != avro::AVRO_STRING)
		return false;
	try {
		std::string val = d.value<std::string> ();
		target = timestamptzIn(val.c_str(), type.getTypeMod(), true);
		return true;
	} catch (...) {
		log("Invalid TimestampTz format: %s", d.value<std::string> ().c_str());
		return false;
	}
}

/**
 * Parse an interval expression (from an avro string) to a Vertica Interval
 */
bool parseInterval(const avro::GenericDatum& d, size_t colNum,
		Interval &target, const VerticaType &type) {
	if (d.type() != avro::AVRO_STRING)
		return false;
	try {
		std::string val = d.value<std::string> ();
		target = intervalIn(val.c_str(), type.getTypeMod(), true);
		return true;
	} catch (...) {
		log("Cannot parse Interval column: %zu", colNum);
		return false;
	}
}

//Receives an avro generic datum and parse it to string (that will be inserted as value in the vmap)
std::string parseAvroFieldToString(const avro::GenericDatum& d) {
        
	std::string parsedString;
	avro::Type datumType = d.type();
	if (datumType == avro::AVRO_BOOL) {
		parsedString = (d.value<bool> () ? "true" : "false");
	}
	else if (datumType == avro::AVRO_INT) {
		parsedString = IntToString(d.value<int32_t> ());
	} else if (datumType == avro::AVRO_LONG) {
		parsedString = LongToString(d.value<int64_t> ());
	} else if (datumType == avro::AVRO_FLOAT) {
		parsedString = FloatToString(d.value<float> ());
	} else if (datumType == avro::AVRO_DOUBLE) {
		parsedString = DoubleToString(d.value<double> ());
	} else if (datumType == avro::AVRO_STRING) {
		parsedString = d.value<std::string> ();
	} else if (datumType == avro::AVRO_BYTES) {
		std::string parsedString;
		std::vector < uint8_t > val = d.value<std::vector<uint8_t> > ();
		parsedString.copy((char*) &val[0], val.size());
	} else if (datumType == avro::AVRO_FIXED) {
		avro::GenericFixed datumFixed = d.value<avro::GenericFixed> ();
		std::string fixedString(datumFixed.value().begin(), datumFixed.value().end());
		parsedString = fixedString;
	}
	return parsedString;
}


/**
 * Parses avro datum according to the column's type
 */
bool parseAvroFieldToType(const avro::GenericDatum& d, size_t colNum,
		const VerticaType &type, ::Vertica::StreamWriter *writer, bool enforcelength) {
	bool retVal;

	if (d.type() == avro::AVRO_NULL) {
		writer->setNull(colNum);
		return true;
	}

	switch (type.getTypeOid()) {
	case BoolOID: {
		vbool val;
		retVal = parseBool(d, colNum, val, type);
		writer->setBool(colNum, val);
		break;

	}
	case Int8OID: {
		vint val;
		retVal = parseInt(d, colNum, val, type);
		writer->setInt(colNum, val);
		break;
	}
	case Float8OID: {
		vfloat val;
		retVal = parseFloat(d, colNum, val, type);
		writer->setFloat(colNum, val);
		break;
	}
	case CharOID: {
		VString val = writer->getStringRef(colNum);
		retVal = parseVarchar(d, colNum, val, type, enforcelength);
		break;
	}
	case LongVarcharOID:
	case VarcharOID: {
		VString val = writer->getStringRef(colNum);
		retVal = parseVarchar(d, colNum, val, type, enforcelength);
		break;
	}
	case NumericOID: {
		VNumeric val = writer->getNumericRef(colNum);
		retVal = parseNumeric(d, colNum, val, type);
		break;
	}
	case DateOID: {
		DateADT val;
		retVal = parseDate(d, colNum, val, type);
		writer->setDate(colNum, val);
		break;
	}
	case TimeOID: {
		TimeADT val;
		retVal = parseTime(d, colNum, val, type);
		writer->setTime(colNum, val);
		break;
	}
	case TimestampOID: {
		Timestamp val;
		retVal = parseTimestamp(d, colNum, val, type);
		writer->setTimestamp(colNum, val);
		break;
	}
	case TimestampTzOID: {
		TimestampTz val;
		retVal = parseTimestampTz(d, colNum, val, type);
		writer->setTimestampTz(colNum, val);
		break;
	}
	case BinaryOID:
	case LongVarbinaryOID:
	case VarbinaryOID: {
		VString val = writer->getStringRef(colNum);
		retVal = parseVarbinary(d, colNum, val, type);
		break;
	}
	case IntervalOID: {
		Interval val;
		retVal = parseInterval(d, colNum, val, type);
		writer->setInterval(colNum, val);
		break;
	}
	case TimeTzOID: {
		TimeTzADT val;
		retVal = parseTimeTz(d, colNum, val, type);
		writer->setTimeTz(colNum, val);
		break;
	}
	case IntervalYMOID:
	default:
		vt_report_error(0, "Error, unrecognized type: '%s'", type.getTypeStr());
		retVal = false;
	}
	return retVal;
}

#endif

#endif // NAVRO
