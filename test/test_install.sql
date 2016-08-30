-- Create test FlexTable library and 
-- a subset of functions defined in the library for testing purposes
\set libfile '\''`echo ${FLEX_LIBFILE}/flextable_package.so`'\''

-- Create library
CREATE OR REPLACE LIBRARY TestFlexTableLib AS :libfile;

-- Flex Table loaders
CREATE OR REPLACE PARSER TestFJSONParser AS LANGUAGE 'C++' NAME 'FJSONParserFactory' LIBRARY TestFlexTableLib NOT FENCED;
CREATE OR REPLACE PARSER TestFDelimitedParser AS LANGUAGE 'C++' NAME 'FDelimitedParserFactory' LIBRARY TestFlexTableLib NOT FENCED;

-- Functions for handling dict types
CREATE OR REPLACE FUNCTION TestMapToString AS LANGUAGE 'C++' NAME 'MapBinToStringFactory' LIBRARY TestFlexTableLib NOT FENCED;
CREATE OR REPLACE FUNCTION TestMapContainsKey AS LANGUAGE 'C++' NAME 'MapBinContainsKeyFactory' LIBRARY TestFlexTableLib NOT FENCED;
CREATE OR REPLACE TRANSFORM FUNCTION TestMapKeys AS LANGUAGE 'C++' NAME 'MapBinKeysFactory' LIBRARY TestFlexTableLib NOT FENCED;
CREATE OR REPLACE TRANSFORM FUNCTION TestMapValues AS LANGUAGE 'C++' NAME 'MapBinValuesFactory' LIBRARY TestFlexTableLib NOT FENCED;
