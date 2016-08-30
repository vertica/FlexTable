-- Drop functions and library
DROP PARSER TestFJSONParser();
DROP PARSER TestFDelimitedParser();
DROP FUNCTION TestMapToString(Long Varbinary);
DROP FUNCTION TestMapContainsKey(Long Varbinary, Varchar);
DROP TRANSFORM FUNCTION TestMapKeys(Long Varbinary);
DROP TRANSFORM FUNCTION TestMapValues(Long Varbinary);

DROP LIBRARY TestFlexTableLib;
