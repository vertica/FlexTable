------------------------------------------------------------------
-- Flex Table Analyses
--   Sneak Peak
------------------------------------------------------------------

-- This short guide will let you try the salient features, but we
-- strongly encourage you to read the documentation for a better
-- understanding of both the power and the limitations of this
-- feature.

-- Load the data (in different formats) and start querying
CREATE FLEX TABLE mountains();

INSERT INTO mountains(name, type, hike_safety) VALUES('Mt St Helens', 'volcano', 15.4);

COPY mountains FROM STDIN PARSER TestFJSONParser();
{ "name": "Everest", "type":"mountain", "height": 29029, "hike_safety": 34.1 }
\.

COPY mountains FROM STDIN PARSER TestFDelimitedParser();
name|type|height
Matterhorn|mountain|4478
\.

SELECT name, type, height, hike_safety FROM mountains;

-- Inspect the contents of the map in human-readable format
SELECT TestMapToString(__raw__) FROM mountains;

-- Check if maps contains a certain key
SELECT name, TestMapContainsKey(__raw__, 'height') FROM mountains;

-- List the keys in every map
SELECT TestMapKeys(__raw__) OVER(PARTITION BEST) FROM mountains;

-- List the values in every map
SELECT TestMapValues(__raw__) OVER(PARTITION BEST) FROM mountains;

-- Find the unique keys in the maps, and guess their data types
SELECT compute_flextable_keys('mountains');
SELECT * FROM mountains_keys;

-- Build a view on the flex table with columns cast to the guessed types
SELECT build_flextable_view('mountains');
\dv mountains_view
SELECT * FROM mountains_view;

-- Materialize the two most frequently occurring keys in mountains_keys.
-- This will create two real columns with the guessed data types in mountains_keys.
-- Queries on real columns have better performance.
SELECT materialize_flextable_columns('mountains', 2);
SELECT * FROM materialize_flextable_columns_results WHERE table_name = 'mountains';
\d mountains

SELECT column_id, column_name, data_type, data_type_id, data_type_length, column_default FROM v_catalog.columns WHERE table_name = 'mountains';
-- Clean Up
DROP TABLE mountains;
