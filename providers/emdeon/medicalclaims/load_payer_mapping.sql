-- Create a table for the payer_vendor_id to payer_name and payer_parent name mapping
DROP TABLE IF EXISTS payer_mapping;
CREATE TABLE payer_mapping (payer_vendor_id text ENCODE lzo,
payer_name text ENCODE lzo,
payer_parent_name text ENCODE lzo) DISTSTYLE ALL SORTKEY(payer_vendor_id);

-- Load matching payload data into table
copy payer_mapping from 's3://salusv/reference/emdeon/payer_id_list.20150727.csv' credentials :credentials CSV;
