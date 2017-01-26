-- Create a table for linking claims to matching payloads
DROP TABLE IF EXISTS allscripts_linking;
CREATE TABLE allscripts_linking (
entityId text ENCODE lzo,
hvJoinKey text ENCODE lzo
)
DISTKEY(hvJoinKey) SORTKEY(hvJoinKey);

-- Load linking data into table
copy allscripts_linking from :input_path credentials :credentials BZIP2 EMPTYASNULL FILLRECORD;
