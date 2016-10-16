-- Create a table for the extra pieces missing from the matching payload
DROP TABLE IF EXISTS extra_pieces;
CREATE TABLE extra_pieces
(hvJoinKey varchar ENCODE lzo,
RXNumber text ENCODE lzo)
DISTKEY(hvJoinKey) SORTKEY(hvJoinKey);

-- Load extra_pieces into table
copy extra_pieces from :extra_pieces_path credentials :credentials BZIP2 format as JSON 's3://healthveritydev/ifishbein/esi_extra_pieces_payloadpaths.json';
