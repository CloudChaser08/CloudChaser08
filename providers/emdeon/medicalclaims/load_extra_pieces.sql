-- Create a table for the extra pieces missing from the matching payload
DROP TABLE IF EXISTS extra_pieces;
CREATE TABLE extra_pieces(
claimId varchar ENCODE lzo,
pcn varchar ENCODE lzo,
groupId varchar ENCODE lzo,
groupName varchar ENCODE lzo)
DISTKEY(claimId) SORTKEY(claimdId);

-- Load extra_pieces into table
copy extra_pieces from :extra_pieces_path credentials :credentials BZIP2 format as JSON 's3://healthveritydev/ifishbein/emdeon_extra_pieces_payloadpaths.json';
