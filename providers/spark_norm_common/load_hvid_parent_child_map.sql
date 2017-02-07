-- parent-child mapping to fix parent child hvid bug
DROP TABLE IF EXISTS parent_child_map;
CREATE TABLE parent_child_map (
        hvid         text ENCODE lzo,
        parentId     text ENCODE lzo
        ) DISTKEY(hvid) SORTKEY(hvid);

COPY parent_child_map FROM 's3://healthveritydev/ifishbein/hvid_parentid/hvid_parentid.csv.bz2' CREDENTIALS '' BZIP2 CSV

