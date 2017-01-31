DROP TABLE IF EXISTS quest_lab;
create table quest_lab (
  ACCN_ID text distkey sortkey encode lzo,
  DOS_ID text encode lzo,
  LAB_ID text encode lzo
);

COPY quest_lab FROM :input_path CREDENTIALS :credentials DELIMITER '\t' IGNOREHEADER 1 ACCEPTINVCHARS MAXERROR 500 GZIP;
