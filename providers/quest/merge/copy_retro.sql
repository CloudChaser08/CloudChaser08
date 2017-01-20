DROP TABLE IF EXISTS quest_dos;
create table quest_dos (
  ACCN_ID text distkey sortkey encode lzo,
  DATE_OF_SERVICE text encode lzo,
  DOS_ID text encode lzo,
  DATE_OF_COLLECTION text encode lzo,
  DIAG_CODE text encode lzo,
  ICD_CODESET_IND text encode lzo
);

COPY quest_dos FROM :input_path CREDENTIALS :credentials DELIMITER '\t' IGNOREHEADER 1 ACCEPTINVCHARS MAXERROR 500 GZIP;
