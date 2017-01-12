DROP TABLE IF EXISTS quest_dos_unique;
create table quest_dos_unique (
  ACCN_ID text distkey sortkey encode lzo,
  DATE_OF_SERVICE text encode lzo,
  DOS_ID text encode lzo,
  DATE_OF_COLLECTION text encode lzo,
  DIAG_CODE text encode lzo,
  ICD_CODESET_IND text encode lzo
);
