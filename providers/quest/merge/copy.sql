DROP TABLE IF EXISTS quest;
CREATE TABLE quest (
  ACCN_ID text distkey sortkey encode lzo,
  DOS_ID text encode lzo,
  LOCAL_ORDER_CODE text encode lzo,
  STANDARD_ORDER_CODE text encode lzo,
  ORDER_NAME text encode lzo,
  LOINC_CODE text encode lzo,
  LOCAL_RESULT_CODE text encode lzo,
  RESULT_NAME text encode lzo
);

COPY quest FROM :input_path CREDENTIALS :credentials DELIMITER '\t' IGNOREHEADER 1 ACCEPTINVCHARS MAXERROR 500 GZIP

