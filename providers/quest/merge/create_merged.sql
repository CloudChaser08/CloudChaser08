DROP TABLE IF EXISTS quest_merged_new;

CREATE TABLE quest_merged_new (
  accn_id text distkey encode lzo,
  dos_id text encode lzo,
  local_order_code text encode lzo,
  standard_order_code text encode lzo,
  order_name text encode lzo,
  loinc_code text encode lzo,
  local_result_code text encode lzo,
  result_name text encode lzo,
  date_of_service text sortkey encode lzo,
  date_of_collection text encode lzo,
  diag_code text encode lzo,
  icd_codeset_ind text encode lzo
);
