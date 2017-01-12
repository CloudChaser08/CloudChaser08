
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

INSERT INTO quest_merged_new SELECT q.accn_id, q.dos_id, q.local_order_code, q.standard_order_code, q.order_name, q.loinc_code, q.local_result_code, q.result_name,
       d.date_of_service, d.date_of_collection, d.diag_code, d.icd_codeset_ind 
   FROM quest q
   INNER JOIN quest_dos_unique d USING (accn_id, dos_id)
   WHERE d.date_of_service like '2014%'
   ORDER BY d.date_of_service;
