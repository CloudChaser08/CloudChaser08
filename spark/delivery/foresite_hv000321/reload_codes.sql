DROP TABLE IF EXISTS {foresite_schema}.mkt_def_ndc;
CREATE TABLE {foresite_schema}.mkt_def_ndc AS
SELECT ndc_code, UPPER(nonproprietary_name) AS molecule, UPPER(proprietary_name) AS brand
FROM external_ref_ndc_code
WHERE  LOWER(nonproprietary_name) LIKE '%metformin%'
    OR LOWER(nonproprietary_name) LIKE '%colesevelam%' 
    OR LOWER(nonproprietary_name) LIKE '%sitagliptin%'
    OR LOWER(nonproprietary_name) LIKE '%linagliptin%'
    OR LOWER(nonproprietary_name) LIKE '%saxagliptin%'
    OR LOWER(nonproprietary_name) LIKE '%liraglutide%'  
    OR LOWER(nonproprietary_name) LIKE '%dulaglutide%'
    OR LOWER(nonproprietary_name) LIKE '%exenatide%'
    OR LOWER(nonproprietary_name) LIKE '%albiglutide%'
    OR LOWER(nonproprietary_name) LIKE '%lixisenatide%' 
    OR LOWER(nonproprietary_name) LIKE '%glargine%'
    OR LOWER(nonproprietary_name) LIKE '%insulin aspart%'
    OR LOWER(nonproprietary_name) LIKE '%lispro%'
    OR LOWER(nonproprietary_name) LIKE '%detemir%' 
    OR LOWER(nonproprietary_name) LIKE '%degludec%'
    OR LOWER(nonproprietary_name) LIKE '%insulin%hum%'
    OR LOWER(nonproprietary_name) LIKE '%pioglitazone%'
    OR LOWER(nonproprietary_name) LIKE '%canagliflozin%'
    OR LOWER(nonproprietary_name) LIKE '%dapagliflozin%'
    OR LOWER(nonproprietary_name) LIKE '%empagliflozin%'
    OR LOWER(nonproprietary_name) LIKE '%glimepiride%'
    OR LOWER(nonproprietary_name) LIKE '%glyburide%'
ORDER BY 1,2
    ;

DROP TABLE IF EXISTS {foresite_schema}.mkt_def_diag;
CREATE TABLE {foresite_schema}.mkt_def_diag as
SELECT code, header, long_description
FROM external_ref_icd10_diagnosis
WHERE code LIKE 'E11%'
ORDER BY ordernum
    ;