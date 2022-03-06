SELECT 
    claim_id,
    hvid,
    created,
    model_version,
    data_set,
    data_feed,
    data_vendor,
    patient_gender,
    patient_year_of_birth,
    date_service, 
    transaction_code_vendor,
    ndc_code,
    dispensed_quantity,
    days_supply,
    ----- Added 2022-03-01
    prov_prescribing_std_taxonomy,
    -- will add after aproval    
    --submitted_gross_due,
    MD5(prov_dispensing_id) AS prov_dispensing_id,    
    prov_dispensing_qual,
    MD5(prov_prescribing_id) AS prov_prescribing_id,
    prov_prescribing_qual,
    logical_delete_reason,
    stg_file_date,
    'inovalon' as part_provider,
    part_best_date

--------------------------- Get the PROD Table (currently there is no table)
---FROM _pharmacyclaims_nb
FROM _temp_pharmacyclaims_hist
    WHERE FALSE
    --part_provider = 'inovalon'
    ----- Look current month and 3 month back
    AND part_best_date >= ADD_MONTHS(TRUNC(CAST('{VDR_FILE_DT}' AS DATE), 'MONTH'), -2)
    AND part_best_date <= ADD_MONTHS(TRUNC(CAST('{VDR_FILE_DT}' AS DATE), 'MONTH'),  0)
--AND EXISTS
--(
----------------------------- change to the PROD Table
--  --SELECT 1 FROM darch._mom_cohort mom WHERE hivd = mom.hvid
-- ----------- Cohort has both mom and baby     TEMPORARY
--  SELECT 1 FROM _temp_mom_cohort mom WHERE hvid = mom.hvid
--)
