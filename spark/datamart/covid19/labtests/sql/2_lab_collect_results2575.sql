SELECT
    accn_id
    ,dos
    ,dos_id
    ,concat(accn_id,'_',dos_id) AS claim_id
    ,lab_code
    ,qbs_payor_cd
    ,insurance_billing_type
    ,local_profile_code
    ,standard_profile_code
    ,profile_name
    ,local_order_code
    ,standard_order_code
    ,order_name
    ,hipaa_zip
    ,hipaa_dob
    ,hipaa_age
    ,gender
    ,acct_id
    ,acct_name
    ,acct_address_1
    ,acct_address_2
    ,acct_city
    ,acct_state
    ,acct_zip
    ,phy_name
    ,npi
    ,market_type
    ,specialty
    ,diagnosis_code
    ,icd_codeset_ind
    ,loinc_code
    ,local_result_code
    ,result_name
    ,result_value_a
    ,units
    ,ref_range_low
    ,ref_range_high
    ,ref_range_alpha
    ,abnormal_ind
    ,hipaa_comment
    ,fasting_indicator
    ,FIRST(batch_id) as batch_id
    ,FIRST(CAST(mod(case when concat(accn_id,'_',dos_id) is null then 0.0 else substring(concat(accn_id,'_',dos_id),1,instr(concat(accn_id,'_',dos_id),'_')-1) end, CAST({nbr_of_buckets} AS INT)) AS INT)) AS claim_bucket_id
    ,FIRST({part_provider}) AS part_provider
    ,FIRST(CONCAT(SUBSTR(dos,1,4),'-',SUBSTR(dos,5,2))) AS part_mth
FROM
    aet2575.HVRequest_output_002575
WHERE
    1 = CASE WHEN {part_provider} = 'quest' THEN 1 ELSE 2 END
    AND UPPER(dos) <> 'DOS'
    AND CONCAT(SUBSTR(dos,1,4),'-',SUBSTR(dos,5,2))  IN ({list_of_part_mth})
GROUP BY -- Remove duplicates based on these columns from aet2575 staging source
    1, 2,  3,  4,  5,  6,  7,  8,  9,  10,
    11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
    21, 22, 23, 24, 25, 26, 27, 28, 29, 30,
    31, 32, 33, 34, 35, 36, 37, 38, 39, 40,
    41
