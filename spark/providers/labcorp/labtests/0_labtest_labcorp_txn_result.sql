SELECT
   rslt.specimen_number, --- moved to top rslt.specimen_number intetionly 
   SPLIT(rslt.input_file_name, '/')[SIZE(SPLIT(rslt.input_file_name, '/')) - 1]  AS data_set,
   ---------------------------------------------------------------------
   ------ Data from transaction
   ---------------------------------------------------------------------
   txn.patient_sex,
   txn.patient_state,
   txn.patient_zip5,
   txn.patient_id,
   txn.hvJoinKey,
   CASE WHEN txn.specimen_number IS NOT NULL THEN 'YES' ELSE NULL END AS exist_in_txn,
   ---------------------------------------------------------------------  
   rslt.test_name,
   rslt.test_number,
   rslt.loinc_code,
   rslt.normal_dec_low,
   rslt.normal_dec_high,
   rslt.result_dec,
   rslt.result_abn_code,
   rslt.result_abbrv,
   rslt.result_date,
   rslt.pat_dos,
   rslt.perf_lab_code,
   rslt.rslt_comments,
   rslt.test_ordered_code,
   rslt.test_ordered_name,
   rslt.npi,
   rslt.specialty_code,
   rslt.report_zip,
   rslt.icd_code_1,
   rslt.icd_code_ind_1,
   rslt.icd_code_2,
   rslt.icd_code_ind_2,
   rslt.icd_code_3,
   rslt.icd_code_ind_3,
   rslt.icd_code_4,
   rslt.icd_code_ind_4,
   rslt.icd_code_5,
   rslt.icd_code_ind_5,
   rslt.icd_code_6,
   rslt.icd_code_ind_6
 
 FROM results rslt
 LEFT OUTER JOIN txn ON rslt.specimen_number = txn.specimen_number
 WHERE UPPER(rslt.specimen_number) <> 'SPECIMEN_NUMBER'
