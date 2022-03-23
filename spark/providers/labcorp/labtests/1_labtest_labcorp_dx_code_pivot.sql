SELECT
   rslt_pvt.specimen_number,
   rslt_pvt.test_name,
   rslt_pvt.test_ordered_code,
   rslt_pvt.diagnosis_code,
   rslt_pvt.diagnosis_code_qual,
   rslt_pvt.diagnosis_code_priority
 
 FROM 
 (
 SELECT
   rslt.specimen_number,
   rslt.test_name,
   rslt.test_ordered_code,
   /* diagnosis_code */
   UPPER
   (
     CASE
       WHEN dx_explode.n = 0 THEN rslt.icd_code_1
       WHEN dx_explode.n = 1 THEN rslt.icd_code_2
       WHEN dx_explode.n = 2 THEN rslt.icd_code_3
       WHEN dx_explode.n = 3 THEN rslt.icd_code_4
       WHEN dx_explode.n = 4 THEN rslt.icd_code_5
       WHEN dx_explode.n = 5 THEN rslt.icd_code_6
     END
   )                                  AS diagnosis_code,
   /* diagnosis_code_qual */
   CASE
     WHEN dx_explode.n = 0 THEN 
       CASE
         WHEN upper(rslt.icd_code_ind_1) = 'I9' THEN '01'
         WHEN upper(rslt.icd_code_ind_1) = 'I10' THEN '02'
         ELSE NULL
       END
     WHEN dx_explode.n = 1 THEN 
       CASE
          WHEN upper(rslt.icd_code_ind_2) = 'I9' THEN '01'
          WHEN upper(rslt.icd_code_ind_2) = 'I10' THEN '02'
          ELSE NULL
       END
     WHEN dx_explode.n = 2 THEN 
       CASE
         WHEN upper(rslt.icd_code_ind_3) = 'I9' THEN '01'
         WHEN upper(rslt.icd_code_ind_3) = 'I10' THEN '02'
         ELSE NULL
       END
     WHEN dx_explode.n = 3 THEN 
       CASE
         WHEN upper(rslt.icd_code_ind_4) = 'I9' THEN '01'
         WHEN upper(rslt.icd_code_ind_4) = 'I10' THEN '02'
         ELSE NULL
       END
     WHEN dx_explode.n = 4 THEN 
       CASE
         WHEN upper(rslt.icd_code_ind_5) = 'I9' THEN '01'
         WHEN upper(rslt.icd_code_ind_5) = 'I10' THEN '02'
         ELSE NULL
       END
     WHEN dx_explode.n = 5 THEN 
       CASE
         WHEN upper(rslt.icd_code_ind_6) = 'I9' THEN '01'
         WHEN upper(rslt.icd_code_ind_6) = 'I10' THEN '02'
         ELSE NULL
       END
     ELSE NULL
   END                                AS diagnosis_code_qual,
   /* diagnosis_code_priority */
   CASE
     WHEN dx_explode.n = 0 THEN '1'
     WHEN dx_explode.n = 1 THEN '2'
     WHEN dx_explode.n = 2 THEN '3'
     WHEN dx_explode.n = 3 THEN '4'
     WHEN dx_explode.n = 4 THEN '5'
     WHEN dx_explode.n = 5 THEN '6'
   END                                AS diagnosis_code_priority 
 FROM labtest_labcorp_txn_result rslt
 --   transactions txn
 --   JOIN results rslt ON txn.specimen_number = rslt.specimen_number
   CROSS JOIN (SELECT EXPLODE(ARRAY(0, 1, 2, 3, 4, 5)) AS n)  dx_explode
 WHERE
 --   EXISTS(SELECT 1 FROM transactions txn WHERE txn.specimen_number = rslt.specimen_number)
 --   AND 
   ---------- Diagnosis code explosion
     (
       ARRAY (
         rslt.icd_code_1, 
         rslt.icd_code_2,
         rslt.icd_code_3,
         rslt.icd_code_4,
         rslt.icd_code_5,
         rslt.icd_code_6
       ) [dx_explode.n] IS NOT NULL
     )
   )rslt_pvt
   GROUP BY 1, 2, 3, 4, 5, 6
