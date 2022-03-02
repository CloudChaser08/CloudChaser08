SELECT DISTINCT gen_ref_cd, gen_ref_desc
FROM
(
SELECT '^TNP124' AS gen_ref_cd, 'TEST NOT PERFORMED' AS gen_ref_desc UNION ALL
SELECT 'ADD^TNP167' AS gen_ref_cd, 'TEST NOT PERFORMED' AS gen_ref_desc UNION ALL
SELECT 'CFUmL' AS gen_ref_cd, 'CFU/mL' AS gen_ref_desc UNION ALL
SELECT 'CONSISTENT' AS gen_ref_cd, 'CONSISTENT' AS gen_ref_desc UNION ALL
SELECT 'CULTURE INDICATED' AS gen_ref_cd, 'CULTURE INDICATED' AS gen_ref_desc UNION ALL
SELECT 'DETECTED' AS gen_ref_cd, 'DETECTED' AS gen_ref_desc UNION ALL
SELECT 'DETECTED (A)' AS gen_ref_cd, 'DETECTED' AS gen_ref_desc UNION ALL
SELECT 'DETECTED (A1)' AS gen_ref_cd, 'DETECTED' AS gen_ref_desc UNION ALL
SELECT 'DETECTED ABN' AS gen_ref_cd, 'DETECTED' AS gen_ref_desc UNION ALL
SELECT 'DETECTED, NOT QUANTIFIED' AS gen_ref_cd, 'DETECTED' AS gen_ref_desc UNION ALL
SELECT 'DTEL^TNP1003' AS gen_ref_cd, 'TEST NOT PERFORMED' AS gen_ref_desc UNION ALL
SELECT 'EQUIOC' AS gen_ref_cd, 'EQUIVOCAL' AS gen_ref_desc UNION ALL
SELECT 'EQUIVOCAL' AS gen_ref_cd, 'EQUIVOCAL' AS gen_ref_desc UNION ALL
SELECT 'FINAL RSLT: NEGATIVE' AS gen_ref_cd, 'NEGATIVE' AS gen_ref_desc UNION ALL
SELECT 'INCONCLUSIVE' AS gen_ref_cd, 'INCONCLUSIVE' AS gen_ref_desc UNION ALL
SELECT 'INCONSISTENT' AS gen_ref_cd, 'INCONSISTENT' AS gen_ref_desc UNION ALL
SELECT 'INDETERMINANT' AS gen_ref_cd, 'INDETERMINATE' AS gen_ref_desc UNION ALL
SELECT 'INDETERMINAT' AS gen_ref_cd, 'INDETERMINATE' AS gen_ref_desc UNION ALL
SELECT 'INDETERMINATE' AS gen_ref_cd, 'INDETERMINATE' AS gen_ref_desc UNION ALL
SELECT 'INDICATED' AS gen_ref_cd, 'INDICATED' AS gen_ref_desc UNION ALL
SELECT 'ISOLATED' AS gen_ref_cd, 'ISOLATED' AS gen_ref_desc UNION ALL
SELECT 'NEG' AS gen_ref_cd, 'NEGATIVE' AS gen_ref_desc UNION ALL
SELECT 'NEG/' AS gen_ref_cd, 'NEGATIVE' AS gen_ref_desc UNION ALL
SELECT 'NEGA CONF' AS gen_ref_cd, 'NEGATIVE CONFIRMED' AS gen_ref_desc UNION ALL
SELECT 'NEGATI' AS gen_ref_cd, 'NEGATIVE' AS gen_ref_desc UNION ALL
SELECT 'NEGATIVE' AS gen_ref_cd, 'NEGATIVE' AS gen_ref_desc UNION ALL
SELECT 'NEGATIVE CONFIRMED' AS gen_ref_cd, 'NEGATIVE CONFIRMED' AS gen_ref_desc UNION ALL
SELECT 'NO CULTURE INDICATED' AS gen_ref_cd, 'NO CULTURE INDICATED' AS gen_ref_desc UNION ALL
SELECT 'NO VARIANT DETECTED' AS gen_ref_cd, 'NO VARIANT DETECTED' AS gen_ref_desc UNION ALL
SELECT 'NON-DETECTED' AS gen_ref_cd, 'NOT DETECTED' AS gen_ref_desc UNION ALL
SELECT 'NON-REACTIVE' AS gen_ref_cd, 'NON-REACTIVE' AS gen_ref_desc UNION ALL
SELECT 'None Detected' AS gen_ref_cd, 'NOT DETECTED' AS gen_ref_desc UNION ALL
SELECT 'Nonreactive' AS gen_ref_cd, 'NON-REACTIVE' AS gen_ref_desc UNION ALL
SELECT 'Not Detected' AS gen_ref_cd, 'NOT DETECTED' AS gen_ref_desc UNION ALL
SELECT 'Not Indicated' AS gen_ref_cd, 'NOT INDICATED' AS gen_ref_desc UNION ALL
SELECT 'NOT INTERPRETED~DNR' AS gen_ref_cd, 'DO NOT REPORT' AS gen_ref_desc UNION ALL
SELECT 'NOT ISOLATED' AS gen_ref_cd, 'NOT ISOLATED' AS gen_ref_desc UNION ALL
SELECT 'POS' AS gen_ref_cd, 'POSITIVE' AS gen_ref_desc UNION ALL
SELECT 'POS/' AS gen_ref_cd, 'POSITIVE' AS gen_ref_desc UNION ALL
SELECT 'POSITIVE' AS gen_ref_cd, 'POSITIVE' AS gen_ref_desc UNION ALL
SELECT 'REACTIVE' AS gen_ref_cd, 'REACTIVE' AS gen_ref_desc UNION ALL
SELECT 'TA/DNR' AS gen_ref_cd, 'DO NOT REPORT' AS gen_ref_desc UNION ALL
SELECT 'NON REACTIVE' AS gen_ref_cd, 'NON-REACTIVE' AS gen_ref_desc UNION ALL -- new
SELECT 'NOT REPORTED' AS gen_ref_cd, 'DO NOT REPORT' AS gen_ref_desc UNION ALL -- new
SELECT 'UNDETECTED' AS gen_ref_cd, 'NOT DETECTED' AS gen_ref_desc UNION ALL -- new
SELECT 'NG' AS gen_ref_cd, 'NOT GIVEN'    AS gen_ref_desc UNION ALL -- new
SELECT 'DETECTED*' AS gen_ref_cd, 'DETECTED' AS gen_ref_desc UNION ALL -- new
SELECT 'NT' AS gen_ref_cd, 'TEST NOT PERFORMED' AS gen_ref_desc  -- new
---- New value from transaction table-------
UNION ALL
SELECT a.result_value   AS gen_ref_cd ,
      CASE
       WHEN UPPER(a.result_value) LIKE 'TNP%'            THEN 'TEST NOT PERFORMED'
       WHEN UPPER(a.result_value) LIKE '%NOT PERFORMED%' THEN 'TEST NOT PERFORMED'
       WHEN UPPER(a.result_value) LIKE '%NOT GIVEN%'     THEN 'NOT GIVEN'
       WHEN UPPER(a.result_value) LIKE 'DNR%'            THEN 'DO NOT REPORT'
       WHEN UPPER(a.result_value) LIKE '%DO NOT REPORT%' THEN 'DO NOT REPORT'
     END                     AS gen_ref_desc
FROM (
    SELECT DISTINCT UPPER(result_value) AS result_value
    FROM order_result
    WHERE (
          UPPER(result_value) LIKE 'TNP%'
       OR UPPER(result_value) LIKE '%NOT PERFORMED%'
       OR UPPER(result_value) LIKE '%NOT GIVEN%'
       OR UPPER(result_value) LIKE 'DNR%'
       OR UPPER(result_value) LIKE '%DO NOT REPORT%'
      )
    ) a
)
--limit 1