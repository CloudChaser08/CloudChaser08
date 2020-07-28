SELECT
    test.part_provider,
    test.test_ordered_name,
    test.result_name,
    test.result,
    test.result_comments,
    test.hv_method_flag,
    CASE WHEN hv_method_flag IN (1,2) THEN
        CASE WHEN test_ordered_std_id IN ('39504','39504X','0039504','0039728','TH99') THEN 'COVID Antibody'
             WHEN procedure_code IN ('86328','86769') THEN 'COVID Antibody'
             WHEN loinc_code IN ('945634')  THEN 'COVID Antibody'
             WHEN LOWER(test_ordered_name) LIKE '%igg%' or LOWER(test_ordered_name) LIKE '%anti%body%' THEN 'COVID Antibody'
             WHEN LOWER(result_name) LIKE '%igg%' or LOWER(result_name) LIKE '%anti%body%'  THEN 'COVID Antibody'
             ELSE 'COVID Diagnostic Test'
        END
    ELSE 'NOTValid Diagnostic Test'
    END AS hv_test_flag,
    CASE WHEN hv_method_flag = 1 THEN
        CASE
            WHEN LOWER(result) LIKE 'pos%'                 THEN '01 Positive'
            WHEN LOWER(result) LIKE 'detect%'              THEN '01 Positive'
            WHEN LOWER(result) LIKE 'abn%'                 THEN '01 Positive'
            WHEN LOWER(result) IN ('yes','detcted')        THEN '01 Positive'
            WHEN LOWER(result) LIKE '%rna detect%'         THEN '01 Positive'
            WHEN LOWER(result) LIKE 'presu%pos%'           THEN '02 Presumed Positive'
            WHEN LOWER(result) LIKE 'negative%'            THEN '03 Negative'
            WHEN LOWER(result) LIKE 'not detect%'          THEN '03 Negative'
            WHEN LOWER(result) LIKE 'none detect%'         THEN '03 Negative'
            WHEN LOWER(result) IN ('neg','nd','no','none') THEN '03 Negative'
            WHEN LOWER(result) LIKE 'inconc%'              THEN '04 Inconclusive'
            WHEN LOWER(result) LIKE 'equiv%'               THEN '04 Inconclusive'
            WHEN LOWER(result) LIKE 'unkno%'               THEN '04 Inconclusive'
            WHEN LOWER(result) LIKE 'undeter%'             THEN '04 Inconclusive'
            WHEN LOWER(result) IN ('inc')                  THEN '04 Inconclusive'
            ELSE NULL
        END
    WHEN hv_method_flag = 2 THEN
        CASE
            WHEN REGEXP_REPLACE(LOWER(result_comments),'[^a-z]+','') LIKE '%thetestispresu%pos%'       THEN '02 Presumed Positive'
            WHEN REGEXP_REPLACE(LOWER(result_comments),'[^a-z]+','') LIKE '%thespecimenispresu%pos%'   THEN '02 Presumed Positive'
            WHEN REGEXP_REPLACE(LOWER(result_comments),'[^a-z]+','') LIKE '%overallresultpresu%pos%'   THEN '02 Presumed Positive'
            WHEN REGEXP_REPLACE(LOWER(result_comments),'[^a-z]+','') LIKE '%sarscovabiggpresu%pos%'    THEN '02 Presumed Positive'
            WHEN REGEXP_REPLACE(LOWER(result_comments),'[^a-z]+','') LIKE 'initialtestingispresu%pos%' THEN '02 Presumed Positive'
            WHEN REGEXP_REPLACE(LOWER(result_comments),'[^a-z]+','') LIKE '%thetestispos%'             THEN '01 Positive'
            WHEN REGEXP_REPLACE(LOWER(result_comments),'[^a-z]+','') LIKE '%thespecimenispos%'         THEN '01 Positive'
            WHEN REGEXP_REPLACE(LOWER(result_comments),'[^a-z]+','') LIKE '%sarscovrnartpcrabndetect%' THEN '01 Positive'
            WHEN REGEXP_REPLACE(LOWER(result_comments),'[^a-z]+','') LIKE '%sarscovrnadetect%'         THEN '01 Positive'
            WHEN REGEXP_REPLACE(LOWER(result_comments),'[^a-z]+','') LIKE '%overallresultdetect%'      THEN '01 Positive'
            WHEN REGEXP_REPLACE(LOWER(result_comments),'[^a-z]+','') LIKE 'detect%'                    THEN '01 Positive'
            WHEN REGEXP_REPLACE(LOWER(result_comments),'[^a-z]+','') LIKE '%sarscovabiggpositive%'     THEN '01 Positive'
            WHEN REGEXP_REPLACE(LOWER(result_comments),'[^a-z]+','') LIKE '%thetestisnega%'            THEN '03 Negative'
            WHEN REGEXP_REPLACE(LOWER(result_comments),'[^a-z]+','') LIKE '%thespecimenisnega%'        THEN '03 Negative'
            WHEN REGEXP_REPLACE(LOWER(result_comments),'[^a-z]+','') LIKE 'notdetect%'                 THEN '03 Negative'
            WHEN REGEXP_REPLACE(LOWER(result_comments),'[^a-z]+','') LIKE '%overallresultnotdetect%'   THEN '03 Negative'
            WHEN REGEXP_REPLACE(LOWER(result_comments),'[^a-z]+','') LIKE '%sarscovrnartpcrnotdetect%' THEN '03 Negative'
            WHEN REGEXP_REPLACE(LOWER(result_comments),'[^a-z]+','') LIKE '%sarscovrnanotdetect%'      THEN '03 Negative'
            WHEN REGEXP_REPLACE(LOWER(result_comments),'[^a-z]+','') LIKE '%sarscovabiggnegative%'     THEN '03 Negative'
            WHEN REGEXP_REPLACE(LOWER(result_comments),'[^a-z]+','') LIKE '%sarscovabiggequiv%'        THEN '04 Inconclusive'
            WHEN REGEXP_REPLACE(LOWER(result_comments),'[^a-z]+','') LIKE '%sarscovabigginconc%'       THEN '04 Inconclusive'
            WHEN REGEXP_REPLACE(LOWER(result_comments),'[^a-z]+','') LIKE 'aninconclusiveresultmeans%' THEN '04 Inconclusive'
            WHEN REGEXP_REPLACE(LOWER(result_comments),'[^a-z]+','') LIKE '%sarscovrnainconc%'         THEN '04 Inconclusive'
            WHEN REGEXP_REPLACE(LOWER(result_comments),'[^a-z]+','') LIKE 'inconc%'                    THEN '04 Inconclusive'
            WHEN REGEXP_REPLACE(LOWER(result_comments),'[^a-z]+','') LIKE 'aninconc%'                  THEN '04 Inconclusive'
            ELSE NULL
        END
    ELSE NULL
    END AS hv_result_flag
FROM
    _temp_lab_covid_tests_cleansed test
WHERE
    hv_method_flag IN (1,2)
GROUP BY
    test.part_provider,
    test.test_ordered_name,
    test.result_name,
    test.result,
    test.result_comments,
    test.hv_method_flag,
    hv_test_flag,
    hv_result_flag