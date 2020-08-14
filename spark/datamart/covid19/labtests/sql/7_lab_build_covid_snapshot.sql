--------------------------------
---------Final Table
--------------------------------
SELECT
    f.*
FROM
(
    SELECT
        src.*
        , ROW_NUMBER() OVER (
                PARTITION BY
                    claim_id
                    ,hvid
                    ,date_service
                    ,hv_test_flag
                    ,part_provider
                ORDER BY
                    claim_id
                    ,hvid
                    ,date_service
                    ,hv_test_flag
                    ,hv_result_flag
                    ,part_provider
            ) as myrow
    FROM
    (
        SELECT
            coalesce(test.claim_id,test.record_id) as claim_id
            ,test.hvid
            ,test.date_service
            ,test.part_provider
            ,ref.hv_test_flag
            ,ref.hv_result_flag
            ,test.result
            ,test.result_comments
            ,test.claim_bucket_id
        FROM
            _temp_lab_covid_tests_cleansed test
            INNER JOIN _temp_lab_covid_ref ref
            ON test.part_provider           = ref.part_provider
                AND test.test_ordered_name  = ref.test_ordered_name
                AND test.result_name        = ref.result_name
                AND test.hv_method_flag     = ref.hv_method_flag
                AND test.result             = ref.result
        WHERE test.hv_method_flag=1 AND test.result_name IS NOT NULL AND test.test_ordered_name IS NOT NULL
    UNION ALL
        --------------------------------
        --------- (result_name IS NULL) 4 Joining fields
        --------------------------------
        SELECT
            coalesce(test.claim_id,test.record_id) as claim_id
            ,test.hvid
            ,test.date_service
            ,test.part_provider
            ,ref.hv_test_flag
            ,ref.hv_result_flag
            ,test.result
            ,test.result_comments
            ,test.claim_bucket_id
        FROM
            _temp_lab_covid_tests_cleansed test
            INNER JOIN _temp_lab_covid_ref ref
            ON ref.result_name IS NULL
                AND test.part_provider      = ref.part_provider
                AND test.test_ordered_name  = ref.test_ordered_name
                AND test.hv_method_flag     = ref.hv_method_flag
                AND test.result             = ref.result
        WHERE test.hv_method_flag=1 AND test.result_name IS NULL AND test.test_ordered_name IS NOT NULL
    UNION ALL
        --------------------------------
        --------- (result_name IS NOT NULL)
        --------------------------------
        SELECT
            coalesce(test.claim_id,test.record_id) as claim_id
            ,test.hvid
            ,test.date_service
            ,test.part_provider
            ,ref.hv_test_flag
            ,ref.hv_result_flag
            ,test.result
            ,test.result_comments
            ,test.claim_bucket_id
        FROM
            _temp_lab_covid_tests_cleansed test
            INNER JOIN _temp_lab_covid_ref ref
            ON ref.test_ordered_name IS NULL
                AND test.part_provider      = ref.part_provider
                AND test.hv_method_flag     = ref.hv_method_flag
                AND test.result_name        = ref.result_name
        WHERE test.hv_method_flag=1 AND test.result_name IS NOT NULL AND test.test_ordered_name IS NULL
    UNION ALL
        SELECT
            coalesce(test.claim_id,test.record_id) as claim_id
            ,test.hvid
            ,test.date_service
            ,test.part_provider
            ,ref.hv_test_flag
            ,ref.hv_result_flag
            ,test.result
            ,test.result_comments
            ,test.claim_bucket_id
        FROM
            _temp_lab_covid_tests_cleansed test
            INNER JOIN _temp_lab_covid_ref ref
            ON test.part_provider           = ref.part_provider
                AND test.test_ordered_name  = ref.test_ordered_name
                AND test.result_name        = ref.result_name
                AND test.hv_method_flag     = ref.hv_method_flag
                AND test.result_comments    = ref.result_comments
                AND test.result             = ref.result
        WHERE test.hv_method_flag=2 AND test.result_name IS NOT NULL AND test.test_ordered_name IS NOT NULL
    UNION ALL
        --------------------------------
        --------- (result_name IS NULL) 4 Joining fields
        --------------------------------
        SELECT
            coalesce(test.claim_id,test.record_id) as claim_id
            ,test.hvid
            ,test.date_service
            ,test.part_provider
            ,ref.hv_test_flag
            ,ref.hv_result_flag
            ,test.result
            ,test.result_comments
            ,test.claim_bucket_id
        FROM
            _temp_lab_covid_tests_cleansed test
            INNER JOIN _temp_lab_covid_ref ref
            ON ref.result_name IS NULL
                AND test.part_provider      = ref.part_provider
                AND test.test_ordered_name  = ref.test_ordered_name
                AND test.hv_method_flag     = ref.hv_method_flag
                AND test.result_comments    = ref.result_comments
                AND test.result             = ref.result
        WHERE test.hv_method_flag=2 AND test.result_name IS NULL AND test.test_ordered_name IS NOT NULL
    UNION ALL
        --------------------------------
        --------- (result_name IS NOT NULL) 4 Joining fields
        --------------------------------
        SELECT
            coalesce(test.claim_id,test.record_id) as claim_id
            ,test.hvid
            ,test.date_service
            ,test.part_provider
            ,ref.hv_test_flag
            ,ref.hv_result_flag
            ,test.result
            ,test.result_comments
            ,test.claim_bucket_id
        FROM
            _temp_lab_covid_tests_cleansed test
            INNER JOIN _temp_lab_covid_ref ref
                ON ref.test_ordered_name IS NULL
                    AND test.part_provider      = ref.part_provider
                    AND test.result_name        = ref.result_name
                    AND test.hv_method_flag     = ref.hv_method_flag
                    AND test.result_comments    = ref.result_comments
                    AND test.result             = ref.result
        WHERE test.hv_method_flag=2 AND test.result_name IS NOT NULL AND test.test_ordered_name IS NULL
    ) src
) f
WHERE
    f.myrow=1