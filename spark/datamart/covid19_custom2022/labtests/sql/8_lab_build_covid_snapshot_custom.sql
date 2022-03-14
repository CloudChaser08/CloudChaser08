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
        --------------------------------
        --------- 1 - claim_bucket_id NULL
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
            lab_cleanse_covid_tests_all_custom test
            INNER JOIN lab_build_covid_ref_custom ref
            ON test.part_provider           = ref.part_provider
                AND test.test_ordered_name  = ref.test_ordered_name
                AND test.result_name        = ref.result_name
                AND test.hv_method_flag     = ref.hv_method_flag
                AND test.result             = ref.result
        WHERE test.claim_bucket_id IS NULL
            AND test.hv_method_flag=1 AND test.result_name IS NOT NULL AND test.test_ordered_name IS NOT NULL
            AND ref.hv_method_flag=1 AND ref.result_name IS NOT NULL AND ref.test_ordered_name IS NOT NULL
    UNION ALL
        --------------------------------
        --------- 1 claim_bucket_id = 0
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
            lab_cleanse_covid_tests_all_custom test
            INNER JOIN lab_build_covid_ref_custom ref
            ON test.part_provider           = ref.part_provider
                AND test.test_ordered_name  = ref.test_ordered_name
                AND test.result_name        = ref.result_name
                AND test.hv_method_flag     = ref.hv_method_flag
                AND test.result             = ref.result
        WHERE test.claim_bucket_id BETWEEN CAST({claim_bucket_id_low_1} AS INT) AND CAST({claim_bucket_id_up_1} AS INT)
            AND test.hv_method_flag=1 AND test.result_name IS NOT NULL AND test.test_ordered_name IS NOT NULL
            AND ref.hv_method_flag=1 AND ref.result_name IS NOT NULL AND ref.test_ordered_name IS NOT NULL
    UNION ALL
        --------------------------------
        --------- 1 claim_bucket_id = 1
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
            lab_cleanse_covid_tests_all_custom test
            INNER JOIN lab_build_covid_ref_custom ref
            ON test.part_provider           = ref.part_provider
                AND test.test_ordered_name  = ref.test_ordered_name
                AND test.result_name        = ref.result_name
                AND test.hv_method_flag     = ref.hv_method_flag
                AND test.result             = ref.result
        WHERE test.claim_bucket_id BETWEEN CAST({claim_bucket_id_low_2} AS INT) AND CAST({claim_bucket_id_up_2} AS INT)
            AND test.hv_method_flag=1 AND test.result_name IS NOT NULL AND test.test_ordered_name IS NOT NULL
            AND ref.hv_method_flag=1 AND ref.result_name IS NOT NULL AND ref.test_ordered_name IS NOT NULL
    UNION ALL
        --------------------------------
        --------- 1 claim_bucket_id = 2
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
            lab_cleanse_covid_tests_all_custom test
            INNER JOIN lab_build_covid_ref_custom ref
            ON test.part_provider           = ref.part_provider
                AND test.test_ordered_name  = ref.test_ordered_name
                AND test.result_name        = ref.result_name
                AND test.hv_method_flag     = ref.hv_method_flag
                AND test.result             = ref.result
        WHERE test.claim_bucket_id BETWEEN CAST({claim_bucket_id_low_3} AS INT) AND CAST({claim_bucket_id_up_3} AS INT)
            AND test.hv_method_flag=1 AND test.result_name IS NOT NULL AND test.test_ordered_name IS NOT NULL
            AND ref.hv_method_flag=1 AND ref.result_name IS NOT NULL AND ref.test_ordered_name IS NOT NULL
    UNION ALL
        --------------------------------
        --------- 1 claim_bucket_id = 3
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
            lab_cleanse_covid_tests_all_custom test
            INNER JOIN lab_build_covid_ref_custom ref
            ON test.part_provider           = ref.part_provider
                AND test.test_ordered_name  = ref.test_ordered_name
                AND test.result_name        = ref.result_name
                AND test.hv_method_flag     = ref.hv_method_flag
                AND test.result             = ref.result
        WHERE test.claim_bucket_id BETWEEN CAST({claim_bucket_id_low_4} AS INT) AND CAST({claim_bucket_id_up_4} AS INT)
            AND test.hv_method_flag=1 AND test.result_name IS NOT NULL AND test.test_ordered_name IS NOT NULL
            AND ref.hv_method_flag=1 AND ref.result_name IS NOT NULL AND ref.test_ordered_name IS NOT NULL
    UNION ALL
        --------------------------------
        --------- 1 claim_bucket_id = 4
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
            lab_cleanse_covid_tests_all_custom test
            INNER JOIN lab_build_covid_ref_custom ref
            ON test.part_provider           = ref.part_provider
                AND test.test_ordered_name  = ref.test_ordered_name
                AND test.result_name        = ref.result_name
                AND test.hv_method_flag     = ref.hv_method_flag
                AND test.result             = ref.result
        WHERE test.claim_bucket_id BETWEEN CAST({claim_bucket_id_low_5} AS INT) AND CAST({claim_bucket_id_up_5} AS INT)
            AND test.hv_method_flag=1 AND test.result_name IS NOT NULL AND test.test_ordered_name IS NOT NULL
            AND ref.hv_method_flag=1 AND ref.result_name IS NOT NULL AND ref.test_ordered_name IS NOT NULL
    UNION ALL
        --------------------------------
        --------- 1 claim_bucket_id = 5
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
            lab_cleanse_covid_tests_all_custom test
            INNER JOIN lab_build_covid_ref_custom ref
            ON test.part_provider           = ref.part_provider
                AND test.test_ordered_name  = ref.test_ordered_name
                AND test.result_name        = ref.result_name
                AND test.hv_method_flag     = ref.hv_method_flag
                AND test.result             = ref.result
        WHERE test.claim_bucket_id BETWEEN CAST({claim_bucket_id_low_6} AS INT) AND CAST({claim_bucket_id_up_6} AS INT)
            AND test.hv_method_flag=1 AND test.result_name IS NOT NULL AND test.test_ordered_name IS NOT NULL
            AND ref.hv_method_flag=1 AND ref.result_name IS NOT NULL AND ref.test_ordered_name IS NOT NULL
    UNION ALL
        --------------------------------
        --------- 1 claim_bucket_id = 6-10
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
            lab_cleanse_covid_tests_all_custom test
            INNER JOIN lab_build_covid_ref_custom ref
            ON test.part_provider           = ref.part_provider
                AND test.test_ordered_name  = ref.test_ordered_name
                AND test.result_name        = ref.result_name
                AND test.hv_method_flag     = ref.hv_method_flag
                AND test.result             = ref.result
        WHERE test.claim_bucket_id BETWEEN CAST({claim_bucket_id_low_7} AS INT) AND CAST({claim_bucket_id_up_7} AS INT)
            AND test.hv_method_flag=1 AND test.result_name IS NOT NULL AND test.test_ordered_name IS NOT NULL
            AND ref.hv_method_flag=1 AND ref.result_name IS NOT NULL AND ref.test_ordered_name IS NOT NULL
    UNION ALL
        --------------------------------
        --------- 1 claim_bucket_id = 11-15
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
            lab_cleanse_covid_tests_all_custom test
            INNER JOIN lab_build_covid_ref_custom ref
            ON test.part_provider           = ref.part_provider
                AND test.test_ordered_name  = ref.test_ordered_name
                AND test.result_name        = ref.result_name
                AND test.hv_method_flag     = ref.hv_method_flag
                AND test.result             = ref.result
        WHERE test.claim_bucket_id BETWEEN CAST({claim_bucket_id_low_8} AS INT) AND CAST({claim_bucket_id_up_8} AS INT)
            AND test.hv_method_flag=1 AND test.result_name IS NOT NULL AND test.test_ordered_name IS NOT NULL
            AND ref.hv_method_flag=1 AND ref.result_name IS NOT NULL AND ref.test_ordered_name IS NOT NULL
    UNION ALL
        --------------------------------
        --------- 1 claim_bucket_id = 16-20
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
            lab_cleanse_covid_tests_all_custom test
            INNER JOIN lab_build_covid_ref_custom ref
            ON test.part_provider           = ref.part_provider
                AND test.test_ordered_name  = ref.test_ordered_name
                AND test.result_name        = ref.result_name
                AND test.hv_method_flag     = ref.hv_method_flag
                AND test.result             = ref.result
        WHERE test.claim_bucket_id BETWEEN CAST({claim_bucket_id_low_9} AS INT) AND CAST({claim_bucket_id_up_9} AS INT)
            AND test.hv_method_flag=1 AND test.result_name IS NOT NULL AND test.test_ordered_name IS NOT NULL
            AND ref.hv_method_flag=1 AND ref.result_name IS NOT NULL AND ref.test_ordered_name IS NOT NULL
    UNION ALL
        --------------------------------
        --------- 1 claim_bucket_id = 21-25
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
            lab_cleanse_covid_tests_all_custom test
            INNER JOIN lab_build_covid_ref_custom ref
            ON test.part_provider           = ref.part_provider
                AND test.test_ordered_name  = ref.test_ordered_name
                AND test.result_name        = ref.result_name
                AND test.hv_method_flag     = ref.hv_method_flag
                AND test.result             = ref.result
        WHERE test.claim_bucket_id BETWEEN CAST({claim_bucket_id_low_10} AS INT) AND CAST({claim_bucket_id_up_10} AS INT)
            AND test.hv_method_flag=1 AND test.result_name IS NOT NULL AND test.test_ordered_name IS NOT NULL
            AND ref.hv_method_flag=1 AND ref.result_name IS NOT NULL AND ref.test_ordered_name IS NOT NULL
    UNION ALL
        --------------------------------
        --------- 1 claim_bucket_id = 26-30
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
            lab_cleanse_covid_tests_all_custom test
            INNER JOIN lab_build_covid_ref_custom ref
            ON test.part_provider           = ref.part_provider
                AND test.test_ordered_name  = ref.test_ordered_name
                AND test.result_name        = ref.result_name
                AND test.hv_method_flag     = ref.hv_method_flag
                AND test.result             = ref.result
        WHERE test.claim_bucket_id BETWEEN CAST({claim_bucket_id_low_11} AS INT) AND CAST({claim_bucket_id_up_11} AS INT)
            AND test.hv_method_flag=1 AND test.result_name IS NOT NULL AND test.test_ordered_name IS NOT NULL
            AND ref.hv_method_flag=1 AND ref.result_name IS NOT NULL AND ref.test_ordered_name IS NOT NULL
    UNION ALL
        --------------------------------
        --------- 1 claim_bucket_id = 31-35
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
            lab_cleanse_covid_tests_all_custom test
            INNER JOIN lab_build_covid_ref_custom ref
            ON test.part_provider           = ref.part_provider
                AND test.test_ordered_name  = ref.test_ordered_name
                AND test.result_name        = ref.result_name
                AND test.hv_method_flag     = ref.hv_method_flag
                AND test.result             = ref.result
        WHERE test.claim_bucket_id BETWEEN CAST({claim_bucket_id_low_12} AS INT) AND CAST({claim_bucket_id_up_12} AS INT)
            AND test.hv_method_flag=1 AND test.result_name IS NOT NULL AND test.test_ordered_name IS NOT NULL
            AND ref.hv_method_flag=1 AND ref.result_name IS NOT NULL AND ref.test_ordered_name IS NOT NULL
    UNION ALL
        --------------------------------
        --------- 1 claim_bucket_id = 36-40
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
            lab_cleanse_covid_tests_all_custom test
            INNER JOIN lab_build_covid_ref_custom ref
            ON test.part_provider           = ref.part_provider
                AND test.test_ordered_name  = ref.test_ordered_name
                AND test.result_name        = ref.result_name
                AND test.hv_method_flag     = ref.hv_method_flag
                AND test.result             = ref.result
        WHERE test.claim_bucket_id BETWEEN CAST({claim_bucket_id_low_13} AS INT) AND CAST({claim_bucket_id_up_13} AS INT)
            AND test.hv_method_flag=1 AND test.result_name IS NOT NULL AND test.test_ordered_name IS NOT NULL
            AND ref.hv_method_flag=1 AND ref.result_name IS NOT NULL AND ref.test_ordered_name IS NOT NULL
    UNION ALL
        --------------------------------
        --------- 1 claim_bucket_id = 41-45
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
            lab_cleanse_covid_tests_all_custom test
            INNER JOIN lab_build_covid_ref_custom ref
            ON test.part_provider           = ref.part_provider
                AND test.test_ordered_name  = ref.test_ordered_name
                AND test.result_name        = ref.result_name
                AND test.hv_method_flag     = ref.hv_method_flag
                AND test.result             = ref.result
        WHERE test.claim_bucket_id BETWEEN CAST({claim_bucket_id_low_14} AS INT) AND CAST({claim_bucket_id_up_14} AS INT)
            AND test.hv_method_flag=1 AND test.result_name IS NOT NULL AND test.test_ordered_name IS NOT NULL
            AND ref.hv_method_flag=1 AND ref.result_name IS NOT NULL AND ref.test_ordered_name IS NOT NULL
    UNION ALL
        --------------------------------
        --------- 1 claim_bucket_id = 46=50
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
            lab_cleanse_covid_tests_all_custom test
            INNER JOIN lab_build_covid_ref_custom ref
            ON test.part_provider           = ref.part_provider
                AND test.test_ordered_name  = ref.test_ordered_name
                AND test.result_name        = ref.result_name
                AND test.hv_method_flag     = ref.hv_method_flag
                AND test.result             = ref.result
        WHERE test.claim_bucket_id BETWEEN CAST({claim_bucket_id_low_15} AS INT) AND CAST({claim_bucket_id_up_15} AS INT)
            AND test.hv_method_flag=1 AND test.result_name IS NOT NULL AND test.test_ordered_name IS NOT NULL
            AND ref.hv_method_flag=1 AND ref.result_name IS NOT NULL AND ref.test_ordered_name IS NOT NULL
    UNION ALL
        --------------------------------
        --------- 1 claim_bucket_id = 51 - max minus 1
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
            lab_cleanse_covid_tests_all_custom test
            INNER JOIN lab_build_covid_ref_custom ref
            ON test.part_provider           = ref.part_provider
                AND test.test_ordered_name  = ref.test_ordered_name
                AND test.result_name        = ref.result_name
                AND test.hv_method_flag     = ref.hv_method_flag
                AND test.result             = ref.result
        WHERE test.claim_bucket_id BETWEEN CAST({claim_bucket_id_low_16} AS INT) AND CAST({claim_bucket_id_up_16} AS INT)
            AND test.hv_method_flag=1 AND test.result_name IS NOT NULL AND test.test_ordered_name IS NOT NULL
            AND ref.hv_method_flag=1 AND ref.result_name IS NOT NULL AND ref.test_ordered_name IS NOT NULL
   UNION ALL
        --------------------------------
        --------- 2 (result_name IS NULL) 4 Joining fields
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
            lab_cleanse_covid_tests_all_custom test
            INNER JOIN lab_build_covid_ref_custom ref
            ON ref.result_name IS NULL
                AND test.part_provider      = ref.part_provider
                AND test.test_ordered_name  = ref.test_ordered_name
                AND test.hv_method_flag     = ref.hv_method_flag
                AND test.result             = ref.result
        WHERE test.hv_method_flag=1 AND test.result_name IS NULL AND test.test_ordered_name IS NOT NULL
            AND ref.hv_method_flag=1 AND ref.result_name IS NULL AND ref.test_ordered_name IS NOT NULL
    UNION ALL
        --------------------------------
        --------- 3 (result_name IS NOT NULL)
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
            lab_cleanse_covid_tests_all_custom test
            INNER JOIN lab_build_covid_ref_custom ref
            ON ref.test_ordered_name IS NULL
                AND test.part_provider      = ref.part_provider
                AND test.hv_method_flag     = ref.hv_method_flag
                AND test.result_name        = ref.result_name
        WHERE test.hv_method_flag=1 AND test.result_name IS NOT NULL AND test.test_ordered_name IS NULL
            AND ref.hv_method_flag=1 AND ref.result_name IS NOT NULL AND ref.test_ordered_name IS NULL
    UNION ALL
        --------------------------------
        --------- '1 - 2nd'
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
            lab_cleanse_covid_tests_all_custom test
            INNER JOIN lab_build_covid_ref_custom ref
            ON test.part_provider           = ref.part_provider
                AND test.test_ordered_name  = ref.test_ordered_name
                AND test.result_name        = ref.result_name
                AND test.hv_method_flag     = ref.hv_method_flag
                AND test.result_comments    = ref.result_comments
                AND test.result             = ref.result
        WHERE test.hv_method_flag=2 AND test.result_name IS NOT NULL AND test.test_ordered_name IS NOT NULL
            AND ref.hv_method_flag=2 AND ref.result_name IS NOT NULL AND ref.test_ordered_name IS NOT NULL
    UNION ALL
        --------------------------------
        --------- '1 - 2nd' (result_name IS NULL) 4 Joining fields
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
            lab_cleanse_covid_tests_all_custom test
            INNER JOIN lab_build_covid_ref_custom ref
            ON ref.result_name IS NULL
                AND test.part_provider      = ref.part_provider
                AND test.test_ordered_name  = ref.test_ordered_name
                AND test.hv_method_flag     = ref.hv_method_flag
                AND test.result_comments    = ref.result_comments
                AND test.result             = ref.result
        WHERE test.hv_method_flag=2 AND test.result_name IS NULL AND test.test_ordered_name IS NOT NULL
            AND ref.hv_method_flag=2 AND ref.result_name IS NULL AND ref.test_ordered_name IS NOT NULL
    UNION ALL
        --------------------------------
        --------- '1 - 2nd' (result_name IS NOT NULL) 4 Joining fields
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
            lab_cleanse_covid_tests_all_custom test
            INNER JOIN lab_build_covid_ref_custom ref
                ON ref.test_ordered_name IS NULL
                    AND test.part_provider      = ref.part_provider
                    AND test.result_name        = ref.result_name
                    AND test.hv_method_flag     = ref.hv_method_flag
                    AND test.result_comments    = ref.result_comments
                    AND test.result             = ref.result
        WHERE test.hv_method_flag=2 AND test.result_name IS NOT NULL AND test.test_ordered_name IS NULL
            AND ref.hv_method_flag=2 AND ref.result_name IS NOT NULL AND ref.test_ordered_name IS NULL
    ) src
) f
WHERE
    f.myrow=1