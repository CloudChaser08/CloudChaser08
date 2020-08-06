SELECT
    week_end
    ,hv_test_flag
    ,hv_result_flag
    ,supplier
    ,FORMAT_NUMBER(COUNT(*), 0) AS tests
    ,FORMAT_NUMBER(COUNT(distinct hvid), 0) AS pats
FROM
(
    SELECT
        CASE
            WHEN date_format(date_service ,'u')  = 1 THEN DATE_ADD(date_service , 6)
            WHEN date_format(date_service ,'u')  = 2 THEN DATE_ADD(date_service , 5)
            WHEN date_format(date_service ,'u')  = 3 THEN DATE_ADD(date_service , 4)
            WHEN date_format(date_service ,'u')  = 4 THEN DATE_ADD(date_service , 3)
            WHEN date_format(date_service ,'u')  = 5 THEN DATE_ADD(date_service , 2)
            WHEN date_format(date_service ,'u')  = 6 THEN DATE_ADD(date_service , 1)
            WHEN date_format(date_service ,'u')  = 7 THEN DATE_ADD(date_service , 0)
        ELSE NULL
        END AS week_end
        ,hv_test_flag
        ,hv_result_flag
        ,'0 Total' AS supplier
        ,hvid
    FROM
        _temp_lab_covid_snapshot
    WHERE
        substr(hv_result_flag,1,2) in ('01','02','03','04')
UNION ALL
    SELECT
        CASE
            WHEN date_format(date_service ,'u')  = 1 THEN DATE_ADD(date_service , 6)
            WHEN date_format(date_service ,'u')  = 2 THEN DATE_ADD(date_service , 5)
            WHEN date_format(date_service ,'u')  = 3 THEN DATE_ADD(date_service , 4)
            WHEN date_format(date_service ,'u')  = 4 THEN DATE_ADD(date_service , 3)
            WHEN date_format(date_service ,'u')  = 5 THEN DATE_ADD(date_service , 2)
            WHEN date_format(date_service ,'u')  = 6 THEN DATE_ADD(date_service , 1)
            WHEN date_format(date_service ,'u')  = 7 THEN DATE_ADD(date_service , 0)
        ELSE NULL
        END AS week_end
        ,hv_test_flag
        ,'0 Total'  AS hv_result_flag
        ,'0 Total'  AS supplier
        ,hvid
    FROM
        _temp_lab_covid_snapshot
    WHERE
        substr(hv_result_flag,1,2) in ('01','02','03','04')
UNION ALL
    SELECT
        CASE
            WHEN date_format(date_service ,'u')  = 1 THEN DATE_ADD(date_service , 6)
            WHEN date_format(date_service ,'u')  = 2 THEN DATE_ADD(date_service , 5)
            WHEN date_format(date_service ,'u')  = 3 THEN DATE_ADD(date_service , 4)
            WHEN date_format(date_service ,'u')  = 4 THEN DATE_ADD(date_service , 3)
            WHEN date_format(date_service ,'u')  = 5 THEN DATE_ADD(date_service , 2)
            WHEN date_format(date_service ,'u')  = 6 THEN DATE_ADD(date_service , 1)
            WHEN date_format(date_service ,'u')  = 7 THEN DATE_ADD(date_service , 0)
        ELSE NULL
        END AS week_end
        ,hv_test_flag
        ,'All Valid' AS hv_result_flag
        ,'0 Total' AS supplier
        ,hvid
    FROM
        _temp_lab_covid_snapshot
    WHERE
        substr(hv_result_flag,1,2) in ('01','02','03','04')
UNION ALL
    SELECT
        CASE
            WHEN date_format(date_service ,'u')  = 1 THEN DATE_ADD(date_service , 6)
            WHEN date_format(date_service ,'u')  = 2 THEN DATE_ADD(date_service , 5)
            WHEN date_format(date_service ,'u')  = 3 THEN DATE_ADD(date_service , 4)
            WHEN date_format(date_service ,'u')  = 4 THEN DATE_ADD(date_service , 3)
            WHEN date_format(date_service ,'u')  = 5 THEN DATE_ADD(date_service , 2)
            WHEN date_format(date_service ,'u')  = 6 THEN DATE_ADD(date_service , 1)
            WHEN date_format(date_service ,'u')  = 7 THEN DATE_ADD(date_service , 0)
        ELSE NULL
        END AS week_end
        ,hv_test_flag
        ,hv_result_flag
        ,part_provider AS supplier
        ,hvid
    FROM
        _temp_lab_covid_snapshot
    WHERE
        substr(hv_result_flag,1,2) in ('01','02','03','04')
UNION ALL
    SELECT
        CASE
            WHEN date_format(date_service ,'u')  = 1 THEN DATE_ADD(date_service , 6)
            WHEN date_format(date_service ,'u')  = 2 THEN DATE_ADD(date_service , 5)
            WHEN date_format(date_service ,'u')  = 3 THEN DATE_ADD(date_service , 4)
            WHEN date_format(date_service ,'u')  = 4 THEN DATE_ADD(date_service , 3)
            WHEN date_format(date_service ,'u')  = 5 THEN DATE_ADD(date_service , 2)
            WHEN date_format(date_service ,'u')  = 6 THEN DATE_ADD(date_service , 1)
            WHEN date_format(date_service ,'u')  = 7 THEN DATE_ADD(date_service , 0)
        ELSE NULL
        END AS week_end
        ,hv_test_flag
        ,'0 Total' AS hv_result_flag
        ,part_provider AS supplier
        ,hvid
    FROM
        _temp_lab_covid_snapshot
UNION ALL
    SELECT
        CASE
            WHEN date_format(date_service ,'u')  = 1 THEN DATE_ADD(date_service , 6)
            WHEN date_format(date_service ,'u')  = 2 THEN DATE_ADD(date_service , 5)
            WHEN date_format(date_service ,'u')  = 3 THEN DATE_ADD(date_service , 4)
            WHEN date_format(date_service ,'u')  = 4 THEN DATE_ADD(date_service , 3)
            WHEN date_format(date_service ,'u')  = 5 THEN DATE_ADD(date_service , 2)
            WHEN date_format(date_service ,'u')  = 6 THEN DATE_ADD(date_service , 1)
            WHEN date_format(date_service ,'u')  = 7 THEN DATE_ADD(date_service , 0)
        ELSE NULL
        END AS week_end
        ,hv_test_flag
        ,'All Valid' AS hv_result_flag
        ,part_provider AS supplier
        ,hvid
    FROM
        _temp_lab_covid_snapshot
    WHERE
        substr(hv_result_flag,1,2) in ('01','02','03','04')
) sm
    GROUP BY
        week_end, hv_test_flag, hv_result_flag, supplier