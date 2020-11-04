
TABLE_CONF = [
    {"table_name": "lqrc_order_result_trans"
        , "sql_stmnt": """
            SELECT
                result_name
                ,local_result_code
                ,units
                ,SUBSTR(date_of_service,1,10) AS date_of_service
                ,loinc_code
                ,lab_id
                ,accn_id
            FROM
                order_result rslt
            GROUP BY
                1,2,3,4,5,6,7  
        """
    }
    , {"table_name": "lqrc_blacklisted_lab_id_lkp"
        , "sql_stmnt": """
            SELECT CAST(lab_id as STRING) AS lab_id
                , CAST(do_not_use_ind as STRING) AS do_not_use_ind
                , CAST(do_not_assign_ind as STRING) AS do_not_assign_ind
            FROM
            (
                SELECT '1' AS lab_id, '1' AS do_not_use_ind, '1' as do_not_assign_ind
                UNION
                SELECT '3' AS lab_id, '1' AS do_not_use_ind, '1' as do_not_assign_ind
                UNION
                SELECT '4' AS lab_id, '1' AS do_not_use_ind, '1' as do_not_assign_ind
                UNION
                SELECT '8' AS lab_id, '1' AS do_not_use_ind, '1' as do_not_assign_ind
                UNION
                SELECT '9' AS lab_id, '1' AS do_not_use_ind, '1' as do_not_assign_ind
                UNION
                SELECT '10' AS lab_id, '1' AS do_not_use_ind, '1' as do_not_assign_ind
                UNION
                SELECT '11' AS lab_id, '1' AS do_not_use_ind, '1' as do_not_assign_ind
                UNION
                SELECT '13' AS lab_id, '1' AS do_not_use_ind, '1' as do_not_assign_ind
                UNION
                SELECT '14' AS lab_id, '1' AS do_not_use_ind, '1' as do_not_assign_ind
                UNION
                SELECT '35' AS lab_id, '1' AS do_not_use_ind, '1' as do_not_assign_ind
                UNION
                SELECT '37' AS lab_id, '1' AS do_not_use_ind, '1' as do_not_assign_ind
                UNION
                SELECT '42' AS lab_id, '1' AS do_not_use_ind, '1' as do_not_assign_ind
                UNION
                SELECT '43' AS lab_id, '1' AS do_not_use_ind, '1' as do_not_assign_ind
                UNION
                SELECT '47' AS lab_id, '1' AS do_not_use_ind, '1' as do_not_assign_ind
                UNION
                SELECT '48' AS lab_id, '1' AS do_not_use_ind, '1' as do_not_assign_ind
                UNION
                SELECT '53' AS lab_id, '1' AS do_not_use_ind, '1' as do_not_assign_ind
                UNION
                SELECT '54' AS lab_id, '1' AS do_not_use_ind, '1' as do_not_assign_ind
            ) lc       
        """
    }
    , {"table_name": "lqrc_missing_loincs"
        , "sql_stmnt": """
            SELECT
                UPPER(result_name) AS upper_result_name
                , local_result_code
                , units
                , date_of_service
                , DATE_ADD(CAST(date_of_service AS DATE), 30) AS date_plus_30_days
            FROM lqrc_order_result_trans rslt
            WHERE
                (
                SUBSTR(loinc_code, 1, 1) = 'L'
                OR LENGTH(TRIM(COALESCE(loinc_code, ''))) = 0
                OR EXISTS
                    (
                        SELECT
                            1
                        FROM
                            lqrc_blacklisted_lab_id_lkp blacklist
                        WHERE
                            blacklist.do_not_use_ind='1'
                            AND blacklist.lab_id = rslt.lab_id
                    )
                )
                AND 0 <> LENGTH(TRIM(COALESCE(result_name, '')))
                AND 0 <> LENGTH(TRIM(COALESCE(local_result_code, '')))
                AND 0 <> LENGTH(TRIM(COALESCE(units, '')))
            GROUP BY
                1, 2, 3, 4
        """
    }
    , {"table_name": "lqrc_future_30_days_possibilities"
        , "sql_stmnt": """
            SELECT
                missing_loincs.date_of_service AS missing_date_of_service
                , UPPER(rslt.result_name) AS upper_result_name
                , rslt.local_result_code
                , rslt.units
                , rslt.loinc_code
                , COUNT(DISTINCT accn_id) AS accn_id_count
            FROM
                lqrc_order_result_trans rslt
            INNER JOIN
                lqrc_missing_loincs missing_loincs
                    ON missing_loincs.upper_result_name = UPPER(rslt.result_name)
                        AND missing_loincs.local_result_code = rslt.local_result_code
                        AND missing_loincs.units = rslt.units
                        AND CAST(missing_loincs.date_of_service AS DATE) <= CAST(rslt.date_of_service AS DATE)
                        AND CAST(missing_loincs.date_of_service AS DATE) >= DATE_ADD(CAST(rslt.date_of_service AS DATE), -30)
            WHERE
                0 <> LENGTH(TRIM(COALESCE(rslt.result_name, '')))
                AND 0 <> LENGTH(TRIM(COALESCE(rslt.local_result_code, '')))
                AND 0 <> LENGTH(TRIM(COALESCE(rslt.units, '')))
                AND 0 <> LENGTH(TRIM(COALESCE(rslt.loinc_code,'')))
                AND SUBSTR(rslt.loinc_code, 1, 1) <> 'L'
                AND NOT EXISTS
                    (
                        SELECT
                            1
                        FROM
                            lqrc_blacklisted_lab_id_lkp blacklist
                        WHERE
                            blacklist.do_not_assign_ind ='1'
                            AND rslt.lab_id = blacklist.lab_id
                    )
            GROUP BY
                1, 2, 3, 4, 5
        """
    }
    , {"table_name": "lqrc_no_future_for_30_days"
        , "sql_stmnt": """
            SELECT
                missing_loincs.*
            FROM
                lqrc_missing_loincs missing_loincs
            WHERE
                NOT EXISTS
                (
                    SELECT
                        1
                    FROM
                        lqrc_order_result_trans rslt
                    WHERE
                        missing_loincs.upper_result_name = UPPER(rslt.result_name)
                        AND missing_loincs.local_result_code = rslt.local_result_code
                        AND missing_loincs.units = rslt.units
                        AND missing_loincs.date_plus_30_days >= CAST(rslt.date_of_service AS DATE)
                        AND rslt.loinc_code IS NOT NULL
                        AND NOT EXISTS
                           (
                               SELECT
                                1
                               FROM
                                lqrc_blacklisted_lab_id_lkp blacklist
                               WHERE
                                    blacklist.do_not_assign_ind ='1'
                                    AND rslt.lab_id = blacklist.lab_id
                           )
                )
        """
    }
    , {"table_name": "lqrc_one_loinc_match"
        , "sql_stmnt": """
            SELECT
                missing_date_of_service
                , upper_result_name
                , local_result_code
                , units
                , COUNT(DISTINCT loinc_code) AS loinc_count
            FROM
                lqrc_future_30_days_possibilities
            GROUP BY
                1, 2, 3, 4
            HAVING
                loinc_count = 1
        """
    }
    , {"table_name": "lqrc_one_loinc_match_joined"
        , "sql_stmnt": """
            SELECT
                missing.date_of_service
                , missing.upper_result_name
                , missing.local_result_code
                , missing.units
                , rslt.loinc_code
            FROM
                (
                    SELECT DISTINCT
                        missing.date_of_service
                        , missing.upper_result_name
                        , missing.local_result_code
                        , missing.units
                    FROM
                        lqrc_missing_loincs missing
                    WHERE EXISTS
                        (
                            SELECT
                                1
                            FROM
                                lqrc_one_loinc_match one_match
                            WHERE
                                one_match.upper_result_name = missing.upper_result_name
                                AND one_match.local_result_code = missing.local_result_code
                                AND one_match.units = missing.units
                                AND one_match.missing_date_of_service = missing.date_of_service
                        )
                ) missing
            INNER JOIN
                (
                    SELECT DISTINCT
                        rslt.date_of_service
                        , UPPER(rslt.result_name) AS upper_result_name
                        , rslt.local_result_code
                        , rslt.loinc_code
                        , rslt.units
                    FROM
                        lqrc_order_result_trans rslt
                    WHERE
                        rslt.loinc_code IS NOT NULL
                        AND SUBSTR(rslt.loinc_code, 1, 1) <> 'L'
                        AND NOT EXISTS
                        (
                            SELECT
                                1
                            FROM
                                lqrc_blacklisted_lab_id_lkp blacklist
                            WHERE
                                blacklist.do_not_assign_ind ='1'
                                AND rslt.lab_id = blacklist.lab_id
                        )
                ) rslt
                    ON missing.upper_result_name = rslt.upper_result_name
                        AND missing.local_result_code = rslt.local_result_code
                        AND missing.units = rslt.units
                        AND CAST(missing.date_of_service AS DATE) <= CAST(rslt.date_of_service AS DATE)
                        AND CAST(missing.date_of_service AS DATE) >= DATE_ADD(CAST(rslt.date_of_service AS DATE), -30)
            GROUP BY
                1, 2, 3, 4, 5
        """
    }
    , {"table_name": "lqrc_many_loinc_matches"
        , "sql_stmnt": """
            SELECT
                missing_date_of_service
                , upper_result_name
                , local_result_code
                , units
                , COUNT(DISTINCT loinc_code) AS loinc_count
            FROM
                lqrc_future_30_days_possibilities
            GROUP BY
                1, 2, 3, 4
            HAVING
                loinc_count > 1
        """
    }
    , {"table_name": "lqrc_many_loinc_matches_joined"
        , "sql_stmnt": """
            SELECT
                poss.missing_date_of_service
                , poss.upper_result_name
                , poss.local_result_code
                , poss.units
                , poss.loinc_code
            FROM
                lqrc_future_30_days_possibilities poss
            INNER JOIN
                (
                   SELECT
                        missing_date_of_service
                        , upper_result_name
                        , local_result_code
                        , units
                        , MAX(accn_id_count) AS max_accn_id_count
                   FROM
                        lqrc_future_30_days_possibilities
                   GROUP BY
                        1, 2, 3, 4
                ) max_accn_id
            ON poss.missing_date_of_service = max_accn_id.missing_date_of_service
                AND poss.upper_result_name = max_accn_id.upper_result_name
                AND poss.local_result_code = max_accn_id.local_result_code
                AND poss.units = max_accn_id.units
                AND poss.accn_id_count = max_accn_id.max_accn_id_count
            WHERE EXISTS
                (
                    SELECT
                        1
                    FROM
                        lqrc_many_loinc_matches multimatch
                    WHERE
                        multimatch.upper_result_name = poss.upper_result_name
                        AND multimatch.local_result_code = poss.local_result_code
                        AND multimatch.units = poss.units
                        AND multimatch.missing_date_of_service = poss.missing_date_of_service
                )
            GROUP BY
                1, 2, 3, 4, 5
        """
    }
    , {"table_name": "lqrc_many_loinc_matches_joined_no_ties"
        , "sql_stmnt": """
            SELECT
                multimatch.missing_date_of_service
                , multimatch.upper_result_name
                , multimatch.local_result_code
                , multimatch.units
                , multimatch.loinc_code
            FROM
                lqrc_many_loinc_matches_joined multimatch
            INNER JOIN
                (
                   SELECT
                        missing_date_of_service
                        , upper_result_name
                        , local_result_code
                        , units
                        , COUNT(DISTINCT loinc_code) AS loinc_count
                   FROM
                        lqrc_many_loinc_matches_joined
                   GROUP BY
                        1, 2, 3, 4
                   HAVING
                        loinc_count = 1
                ) no_ties
             ON multimatch.missing_date_of_service = no_ties.missing_date_of_service
                AND multimatch.upper_result_name = no_ties.upper_result_name
                AND multimatch.local_result_code = no_ties.local_result_code
                AND multimatch.units = no_ties.units
            GROUP BY
                1, 2, 3, 4, 5
        """
    }
    , {"table_name": "lqrc_many_loinc_matches_tied"
        , "sql_stmnt": """
            SELECT
                missing_date_of_service
                , upper_result_name
                , local_result_code
                , units
                , COUNT(*) AS row_cnt
            FROM
                lqrc_many_loinc_matches_joined
            GROUP BY
                1, 2, 3, 4
            HAVING
                row_cnt > 1
        """
    }
    , {"table_name": "lqrc_many_loinc_matches_tied_joined"
        , "sql_stmnt": """
            SELECT
                date_match.missing_date_of_service
                , UPPER(rslt.result_name) AS upper_result_name
                , rslt.local_result_code
                , rslt.units
                , FIRST(rslt.loinc_code) AS loinc_code
            FROM
                lqrc_order_result_trans rslt
                INNER JOIN
                (
                    SELECT
                        upper_result_name
                        , local_result_code
                        , units
                        , missing_date_of_service
                        , MIN(date_of_service) AS min_date_of_service
                    FROM
                        (
                            SELECT
                                UPPER(rslt.result_name) AS upper_result_name
                                , rslt.local_result_code
                                , rslt.units
                                , rslt.date_of_service
                                , multimatch_tied.missing_date_of_service
                            FROM
                                lqrc_order_result_trans rslt
                                INNER JOIN
                                    lqrc_many_loinc_matches_tied multimatch_tied
                                  ON multimatch_tied.upper_result_name = UPPER(rslt.result_name)
                                     AND multimatch_tied.local_result_code = rslt.local_result_code
                                     AND multimatch_tied.units = rslt.units
                                     AND CAST(date_of_service AS DATE) >= CAST(multimatch_tied.missing_date_of_service AS DATE)
                                     AND CAST(date_of_service AS DATE) <= DATE_ADD(CAST(multimatch_tied.missing_date_of_service AS DATE), 30)
                                     AND loinc_code IS NOT NULL
                            GROUP BY
                                1, 2, 3, 4, 5
                        ) loinc_poss
                    GROUP BY
                        1, 2, 3, 4
                ) date_match
                ON date_match.upper_result_name = UPPER(rslt.result_name)
                    AND date_match.local_result_code = rslt.local_result_code
                    AND date_match.units = rslt.units
                    AND date_match.min_date_of_service = rslt.date_of_service
            WHERE
                loinc_code IS NOT NULL
            GROUP BY
                1, 2, 3, 4
        """
    }
    , {"table_name": "lqrc_closest_future_loinc_result"
        , "sql_stmnt": """
            SELECT
                UPPER(rslt.result_name) AS upper_result_name
                , rslt.local_result_code
                , rslt.units
                , rslt.loinc_code
            FROM
                lqrc_order_result_trans rslt
            INNER JOIN
                (
                    SELECT
                        UPPER(result_name) AS upper_result_name
                        , local_result_code
                        , units
                        , MIN(date_of_service) AS min_date_service
                    FROM
                        lqrc_order_result_trans rslt
                    WHERE
                        0 <> LENGTH(TRIM(COALESCE(result_name, '')))
                        AND 0 <> LENGTH(TRIM(COALESCE(local_result_code, '')))
                        AND 0 <> LENGTH(TRIM(COALESCE(units, '')))
                        AND 0 <> LENGTH(TRIM(COALESCE(loinc_code,'')))
                        AND SUBSTR(loinc_code, 1, 1) <> 'L'
                        AND EXISTS
                            (
                                SELECT 1
                                FROM
                                    lqrc_no_future_for_30_days missing
                                WHERE
                                    missing.upper_result_name = UPPER(rslt.result_name)
                                    AND missing.local_result_code = rslt.local_result_code
                                    AND missing.units = rslt.units
                                    AND CAST(missing.date_of_service AS DATE) <= CAST(rslt.date_of_service AS DATE)
                            )
                    GROUP BY 1, 2, 3
                ) min_service_date
            ON UPPER(rslt.result_name) = min_service_date.upper_result_name
                AND rslt.local_result_code = min_service_date.local_result_code
                AND rslt.units = min_service_date.units
                AND rslt.date_of_service = min_service_date.min_date_service
            WHERE
                rslt.loinc_code IS NOT NULL
                AND NOT EXISTS
                   (
                       SELECT 1
                       FROM lqrc_blacklisted_lab_id_lkp blacklist
                       WHERE blacklist.do_not_assign_ind ='1' AND rslt.lab_id = blacklist.lab_id
                   )
            GROUP BY
                1, 2, 3, 4
        """
    }
    , {"table_name": "lqrc_closest_future_loinc_joined"
        , "sql_stmnt": """
            SELECT
                missing.date_of_service
                , missing.upper_result_name
                , missing.local_result_code
                , missing.units
                , closest_future.loinc_code
            FROM
                lqrc_no_future_for_30_days missing
                INNER JOIN
                    lqrc_closest_future_loinc_result closest_future
                ON missing.upper_result_name = closest_future.upper_result_name
                    AND missing.local_result_code = closest_future.local_result_code
                    AND missing.units = closest_future.units
            GROUP BY
                1, 2, 3, 4, 5
        """
    }
    , {"table_name": "lqrc_loinc_final"
        , "sql_stmnt": """
            SELECT * FROM lqrc_one_loinc_match_joined
            UNION ALL SELECT * FROM lqrc_many_loinc_matches_joined_no_ties
            UNION ALL SELECT * FROM lqrc_many_loinc_matches_tied_joined
            UNION ALL SELECT * FROM lqrc_closest_future_loinc_joined
        """
    }
    , {"table_name": "loinc_delta"
        , "sql_stmnt": """
            SELECT
                curr.*
                ,CAST(year(CAST(SUBSTR(date_of_service,1,10) AS DATE)) AS STRING) as year
            FROM
                lqrc_loinc_final curr
            WHERE
                NOT EXISTS
                (
                    SELECT
                        1
                    FROM
                        loinc hist
                    WHERE
                       CAST(SUBSTR(curr.date_of_service,1,10) AS DATE) = CAST(SUBSTR(hist.date_of_service,1,10) AS DATE)
                       AND UPPER(curr.upper_result_name) = UPPER(hist.upper_result_name)
                       AND curr.local_result_code = hist.local_result_code
                       AND curr.units = hist.units
                )
        """
    }
]
