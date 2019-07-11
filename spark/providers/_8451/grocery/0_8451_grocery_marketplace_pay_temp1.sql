SELECT
    hvid,
    personid,
    threedigitzip,
    yearofbirth,
    gender,
    age,
    state,
    info_combo_len,
    ROW_NUMBER() OVER(PARTITION BY personid ORDER BY info_combo_len DESC) AS best_row_num
 FROM
(
    SELECT
        hvid,
        personid,
        threedigitzip,
        yearofbirth,
        gender,
        age,
        state,
        LENGTH
            (
                CONCAT
                    (
                        COALESCE(hvid, ''),
                        COALESCE(threedigitzip, ''),
                        COALESCE(yearofbirth, ''),
                        COALESCE(gender, ''),
                        COALESCE(age, ''),
                        COALESCE(state, '')
                    )
            ) AS info_combo_len
     FROM matching_payload
)
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8
