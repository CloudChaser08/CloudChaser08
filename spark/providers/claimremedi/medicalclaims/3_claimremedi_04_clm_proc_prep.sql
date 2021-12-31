SELECT
    claim_svc_num, proc_code
FROM
    (
        SELECT CONCAT(
                COALESCE(transactional.src_claim_id,''), '__',
                COALESCE(transactional.src_svc_id,'')
                )  as claim_svc_num, -- pk
            TRIM(REGEXP_REPLACE(SUBSTRING(SPLIT(other_proc_codes, ',')[other_proc_codes_exploder.n],
                        LOCATE('=>', SPLIT(other_proc_codes, ',')[other_proc_codes_exploder.n]) + 2,
                        LENGTH(SPLIT(other_proc_codes, ',')[other_proc_codes_exploder.n])), '"', '')) as proc_code
        FROM txn transactional
            CROSS JOIN (SELECT EXPLODE(ARRAY(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11)) AS n) other_proc_codes_exploder
        WHERE
            SPLIT(TRIM(transactional.other_proc_codes), ',')[other_proc_codes_exploder.n] IS NULL
            OR SPLIT(TRIM(transactional.other_proc_codes), ',')[other_proc_codes_exploder.n] != ''
    UNION ALL
        SELECT CONCAT(
                COALESCE(all_codes.src_claim_id,''), '__',
                COALESCE(all_codes.src_svc_id,'')
                ) as claim_svc_num,
            TRIM(SPLIT(all_codes.rest_of_codes, ',')[rest_of_codes_exploder.n]) as proc_code
        FROM (
            SELECT DISTINCT
                transactional.src_claim_id, transactional.src_svc_id, -- pk
                CONCAT(
                    COALESCE(transactional.proc_cd,''), ',',
                    COALESCE(transactional.prinpl_proc_cd,'')
                    ) as rest_of_codes
            FROM txn transactional
                ) all_codes
            CROSS JOIN (SELECT EXPLODE(ARRAY(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11)) AS n) rest_of_codes_exploder
        WHERE
            SPLIT(TRIM(all_codes.rest_of_codes), ',')[rest_of_codes_exploder.n] IS NULL
            OR SPLIT(TRIM(all_codes.rest_of_codes), ',')[rest_of_codes_exploder.n] != ''
    )
