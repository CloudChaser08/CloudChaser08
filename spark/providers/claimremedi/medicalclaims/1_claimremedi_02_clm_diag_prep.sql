SELECT
    claim_svc_num, diag_code
FROM
    (
        SELECT
            CONCAT(
                COALESCE(transactional.src_claim_id,''), '__',
                COALESCE(transactional.src_svc_id,'')
            ) as claim_svc_num, -- pk
            TRIM(REGEXP_REPLACE(SUBSTRING(SPLIT(ecodes, ',')[ecodes_exploder.n], 0,
                        locate('=>', SPLIT(ecodes, ',')[ecodes_exploder.n])-1), '"', '')) as diag_code
        FROM txn transactional
            CROSS JOIN (SELECT EXPLODE(ARRAY(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11)) AS n) ecodes_exploder
        WHERE
            SPLIT(TRIM(transactional.ecodes), ',')[ecodes_exploder.n] IS NULL
            OR SPLIT(TRIM(transactional.ecodes), ',')[ecodes_exploder.n] != ''
    UNION ALL
        SELECT CONCAT(
                COALESCE(transactional.src_claim_id,''), '__',
                COALESCE(transactional.src_svc_id,'')
                ) as claim_svc_num, -- pk
            TRIM(REGEXP_REPLACE(SUBSTRING(SPLIT(dx, ',')[dx_exploder.n], 0,
                        locate('=>', SPLIT(dx, ',')[dx_exploder.n])-1), '"', '')) as diag_code
        FROM txn transactional
            CROSS JOIN (SELECT EXPLODE(ARRAY(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11)) AS n) dx_exploder
        WHERE
            SPLIT(TRIM(transactional.dx), ',')[dx_exploder.n] IS NULL
            OR SPLIT(TRIM(transactional.dx), ',')[dx_exploder.n] != ''
    UNION ALL
        SELECT CONCAT(
                COALESCE(transactional.src_claim_id,''), '__',
                COALESCE(transactional.src_svc_id,'')
                ) as claim_svc_num, -- pk
            TRIM(REGEXP_REPLACE(SUBSTRING(SPLIT(other_diag_codes, ',')[other_diag_codes_exploder.n], 0,
                        locate('=>', SPLIT(other_diag_codes, ',')[other_diag_codes_exploder.n])-1), '"', '')) as diag_code
        FROM txn transactional
            CROSS JOIN (SELECT EXPLODE(ARRAY(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11)) AS n) other_diag_codes_exploder
        WHERE
            SPLIT(TRIM(transactional.other_diag_codes), ',')[other_diag_codes_exploder.n] IS NULL
            OR SPLIT(TRIM(transactional.other_diag_codes), ',')[other_diag_codes_exploder.n] != ''
    UNION ALL
        SELECT CONCAT(
                COALESCE(all_codes.src_claim_id,''), '__',
                COALESCE(all_codes.src_svc_id,'')
                ) as claim_svc_num,
            TRIM(SPLIT(all_codes.rest_of_codes, ',')[rest_of_codes_exploder.n]) as diag_code
        FROM (
            SELECT DISTINCT
                transactional.src_claim_id, transactional.src_svc_id, -- pk
                CONCAT(
                    COALESCE(transactional.prinpl_diag_cd, ''), ',',
                    COALESCE(transactional.admtg_diag_cd, ''), ',',
                    COALESCE(transactional.patnt_rsn_for_visit_01, ''), ',',
                    COALESCE(transactional.patnt_rsn_for_visit_02, ''), ',',
                    COALESCE(transactional.patnt_rsn_for_visit_03, '')
                    ) as rest_of_codes
            FROM txn transactional
            ) all_codes
            CROSS JOIN (SELECT EXPLODE(ARRAY(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11)) AS n) rest_of_codes_exploder
        WHERE
            SPLIT(TRIM(all_codes.rest_of_codes), ',')[rest_of_codes_exploder.n] IS NULL
            OR SPLIT(TRIM(all_codes.rest_of_codes), ',')[rest_of_codes_exploder.n] != ''
    )
