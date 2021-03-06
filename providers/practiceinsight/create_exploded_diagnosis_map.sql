DROP TABLE IF EXISTS exploded_ecodes;
CREATE TABLE exploded_ecodes AS
SELECT COALESCE(transactional.src_claim_id,'') || COALESCE(transactional.src_svc_id,''), -- pk
    TRIM(REPLACE(SUBSTRING(split_part(ecodes, ',', ecodes_exploder.n), 0, 
                charindex('=>', split_part(ecodes, ',', ecodes_exploder.n))), '"', '')) as ecodes
FROM transactional_raw transactional
    CROSS JOIN diagnosis_exploder ecodes_exploder
WHERE split_part(TRIM(transactional.ecodes), ',', ecodes_exploder.n) IS NULL
    OR split_part(TRIM(transactional.ecodes), ',', ecodes_exploder.n) != ''
    ;

DROP TABLE IF EXISTS exploded_dx;
CREATE TABLE exploded_dx AS
SELECT COALESCE(transactional.src_claim_id,'') || COALESCE(transactional.src_svc_id,''), -- pk
    TRIM(REPLACE(SUBSTRING(split_part(dx, ',', dx_exploder.n), 0, 
                charindex('=>', split_part(dx, ',', dx_exploder.n))), '"', '')) as dx
FROM transactional_raw transactional
    CROSS JOIN diagnosis_exploder dx_exploder
WHERE split_part(TRIM(transactional.dx), ',', dx_exploder.n) IS NULL
    OR split_part(TRIM(transactional.dx), ',', dx_exploder.n) != ''
    ;

DROP TABLE IF EXISTS exploded_other_diag_codes;
CREATE TABLE exploded_other_diag_codes AS
SELECT COALESCE(transactional.src_claim_id,'') || COALESCE(transactional.src_svc_id,''), -- pk
    TRIM(REPLACE(SUBSTRING(split_part(other_diag_codes, ',', other_diag_codes_exploder.n), 0, 
                charindex('=>', split_part(other_diag_codes, ',', other_diag_codes_exploder.n))), '"', '')) as other_diag_codes
FROM transactional_raw transactional
    CROSS JOIN diagnosis_exploder other_diag_codes_exploder
WHERE split_part(TRIM(transactional.other_diag_codes), ',', other_diag_codes_exploder.n) IS NULL
    OR split_part(TRIM(transactional.other_diag_codes), ',', other_diag_codes_exploder.n) != ''
    ;

DROP TABLE IF EXISTS exploded_rest_of_codes;
CREATE TABLE exploded_rest_of_codes AS
SELECT COALESCE(all_codes.src_claim_id,'') || COALESCE(all_codes.src_svc_id,''), 
    TRIM(split_part(all_codes.rest_of_codes, ',', rest_of_codes_exploder.n))
FROM (
    SELECT DISTINCT
        transactional.src_claim_id, transactional.src_svc_id, -- pk
        COALESCE(transactional.prinpl_diag_cd, '') || ',' || 
        COALESCE(transactional.admtg_diag_cd, '') || ',' || 
        COALESCE(transactional.patnt_rsn_for_visit_01, '') || ',' || 
        COALESCE(transactional.patnt_rsn_for_visit_02, '') || ',' || 
        COALESCE(transactional.patnt_rsn_for_visit_03, '') as rest_of_codes
    FROM transactional_raw transactional
        ) all_codes
    CROSS JOIN diagnosis_exploder rest_of_codes_exploder
WHERE split_part(TRIM(all_codes.rest_of_codes), ',', rest_of_codes_exploder.n) IS NULL
    OR split_part(TRIM(all_codes.rest_of_codes), ',', rest_of_codes_exploder.n) != ''
    ;

DROP TABLE IF EXISTS exploded_diag_codes_nulls;
CREATE TABLE exploded_diag_codes_nulls (
        claim_svc_num text,
        diag_code text
        ) DISTKEY(claim_svc_num) SORTKEY(claim_svc_num);

-- union will remove duplicates
INSERT INTO exploded_diag_codes_nulls (
    SELECT * FROM exploded_ecodes
    UNION
    SELECT * FROM exploded_dx
    UNION
    SELECT * FROM exploded_other_diag_codes
    UNION
    SELECT * FROM exploded_rest_of_codes
        )
    ;

-- strip out nulls for claim_svc_nums with populated codes
DROP TABLE IF EXISTS exploded_diag_codes;
CREATE TABLE exploded_diag_codes (
        claim_svc_num text,
        diag_code text
        ) DISTKEY(claim_svc_num) SORTKEY(claim_svc_num);
INSERT INTO exploded_diag_codes (
    SELECT DISTINCT claim_svc_num, diag_code 
    FROM exploded_diag_codes_nulls         
    WHERE diag_code IS NOT NULL
        OR claim_svc_num IN (
        SELECT claim_svc_num 
        FROM exploded_diag_codes_nulls
        GROUP BY claim_svc_num
        HAVING max(diag_code) IS NULL
            )
        )
    ;
