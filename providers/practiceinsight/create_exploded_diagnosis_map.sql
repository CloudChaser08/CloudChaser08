DROP TABLE IF EXISTS exploded_ecodes;
CREATE TABLE exploded_ecodes AS
SELECT transactional.src_claim_id || transactional.src_svc_id, -- pk
    replace(substring(split_part(ecodes, ',', ecodes_exploder.n), 0, 
            charindex('=>', split_part(ecodes, ',', ecodes_exploder.n))), '"', '') as ecodes
FROM transactional_raw transactional
    CROSS JOIN diagnosis_exploder ecodes_exploder
WHERE split_part(trim(transactional.ecodes), ',', ecodes_exploder.n) IS NOT NULL
    AND split_part(trim(transactional.ecodes), ',', ecodes_exploder.n) != ''
    ;

DROP TABLE IF EXISTS exploded_dx;
CREATE TABLE exploded_dx AS
SELECT transactional.src_claim_id || transactional.src_svc_id, -- pk
    replace(substring(split_part(dx, ',', dx_exploder.n), 0, 
            charindex('=>', split_part(dx, ',', dx_exploder.n))), '"', '') as dx
FROM transactional_raw transactional
    CROSS JOIN diagnosis_exploder dx_exploder
WHERE split_part(trim(transactional.dx), ',', dx_exploder.n) IS NOT NULL
    AND split_part(trim(transactional.dx), ',', dx_exploder.n) != ''
    ;

DROP TABLE IF EXISTS exploded_other_diag_codes;
CREATE TABLE exploded_other_diag_codes AS
SELECT transactional.src_claim_id || transactional.src_svc_id, -- pk
    replace(substring(split_part(other_diag_codes, ',', other_diag_codes_exploder.n), 0, 
            charindex('=>', split_part(other_diag_codes, ',', other_diag_codes_exploder.n))), '"', '') as other_diag_codes
FROM transactional_raw transactional
    CROSS JOIN diagnosis_exploder other_diag_codes_exploder
WHERE split_part(trim(transactional.other_diag_codes), ',', other_diag_codes_exploder.n) IS NOT NULL
    AND split_part(trim(transactional.other_diag_codes), ',', other_diag_codes_exploder.n) != ''
    ;

DROP TABLE IF EXISTS exploded_rest_of_codes;
CREATE TABLE exploded_rest_of_codes AS
SELECT all_codes.src_claim_id || all_codes.src_svc_id, 
    split_part(all_codes.rest_of_codes, ',', rest_of_codes_exploder.n)
FROM (
    SELECT DISTINCT
        transactional.src_claim_id, transactional.src_svc_id, -- pk
        transactional.prinpl_diag_cd || ',' || 
        transactional.admtg_diag_cd || ',' || 
        transactional.patnt_rsn_for_visit_01 || ',' || 
        transactional.patnt_rsn_for_visit_02 || ',' || 
        transactional.patnt_rsn_for_visit_03 as rest_of_codes
    FROM transactional_raw transactional
        ) all_codes
    CROSS JOIN diagnosis_exploder rest_of_codes_exploder
WHERE split_part(trim(all_codes.rest_of_codes), ',', rest_of_codes_exploder.n) IS NOT NULL
    AND split_part(trim(all_codes.rest_of_codes), ',', rest_of_codes_exploder.n) != ''
        ;

DROP TABLE IF EXISTS exploded_diag_codes;
CREATE TABLE exploded_diag_codes (
        claim_svc_num text,
        diag_code text
        ) DISTKEY(claim_svc_num) SORTKEY(claim_svc_num);

-- union will remove duplicates
INSERT INTO exploded_diag_codes (
    SELECT * FROM exploded_ecodes
    UNION
    SELECT * FROM exploded_dx
    UNION
    SELECT * FROM exploded_other_diag_codes
    UNION
    SELECT * FROM exploded_rest_of_codes
        )
;
