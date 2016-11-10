DROP TABLE IF EXISTS exploded_other_proc_codes;
CREATE TABLE exploded_other_proc_codes AS
SELECT COALESCE(transactional.src_claim_id,'') || COALESCE(transactional.src_svc_id,''), -- pk
    TRIM(REPLACE(SUBSTRING(split_part(other_proc_codes, ',', other_proc_codes_exploder.n), 
                charindex('=>', split_part(other_proc_codes, ',', other_proc_codes_exploder.n)) + 2, 
                LENGTH(split_part(other_proc_codes, ',', other_proc_codes_exploder.n))), '"', '')) as other_proc_codes
FROM transactional_raw transactional
    CROSS JOIN diagnosis_exploder other_proc_codes_exploder
WHERE split_part(TRIM(transactional.other_proc_codes), ',', other_proc_codes_exploder.n) IS NULL
    OR split_part(TRIM(transactional.other_proc_codes), ',', other_proc_codes_exploder.n) != ''
    ;

DROP TABLE IF EXISTS exploded_rest_of_codes;
CREATE TABLE exploded_rest_of_codes AS
SELECT COALESCE(all_codes.src_claim_id,'') || COALESCE(all_codes.src_svc_id,''), 
    TRIM(split_part(all_codes.rest_of_codes, ',', rest_of_codes_exploder.n))
FROM (
    SELECT DISTINCT
        transactional.src_claim_id, transactional.src_svc_id, -- pk
        COALESCE(transactional.proc_cd,'') || ',' || 
        COALESCE(transactional.prinpl_proc_cd,'') as rest_of_codes
    FROM transactional_raw transactional
        ) all_codes
    CROSS JOIN diagnosis_exploder rest_of_codes_exploder
WHERE split_part(TRIM(all_codes.rest_of_codes), ',', rest_of_codes_exploder.n) IS NULL
    OR split_part(TRIM(all_codes.rest_of_codes), ',', rest_of_codes_exploder.n) != ''
    ;

DROP TABLE IF EXISTS exploded_proc_codes_nulls;
CREATE TABLE exploded_proc_codes_nulls (
        claim_svc_num text,
        proc_code text
        ) DISTKEY(claim_svc_num) SORTKEY(claim_svc_num);

-- union will remove duplicates
INSERT INTO exploded_proc_codes_nulls (
    SELECT * FROM exploded_other_proc_codes
    UNION
    SELECT * FROM exploded_rest_of_codes
        )
    ;

-- strip out nulls for claim_svc_nums with populated codes
DROP TABLE IF EXISTS exploded_proc_codes;
CREATE TABLE exploded_proc_codes (
        claim_svc_num text,
        proc_code text
        ) DISTKEY(claim_svc_num) SORTKEY(claim_svc_num);
INSERT INTO exploded_proc_codes (
    SELECT DISTINCT * 
    FROM exploded_proc_codes_nulls         
    WHERE proc_code IS NOT NULL
        OR claim_svc_num IN (
        SELECT claim_svc_num 
        FROM exploded_proc_codes_nulls
        GROUP BY claim_svc_num
        HAVING max(proc_code) IS NULL
            )
        )
    ;
