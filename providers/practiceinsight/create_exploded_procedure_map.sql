DROP TABLE IF EXISTS exploded_other_proc_codes;
CREATE TABLE exploded_other_proc_codes AS
SELECT transactional.src_claim_id || transactional.src_svc_id, -- pk
    replace(substring(split_part(other_proc_codes, ',', other_proc_codes_exploder.n), 
            charindex('=>', split_part(other_proc_codes, ',', other_proc_codes_exploder.n)) + 2, 
            length(split_part(other_proc_codes, ',', other_proc_codes_exploder.n))), '"', '') as other_proc_codes
FROM transactional_raw transactional
    CROSS JOIN diagnosis_exploder other_proc_codes_exploder
WHERE split_part(trim(transactional.other_proc_codes), ',', other_proc_codes_exploder.n) IS NOT NULL
    AND split_part(trim(transactional.other_proc_codes), ',', other_proc_codes_exploder.n) != ''
    ;

DROP TABLE IF EXISTS exploded_rest_of_codes;
CREATE TABLE exploded_rest_of_codes AS
SELECT all_codes.src_claim_id || all_codes.src_svc_id, 
    split_part(all_codes.rest_of_codes, ',', rest_of_codes_exploder.n)
FROM (
    SELECT DISTINCT
        transactional.src_claim_id, transactional.src_svc_id, -- pk
        transactional.proc_cd || ',' || 
        transactional.principal_proc_cd as rest_of_codes
    FROM transactional_raw transactional
        ) all_codes
    CROSS JOIN diagnosis_exploder rest_of_codes_exploder
WHERE split_part(trim(all_codes.rest_of_codes), ',', rest_of_codes_exploder.n) IS NOT NULL
    AND split_part(trim(all_codes.rest_of_codes), ',', rest_of_codes_exploder.n) != ''
        ;

DROP TABLE IF EXISTS exploded_proc_codes;
CREATE TABLE exploded_proc_codes (
        claim_svc_num text,
        proc_code text
        ) DISTKEY(claim_svc_num) SORTKEY(claim_svc_num);

-- union will remove duplicates
INSERT INTO exploded_proc_codes (
    SELECT * FROM exploded_other_proc_codes
    UNION
    SELECT * FROM exploded_rest_of_codes
        )
;
