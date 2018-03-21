DROP TABLE IF EXISTS exploded_proc_codes_nulls;
CREATE TABLE exploded_proc_codes_nulls (claim_svc_num string,proc_code string);

-- insert other proc codes
INSERT INTO exploded_proc_codes_nulls (
    SELECT CONCAT(
            COALESCE(transactional.src_claim_id,''), '__',
            COALESCE(transactional.src_svc_id,'')
            ), -- pk
        TRIM(REGEXP_REPLACE(SUBSTRING(SPLIT(other_proc_codes, ',')[other_proc_codes_exploder.n], 
                    LOCATE('=>', SPLIT(other_proc_codes, ',')[other_proc_codes_exploder.n]) + 2, 
                    LENGTH(SPLIT(other_proc_codes, ',')[other_proc_codes_exploder.n])), '"', '')) as other_proc_codes
    FROM transactional_raw transactional
        CROSS JOIN diagnosis_exploder other_proc_codes_exploder
    WHERE SPLIT(TRIM(transactional.other_proc_codes), ',')[other_proc_codes_exploder.n] IS NULL
        OR SPLIT(TRIM(transactional.other_proc_codes), ',')[other_proc_codes_exploder.n] != ''
        );

-- insert rest of codes
INSERT INTO exploded_proc_codes_nulls (
    SELECT CONCAT(
            COALESCE(all_codes.src_claim_id,''), '__',
            COALESCE(all_codes.src_svc_id,'')
            ), 
        TRIM(SPLIT(all_codes.rest_of_codes, ',')[rest_of_codes_exploder.n])
    FROM (
        SELECT DISTINCT
            transactional.src_claim_id, transactional.src_svc_id, -- pk
            CONCAT(
                COALESCE(transactional.proc_cd,''), ',', 
                COALESCE(transactional.prinpl_proc_cd,'')
                ) as rest_of_codes
        FROM transactional_raw transactional
            ) all_codes
        CROSS JOIN diagnosis_exploder rest_of_codes_exploder
    WHERE SPLIT(TRIM(all_codes.rest_of_codes), ',')[rest_of_codes_exploder.n] IS NULL
        OR SPLIT(TRIM(all_codes.rest_of_codes), ',')[rest_of_codes_exploder.n] != ''
        );

-- strip out nulls for claim_svc_nums with populated codes, remove dupes
DROP VIEW IF EXISTS exploded_proc_codes;
DROP TABLE IF EXISTS exploded_proc_codes;
CREATE TABLE exploded_proc_codes (
        claim_svc_num string,
        proc_code string
        );
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
