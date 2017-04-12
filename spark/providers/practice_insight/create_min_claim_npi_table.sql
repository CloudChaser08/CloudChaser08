DROP TABLE IF EXISTS claim_min_npi_map;
CREATE TABLE claim_min_npi_map (
        src_claim_id string,
        rendr_provdr_npi string,
        refrn_provdr_npi string,
        fclty_npi string
        )
;

INSERT INTO claim_min_npi_map
SELECT
    src_claim_id,
    MIN(CASE
        WHEN TRIM(rendr_provdr_npi) = '' THEN NULL
        ELSE rendr_provdr_npi
        END),
    MIN(CASE
        WHEN TRIM(refrn_provdr_npi) = '' THEN NULL
        ELSE refrn_provdr_npi
        END),
    MIN(CASE
        WHEN TRIM(fclty_npi) = '' THEN NULL
        ELSE fclty_npi
        END)
FROM transactional_raw
GROUP BY src_claim_id
;
