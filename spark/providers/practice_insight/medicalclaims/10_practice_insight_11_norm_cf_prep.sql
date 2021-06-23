--
-- exclude redundant rows from the table!
--
-- we want to exclude rows where the diagnosis_code on that row exists
-- elsewhere on the claim where the service_line_number is not null.
--
-- i.e. we don't want to keep duplicate diagnosis_code values that are
-- associated with null service lines on a claim if those values
-- already exist on the same claim associated with a non-null service
-- line
--

-- rows with a non-null service_line_number are OK
    SELECT
        *
    FROM
        practice_insight_10_norm_clm_srv_prefinal
    WHERE
        service_line_number IS NOT NULL
UNION ALL
    -- if the service_line_number is null, only keep rows that contain
    -- diagnoses that don't exist elsewhere on one of the claim's service
    -- lines
    SELECT
        base.*
    FROM
        practice_insight_10_norm_clm_srv_prefinal base
        LEFT OUTER JOIN
        (
            SELECT claim_id,
                COLLECT_SET(COALESCE(diagnosis_code, '<NULL>')) as codes
            FROM
                practice_insight_10_norm_clm_srv_prefinal
            WHERE
                service_line_number IS NOT NULL
            GROUP BY claim_id
        ) claim_code ON base.claim_id = claim_code.claim_id
    WHERE base.service_line_number IS NULL
        AND base.diagnosis_code IS NOT NULL
        AND (NOT ARRAY_CONTAINS(
            claim_code.codes,
            COALESCE(base.diagnosis_code, '<NULL>')
            ) OR claim_code.codes IS NULL)
UNION ALL
    -- we will still need to add in rows where there are no diagnosis
    -- codes or service lines
    SELECT
        base.*
    FROM
        practice_insight_10_norm_clm_srv_prefinal base
        INNER JOIN
        (
            SELECT claim_id,
                COLLECT_SET(
                    CAST(COALESCE(diagnosis_code, '<NULL>') AS STRING)
                    ) as codes
            FROM
                practice_insight_10_norm_clm_srv_prefinal
            WHERE
                service_line_number IS NULL
            GROUP BY claim_id
        ) claim_code ON base.claim_id = claim_code.claim_id
    WHERE base.service_line_number IS NULL
        AND SIZE(claim_code.codes) = 1 AND claim_code.codes[0] = '<NULL>'