SELECT
    -- HVIDs and custom IDs need to be linkable between different sources for
    -- the same manufacturer. For different manufacturers, they can't link.
    -- Manufacturer 'Amgen' has to be obfuscated differently because we went
    -- live with them before the new requirements/spec were established
    CASE WHEN hvid IS NULL THEN NULL
        WHEN LOWER(manufacturer) = 'amgen' THEN LOWER(obfuscate_hvid(hvid, 'LHv2'))
        ELSE obfuscate_hvid(hvid, CONCAT('LHv2', COALESCE(LOWER(manufacturer), 'unknown')))
    END                                         AS hvid,
    {source_patient_id_col}                     AS source_patient_id,
    source_name                                 AS source_name,
    brand                                       AS brand,
    manufacturer                                AS manufacturer,
    CAST(NULL AS STRING)                        AS filler_1,
    CAST(NULL AS STRING)                        AS filler_2,
    CAST(NULL AS STRING)                        AS filler_3,
    CAST(NULL AS STRING)                        AS filler_4,
    CAST(NULL AS STRING)                        AS filler_5,
    CAST(NULL AS STRING)                        AS filler_6,
    CAST(NULL AS STRING)                        AS filler_7,
    CASE WHEN isWeak IS NOT NULL AND CAST(isWeak as boolean) THEN 'true'
    ELSE 'false' END                            AS weak_match,
    CASE WHEN providerMatchId IS NULL THEN NULL
        WHEN LOWER(manufacturer) = 'amgen' THEN obfuscate_hvid(providerMatchId, 'LHv2')
        ELSE obfuscate_hvid(providerMatchId, CONCAT('LHv2', COALESCE(LOWER(manufacturer), 'unknown')))
    END                                         AS custom_hv_id,
    CAST(NULL AS STRING)                        AS provider_meta,
    CASE WHEN matchStatus = 'multi_match' THEN
        CASE WHEN LOWER(manufacturer) = 'amgen'
                THEN LOWER(to_json(obfuscate_candidate_hvids(topCandidates, 'LHv2')))
            ELSE to_json(obfuscate_candidate_hvids(topCandidates, CONCAT('LHv2', COALESCE(LOWER(manufacturer), 'unknown'))))
        END
    END                                         AS matching_meta
FROM matching_payload
    LEFT JOIN liquidhub_raw USING (hvJoinKey)
