SELECT
    CASE WHEN hvid IS NULL THEN NULL
        WHEN LOWER(manufacturer) = 'amgen' THEN LOWER(obfuscate_hvid(hvid, 'LHv2'))
        ELSE LOWER(obfuscate_hvid(hvid, CONCAT('LHv2', COALESCE(LOWER(manufacturer), 'unknown'))))
    END                                         AS hvid,
    claimId                                     AS source_patient_id,
    source_name                                 AS source_name,
    brand                                       AS brand,
    manufacturer                                AS manufacturer,
    CASE WHEN isWeak IS NOT NULL AND CAST(isWeak as boolean) THEN 'true'
    ELSE 'false' END                            AS weak_match,
    CASE WHEN providerMatchId IS NULL THEN NULL
        WHEN LOWER(manufacturer) = 'amgen' THEN LOWER(obfuscate_hvid(providerMatchId, 'LHv2'))
        ELSE LOWER(obfuscate_hvid(providerMatchId, CONCAT('LHv2', COALESCE(LOWER(manufacturer), 'unknown'))))
    END                                         AS custom_hv_id,
    CASE WHEN matchStatus = 'multi_match' THEN
        CASE WHEN LOWER(manufacturer) = 'amgen'
                THEN LOWER(to_json(obfuscate_candidate_hvids(topCandidates, 'LHv2')))
            ELSE LOWER(to_json(obfuscate_candidate_hvids(topCandidates ,CONCAT('LHv2', COALESCE(LOWER(manufacturer), 'unknown')))))
        END
    END                                         AS matching_meta
FROM matching_payload
    LEFT JOIN liquidhub_raw USING (hvJoinKey)
