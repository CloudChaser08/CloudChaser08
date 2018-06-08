SELECT
    CASE WHEN hvid IS NOT NULL THEN LOWER(obfuscate_hvid(hvid, 'LHv2'))
    END                                         AS hvid,
    claimId                                     AS lhid,
    pharmacy_name                               AS pharmacy_name,
    brand                                       AS brand,
    CASE WHEN isWeak THEN 'true'
    ELSE 'false' END                            AS weak_match,
    CASE WHEN providerMatchId IS NOT NULL THEN  LOWER(obfuscate_hvid(providerMatchId, 'LHv2'))
    END                                         AS provider_specific_id,
    CASE WHEN matchStatus = 'multi_match' THEN
        LOWER(to_json(obfuscate_candidate_hvids(topCandidates, 'LHv2')))
    END                                         AS matching_meta_data
FROM matching_payload
    LEFT JOIN liquidhub_raw USING (hvJoinKey)
