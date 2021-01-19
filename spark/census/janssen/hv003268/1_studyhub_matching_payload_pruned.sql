SELECT *
FROM matching_payload_studyhub
WHERE hvid is not null
    AND eConsentHVID NOT IN (
        SELECT eConsentHVID
        FROM (
            SELECT eConsentHVID, count(*) as row_count
            FROM matching_payload_studyhub
            WHERE eConsentHVID IS NOT NULL
            GROUP BY eConsentHVID
            HAVING row_count > 1
        ) x2
    )
