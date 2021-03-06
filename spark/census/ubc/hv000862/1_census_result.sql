SELECT
    LOWER(obfuscate_hvid(matching_payload.hvid, {salt}))             AS HVID,
    matching_payload.personId                                        AS GUID,
    transactions.UBCApp                                              AS UBCApp,
    transactions.UBCDB                                               AS UBCDB,
    transactions.UBCProgram                                          AS UBCProgram
FROM transactions
    INNER JOIN matching_payload
        USING (hvjoinkey)
