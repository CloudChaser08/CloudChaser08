SELECT
    LOWER(obfuscate_hvid(matching_payload.hvid, {salt}))            AS HVID,
    matching_payload.personId                                        AS GUID,
    transactions.ubcapp                                              AS UBCApp,
    transactions.ubcdb                                               AS UBCDB,
    transactions.ubcprogram                                          AS UBCProgram
FROM transactions
    INNER JOIN matching_payload
        USING (hvjoinkey)
