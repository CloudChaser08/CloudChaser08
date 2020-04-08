/** This table will go to the warehouse */
SELECT *
FROM esi_join_transactions_and_payload_dx
WHERE instr(hvid, '_') = 0
