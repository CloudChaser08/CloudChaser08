SELECT
         upk.upk_key_1 AS upk_key_1
       , upk.upk_key_2 AS upk_key_2
       , MD5(CONCAT(pay.hvid, "hvidDHC")) AS hashed_hvid
FROM upk
INNER JOIN matching_payload pay
        ON upk.memberuid = pay.claimId