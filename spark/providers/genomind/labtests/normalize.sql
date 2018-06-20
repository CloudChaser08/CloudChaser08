SELECT
    /*FIELDS*/
FROM patient ptn
LEFT OUTER JOIN matching_payload pay ON ptn.hvJoinKey = pay.hvJoinKey
LEFT OUTER JOIN clinicians ON ptn.patientkey = cln.patientkey
