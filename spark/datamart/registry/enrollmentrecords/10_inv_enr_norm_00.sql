SELECT DISTINCT
    enr.memberuid,
    enr.effectivedate,
    enr.terminationdate ,
    enr.createddate,
    enr.productcode,
    enr.groupplantypecode,
    enr.acaindicator,
    enr.acaissuerstatecode,
    enr.acaonexchangeindicator,
    enr.acaactuarialvalue,
    enr.payergroupcode,
    enr.payertypecode,
    enr.input_file_name,
	mbr.statecode,
    mbr.birthyear,
    mbr.zip3value,
    mbr.gendercode,
	pay.age,
    pay.state,
    pay.hvid,
    pay.yearofbirth,
    pay.threedigitzip,
    pay.gender,
	'MEDICAL'                                      AS benefit_type
 FROM enr
LEFT OUTER JOIN mbr ON enr.memberuid = mbr.memberuid
--LEFT OUTER JOIN matching_payload pay ON enr.memberuid = pay.claimid
INNER JOIN matching_payload pay ON enr.memberuid = pay.claimid
--INNER JOIN _mom_cohort mom ON pay.hvid = mom.hvid
WHERE UPPER(COALESCE(enr.memberuid, 'X')) <> 'MEMBERUID'
  AND COALESCE(medicalindicator, 'X') = '1'
UNION ALL
SELECT DISTINCT
    enr.memberuid,
    enr.effectivedate,
    enr.terminationdate,
    enr.createddate,
    enr.productcode,
    enr.groupplantypecode,
    enr.acaindicator,
    enr.acaissuerstatecode,
    enr.acaonexchangeindicator,
    enr.acaactuarialvalue,
    enr.payergroupcode,
    enr.payertypecode,
    enr.input_file_name,
	mbr.statecode,
    mbr.birthyear,
    mbr.zip3value,
    mbr.gendercode,
	pay.age,
    pay.state,
    pay.hvid,
    pay.yearofbirth,
    pay.threedigitzip,
    pay.gender,
	'PHARMACY'                                         AS benefit_type
 FROM enr
LEFT OUTER JOIN mbr ON enr.memberuid = mbr.memberuid
--LEFT OUTER JOIN matching_payload pay ON enr.memberuid = pay.claimid
INNER JOIN matching_payload pay ON enr.memberuid = pay.claimid
INNER JOIN _mom_cohort mom ON pay.hvid = mom.hvid
WHERE UPPER(COALESCE(enr.memberuid, 'X')) <> 'MEMBERUID'
  AND COALESCE(rxindicator, 'X') = '1'
