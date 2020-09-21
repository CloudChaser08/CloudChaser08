SELECT /*+ BROADCAST (ref_gen_ref)
 */
    DISTINCT
    enr.memberuid,
    CAST(enr.effectivedate AS DATE) as effectivedate,
    CAST(enr.terminationdate AS DATE) as terminationdate,
    CAST(CONCAT(1 + YEAR('{VDR_FILE_DT}'), '-12-31') AS DATE) AS max_terminationdate,
    enr.createddate,
    enr.productcode,
    enr.groupplantypecode,
    enr.acaindicator,
    enr.acaissuerstatecode,
    enr.acaonexchangeindicator,
    enr.acaactuarialvalue,
    enr.payergroupcode,
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
	'MEDICAL'                                                                           AS benefit_type
 FROM enr
 LEFT OUTER JOIN mbr
              ON enr.memberuid = mbr.memberuid
 LEFT OUTER JOIN matching_payload pay
              ON enr.memberuid = pay.claimid
 LEFT OUTER JOIN
    (
        SELECT gen_ref_1_dt
         FROM ref_gen_ref
        WHERE hvm_vdr_feed_id = 179
          AND gen_ref_domn_nm = 'EARLIEST_VALID_SERVICE_DATE'
        LIMIT 1
    ) esdt
   ON 1 = 1
WHERE UPPER(COALESCE(enr.memberuid, 'X')) <> 'MEMBERUID'
  AND COALESCE(medicalindicator, 'X') = '1'
UNION ALL
SELECT DISTINCT
    enr.memberuid,
    CAST(enr.effectivedate AS DATE) as effectivedate,
    CAST(enr.terminationdate AS DATE) as terminationdate,
    CAST(CONCAT(1 + YEAR('{VDR_FILE_DT}'), '-12-31') AS DATE) AS max_terminationdate,
    enr.createddate,
    enr.productcode,
    enr.groupplantypecode,
    enr.acaindicator,
    enr.acaissuerstatecode,
    enr.acaonexchangeindicator,
    enr.acaactuarialvalue,
    enr.payergroupcode,
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
	'PHARMACY'                                                                          AS benefit_type
 FROM enr
 LEFT OUTER JOIN mbr
              ON enr.memberuid = mbr.memberuid
 LEFT OUTER JOIN matching_payload pay
              ON enr.memberuid = pay.claimid
 LEFT OUTER JOIN
    (
        SELECT gen_ref_1_dt
         FROM ref_gen_ref
        WHERE hvm_vdr_feed_id = 179
          AND gen_ref_domn_nm = 'EARLIEST_VALID_SERVICE_DATE'
        LIMIT 1
    ) esdt
   ON 1 = 1
WHERE UPPER(COALESCE(enr.memberuid, 'X')) <> 'MEMBERUID'
  AND COALESCE(rxindicator, 'X') = '1'
