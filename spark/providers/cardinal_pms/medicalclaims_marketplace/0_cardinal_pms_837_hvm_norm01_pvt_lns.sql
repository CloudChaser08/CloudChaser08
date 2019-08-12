SELECT
	sub.billproviderid,
	sub.billprovideridqualifier,
	sub.billprovidername,
	sub.billprovidernpid,
	sub.billprovidertaxonomycode,
	sub.diagnosiseight,
	sub.diagnosisfive,
	sub.diagnosisfour,
	sub.diagnosisseven,
	sub.diagnosissix,
	sub.diagnosisthree,
	sub.diagnosistwo,
	sub.facilitycode,
	sub.patientdob,
	sub.patientgender,
	sub.payerid,
	sub.payername,
	sub.principaldiagnosis,
	sub.submittedchargetotal,
	sub.diagnosisnine,
	sub.diagnosisten,
	sub.diagnosiseleven,
	sub.diagnosistwelve,
	sub.dateservicestart,
	sub.ediclaim_id,
	sub.id_3,
	sub.linesequencenumber,
	sub.linkeddiagnosisfour,
	sub.linkeddiagnosisone,
	sub.linkeddiagnosisthree,
	sub.linkeddiagnosistwo,
	sub.procedurecode,
	sub.procedurecodequalifier,
	sub.proceduremodifierfour,
	sub.proceduremodifierone,
	sub.proceduremodifierthree,
	sub.proceduremodifiertwo,
	sub.referringproviderid,
	sub.referringprovideridqualifier,
	sub.referringprovidername,
	sub.renderingproviderid,
	sub.renderingprovideridqualifier,
	sub.renderingprovidername,
	sub.renderingprovidernpid,
	sub.renderingprovidertaxonomycode,
	sub.servicefacilityaddress,
	sub.servicefacilitycity,
	sub.servicefacilityid,
	sub.servicefacilityidqualifier,
	sub.servicefacilityname,
	sub.servicefacilitystate,
	sub.servicefacilityzip,
	sub.submittedcharge,
	sub.submittedunits,
	sub.product_service_id_qualifier,
	sub.product_service_id,
	sub.master_patient_id,
	sub.tenant_id,
	MAX(sub.input_file_name)                                                                AS input_file_name,
	sub.hvid,
	sub.yearofbirth,
	sub.gender,
	sub.diagnosis_code,
	MIN(sub.diagnosis_pointer)                                                              AS diagnosis_pointer
FROM
(
    SELECT
    	txn.billproviderid,
    	txn.billprovideridqualifier,
    	txn.billprovidername,
    	txn.billprovidernpid,
    	txn.billprovidertaxonomycode,
    	txn.diagnosiseight,
    	txn.diagnosisfive,
    	txn.diagnosisfour,
    	txn.diagnosisseven,
    	txn.diagnosissix,
    	txn.diagnosisthree,
    	txn.diagnosistwo,
    	txn.facilitycode,
    	txn.patientdob,
    	txn.patientgender,
    	txn.payerid,
    	txn.payername,
    	txn.principaldiagnosis,
    	txn.submittedchargetotal,
    	txn.diagnosisnine,
    	txn.diagnosisten,
    	txn.diagnosiseleven,
    	txn.diagnosistwelve,
    	txn.dateservicestart,
    	txn.ediclaim_id,
    	txn.id_3,
    	txn.linesequencenumber,
    	txn.linkeddiagnosisfour,
    	txn.linkeddiagnosisone,
    	txn.linkeddiagnosisthree,
    	txn.linkeddiagnosistwo,
    	txn.procedurecode,
    	txn.procedurecodequalifier,
    	txn.proceduremodifierfour,
    	txn.proceduremodifierone,
    	txn.proceduremodifierthree,
    	txn.proceduremodifiertwo,
    	txn.referringproviderid,
    	txn.referringprovideridqualifier,
    	txn.referringprovidername,
    	txn.renderingproviderid,
    	txn.renderingprovideridqualifier,
    	txn.renderingprovidername,
    	txn.renderingprovidernpid,
    	txn.renderingprovidertaxonomycode,
    	txn.servicefacilityaddress,
    	txn.servicefacilitycity,
    	txn.servicefacilityid,
    	txn.servicefacilityidqualifier,
    	txn.servicefacilityname,
    	txn.servicefacilitystate,
    	txn.servicefacilityzip,
    	txn.submittedcharge,
    	txn.submittedunits,
    	txn.product_service_id_qualifier,
    	txn.product_service_id,
    	txn.master_patient_id,
    	txn.tenant_id,
    	txn.input_file_name,
    	pay.hvid,
    	pay.yearofbirth,
    	pay.gender,
    	/* diagnosis_code */
    	/* Leave the privacy filtering to the final normalization */
    	/* so we can accurately add the claim-level diagnoses.    */
        CASE
            WHEN ARRAY
                    (
                        txn.linkeddiagnosisone,
                        txn.linkeddiagnosistwo,
                        txn.linkeddiagnosisthree,
                        txn.linkeddiagnosisfour
                    )[diag_explode.n] IS NULL
                 THEN NULL
            ELSE CLEAN_UP_FREETEXT
                    (
                        ARRAY
                            (
                                txn.principaldiagnosis,
                                txn.diagnosistwo,
                                txn.diagnosisthree,
                                txn.diagnosisfour,
                                txn.diagnosisfive,
                                txn.diagnosissix,
                                txn.diagnosisseven,
                                txn.diagnosiseight,
                                txn.diagnosisnine,
                                txn.diagnosisten,
                                txn.diagnosiseleven,
                                txn.diagnosistwelve
                            )
                                [
                                    CAST(-1 AS INTEGER) +
                                    CAST
                                        (
                                            COALESCE
                                                (
                                                    ARRAY
                                                        (
                                                            txn.linkeddiagnosisone,
                                                            txn.linkeddiagnosistwo,
                                                            txn.linkeddiagnosisthree,
                                                            txn.linkeddiagnosisfour
                                                        )[diag_explode.n], '1'
                                                ) AS INTEGER
                                        )
                                ],
                        true
                    )
        END                                                                                     AS diagnosis_code,
        1 + diag_explode.n                                                                      AS diagnosis_pointer
     FROM claims txn
     LEFT OUTER JOIN matching_payload pay
       ON txn.hvjoinkey = pay.hvjoinkey
    CROSS JOIN (SELECT EXPLODE(ARRAY(0, 1, 2, 3)) AS n) diag_explode
    WHERE LOWER(COALESCE(txn.billproviderid, '')) <> 'billproviderid'
      /* Laurie 7/18/19: Added filtering by the new HVM Approved flag. */
      AND SUBSTR(COALESCE(txn.hvm_approved, '0'), 1, 1) = '1'
      AND
    ---------- Diagnosis code explosion
        (
            ARRAY
                (
                    txn.linkeddiagnosisone,
                    txn.linkeddiagnosistwo,
                    txn.linkeddiagnosisthree,
                    txn.linkeddiagnosisfour
                )[diag_explode.n] IS NOT NULL
         OR
            (
                COALESCE
                    (
                        txn.linkeddiagnosisone,
                        txn.linkeddiagnosistwo,
                        txn.linkeddiagnosisthree,
                        txn.linkeddiagnosisfour
                    ) IS NULL
            AND diag_explode.n = 0
            )
        )
) sub
GROUP BY
	sub.billproviderid,
	sub.billprovideridqualifier,
	sub.billprovidername,
	sub.billprovidernpid,
	sub.billprovidertaxonomycode,
	sub.diagnosiseight,
	sub.diagnosisfive,
	sub.diagnosisfour,
	sub.diagnosisseven,
	sub.diagnosissix,
	sub.diagnosisthree,
	sub.diagnosistwo,
	sub.facilitycode,
	sub.patientdob,
	sub.patientgender,
	sub.payerid,
	sub.payername,
	sub.principaldiagnosis,
	sub.submittedchargetotal,
	sub.diagnosisnine,
	sub.diagnosisten,
	sub.diagnosiseleven,
	sub.diagnosistwelve,
	sub.dateservicestart,
	sub.ediclaim_id,
	sub.id_3,
	sub.linesequencenumber,
	sub.linkeddiagnosisfour,
	sub.linkeddiagnosisone,
	sub.linkeddiagnosisthree,
	sub.linkeddiagnosistwo,
	sub.procedurecode,
	sub.procedurecodequalifier,
	sub.proceduremodifierfour,
	sub.proceduremodifierone,
	sub.proceduremodifierthree,
	sub.proceduremodifiertwo,
	sub.referringproviderid,
	sub.referringprovideridqualifier,
	sub.referringprovidername,
	sub.renderingproviderid,
	sub.renderingprovideridqualifier,
	sub.renderingprovidername,
	sub.renderingprovidernpid,
	sub.renderingprovidertaxonomycode,
	sub.servicefacilityaddress,
	sub.servicefacilitycity,
	sub.servicefacilityid,
	sub.servicefacilityidqualifier,
	sub.servicefacilityname,
	sub.servicefacilitystate,
	sub.servicefacilityzip,
	sub.submittedcharge,
	sub.submittedunits,
	sub.product_service_id_qualifier,
	sub.product_service_id,
	sub.master_patient_id,
	sub.tenant_id,
	sub.hvid,
	sub.yearofbirth,
	sub.gender,
	sub.diagnosis_code