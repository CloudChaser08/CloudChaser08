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
	sub.ediclaim_id,
	sub.master_patient_id,
	sub.tenant_id,
	MAX(sub.input_file_name)                                                                AS input_file_name,
	sub.hvid,
	sub.yearofbirth,
	sub.gender,
	sub.diagnosis_code
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
    	txn.ediclaim_id,
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
                            )[diag_explode.n],
                        true
                    )
        END                                                                                     AS diagnosis_code
     FROM claims txn
     LEFT OUTER JOIN matching_payload pay
       ON txn.hvjoinkey = pay.hvjoinkey
    CROSS JOIN (SELECT EXPLODE(ARRAY(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11)) AS n) diag_explode
    WHERE LOWER(COALESCE(txn.billproviderid, '')) <> 'billproviderid'
      /* Laurie 7/18/19: Added filtering by the new HVM Approved flag. */
      AND SUBSTR(COALESCE(txn.hvm_approved, '0'), 1, 1) = '1'
      AND
      /* Select only where the diagnosis code is populated. */
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
                )[diag_explode.n] IS NOT NULL
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
	sub.ediclaim_id,
	sub.master_patient_id,
	sub.tenant_id,
	sub.hvid,
	sub.yearofbirth,
	sub.gender,
	sub.diagnosis_code