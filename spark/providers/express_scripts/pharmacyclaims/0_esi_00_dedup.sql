SELECT
    TRIM(txn.pharmacy_claim_id)                                         as pharmacy_claim_id,
    txn.compound_code,
    txn.date_of_service,
    txn.date_prescription_written,
    txn.days_supply,
    txn.diagnosis_code,
    txn.diagnosis_code_qualifier,
    txn.dispense_as_written,
    txn.fill_number,
    txn.level_of_service,
    txn.number_of_refills_authorized,
    txn.patient_state,
    txn.prescriber_id,
    txn.prescriber_id_qualifier,
    txn.prescription_service_reference_number_qualifier,
    txn.processor_control_number,
    txn.product_service_id,
    txn.product_service_id_qualifier,
    txn.quantity_dispensed,
    txn.reject_code_1,
    txn.reject_code_2,
    txn.reject_code_3,
    txn.reject_code_4,
    txn.reject_code_5,
    txn.service_provider_id,
    txn.service_provider_id_qualifier,
    txn.transaction_code,
    txn.unit_dose_indicator,
    txn.unit_of_measure,
    payload.age,
    payload.gender,
    payload.hvid,
    payload.rxnumber,
    payload.state,
    payload.threeDigitZip,
    payload.yearOfBirth,
    MAX(txn.input_file_name)                                            as input_file_name
FROM transaction txn
    LEFT OUTER JOIN matching_payload payload
        ON TRIM(txn.hvjoinkey) = TRIM(payload.hvjoinkey)
WHERE
    TRIM(UPPER(COALESCE(txn.pharmacy_claim_id, 'empty'))) <> 'CLAIM_ID'
GROUP BY
    1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,
    21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36