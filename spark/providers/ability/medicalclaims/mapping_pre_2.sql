SELECT diagnosiscode, `type`, claimid
FROM transactional_diagnosis diagnosis
LEFT JOIN medicalclaims_common_model m2
    ON m2.diagnosis_code = diagnosiscode
    AND m2.claim_id = claimid
WHERE claim_id IS NULL AND diagnosis_code IS NULL
