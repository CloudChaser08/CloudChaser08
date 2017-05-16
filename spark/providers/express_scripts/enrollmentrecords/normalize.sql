INSERT INTO enrollment_common_model SELECT
    monotonically_increasing_id(),
    phi.hvid,
    {today},
    '1',
    {filename},
    {feedname},
    {vendor},
    NULL,
    NULL,
    phi.year_of_birth,
    phi.zip,
    zip3.state,
    phi.gender,
    NULL,
    NULL,
    extract_date(e.operation_date, '%Y%m%d'),
    extract_date(e.start_date, '%Y%m%d'),
    extract_date(e.end_date, '%Y%m%d'),
    NULL
FROM enrollment_records e
    LEFT JOIN matching_payload mp ON e.hv_join_key = mp.hvJoinKey
    INNER JOIN local_phi phi ON mp.patientId = phi.patient_id
    LEFT JOIN zip3_to_state zip3 ON zip3.zip3 = phi.zip;
