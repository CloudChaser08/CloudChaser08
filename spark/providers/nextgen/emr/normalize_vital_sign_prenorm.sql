SELECT *,
    ARRAY(
        'SYSTOLIC',
        'DIASTOLIC',
        'PULSE',
        'BMI',
        'BMI',
        'O2_SATURATION',
        'O2_SATURATION',
        'RESPIRATION_FLOW',
        'RESPIRATION_FLOW',
        'BODY_TEMPERATURE',
        'RESPIRATION',
        'STANFORD_HAQ',
        'PAIN',
        'HEIGHT',
        'WEIGHT'
    ) as vit_sign_typ_cd,
    ARRAY(
        'mmHg',
        'mmHg',
        'BEATS_PER_MINUTE',
        'INDEX',
        'PERCENT',
        'PERCENT',
        'TIMING',
        'RATE',
        'TIMING',
        'FAHRENHEIT',
        'BREATHS_PER_MINUTE',
        'SCORE',
        '0_THROUGH_10',
        'INCHES',
        'POUNDS'
    ) as vit_sign_uom,
    ARRAY(
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        vsn.heightdate,
        NULL
    ) as vit_sign_last_msrmt_dt,
    ARRAY(
        vsn.systolic,
        vsn.diastolic,
        vsn.pulserate,
        vsn.bmi,
        vsn.bmipercent,
        vsn.spo2dtl,
        vsn.spo2timingid,
        vsn.peakflow,
        vsn.peakflowtiming,
        vsn.tempdegf,
        vsn.respirationrate,
        vsn.haqscore,
        vsn.pain,
        CASE WHEN vsn.heightft IS NOT NULL OR vsn.heightin IS NOT NULL
                THEN coalesce(extract_number(vsn.heightft), 0) * 12 + coalesce(extract_number(vsn.heightin), 0)
            WHEN vsn.heightcm IS NOT NULL
                THEN floor(extract_number(vsn.heightcm) * 2.54)
            END,
        CASE WHEN vsn.weightlb IS NOT NULL
                THEN extract_number(vsn.weightlb)
            WHEN vsn.weightkg IS NOT NULL
                THEN floor(extract_number(vsn.weightkg) * 2.2)
            END
    ) as vit_sign_msrmt
FROM vitalsigns vsn
