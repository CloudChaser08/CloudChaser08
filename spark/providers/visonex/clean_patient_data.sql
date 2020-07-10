DROP TABLE IF EXISTS clean_patientdata;
CREATE TABLE clean_patientdata AS (
    SELECT
    analyticrowidnumber,
    clinicorganizationidnumber,
    defaultclinicidnumber,
    patientidnumber,
    pd.patientid,
    hvid,
    medicalrecordnumber,
    labidnumber,
    pd.state,
    mask_zip_code(substring(zipcode, 1, 3)) as zipcode,
    sex,
    status_original,
    patientstatus,
    statusisactive,
    primarymodality,
    primarymodality_original,
    primarydialysissetting,
    extract_date(substring(datefirstdialysis, 1, 10), '%Y-%m-%d') as datefirstdialysis,
    extract_date(substring(laststatuschangedate, 1, 10), '%Y-%m-%d') as laststatuschangedate,
    tribecode,
    extract_date(substring(inactivatedate, 1, 10), '%Y-%m-%d') as inactivatedate,
    cap_age(pd.age) as age,
    monthsindialysis,
    transplantwaitlist,
    medicalcoveragemedicare,
    extract_date(substring(medicalcoveragemedicareeffectivedate, 1, 10), '%Y-%m-%d') as medicalcoveragemedicareeffectivedate,
    masterpatientidnumber,
    extract_date(substring(datefirstdialysiscurrentunit, 1, 10), '%Y-%m-%d') as datefirstdialysiscurrentunit
    FROM patientdata pd
    LEFT JOIN matching_payload ON analyticrowidnumber = claimId
);