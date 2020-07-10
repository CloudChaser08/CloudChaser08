DROP TABLE IF EXISTS clean_address;
CREATE TABLE clean_address AS (
SELECT
    analyticrowidnumber as record_id,
    clinicorganizationidnumber,
    hvid,
    patientdataanalyticrowidnumber,
    state,
    mask_zip_code(substring(zipcode, 1, 3)) as zipcode,
    patientaddressidnumber,
    patientidnumber,
    extract_date(substring(inactivatedate, 1, 10), '%Y-%m-%d') as inactivatedate,
    associationidnumber
FROM address
);

DROP TABLE IF EXISTS clean_clinicpreference;
CREATE TABLE clean_clinicpreference AS (
SELECT
    analyticrowidnumber as record_id,
    clinicorganizationidnumber,
    associationidnumber,
    labmethodalbumin,
    serumalbuminlower,
    hemodialysismethod,
    peritonealdialysismethod,
    pnameasure,
    bsamethod,
    creatineclearancemethod,
    extract_date(substring(inactivatedate, 1, 10), '%Y-%m-%d') as inactivatedate
FROM clinicpreference
);

DROP TABLE IF EXISTS clean_dialysistraining;
CREATE TABLE clean_dialysistraining AS (
    SELECT analyticrowidnumber as record_id,
           clinicorganizationidnumber,
           hvid,
           patientdataanalyticrowidnumber,
           cklresultid,
           patientidnumber,
           analyticdos,
           extract_date(substring(addeddate, 1, 10), '%Y-%m-%d')      as addeddate,
           extract_date(substring(editdate, 1, 10), '%Y-%m-%d')       as editdate,
           trainingtype,
           othertraining,
           expectedselfcare,
           dialysisperiod,
           dialysistrainingstart,
           dialysistrainingend,
           extract_date(substring(inactivatedate, 1, 10), '%Y-%m-%d') as inactivatedate
    FROM dialysistraining
);

DROP TABLE IF EXISTS clean_dialysistreatment;
CREATE TABLE clean_dialysistreatment AS (
    SELECT analyticrowidnumber as record_id,
           clinicorganizationidnumber,
           hvid,
           patientdataanalyticrowidnumber,
           runidnumber,
           patientidnumber,
           case
                when status_original = 'Deceased' then substr(analyticdos, 1, 7)
                else analyticdos
            end as analyticdos,
           status_original,
           patientstatus,
           primarydialysissetting,
           clinicidnumber,
           locationid,
           outpatientrun,
           case when status_original = 'Deceased' then substr(treatmentstart, 1, 7)
                else substr(treatmentstart, 1, 10)
           end as treatmentstart,
           case when status_original = 'Deceased' then substr(treatmentend, 1, 7)
                else substr(treatmentend, 1, 10)
           end as treatmentend,
           averagebloodflowrate,
           averagebloodflowrateuom,
           patientvascularaccessidnumber,
           startsittingbp,
           startsittingpulse,
           endsittingbp,
           endsittingpulse,
           startstandingbp,
           startstandingpulse,
           endstandingbp,
           endstandingpulse,
           runhighbp,
           runlowbp,
           runactualdialysisrxidnumber,
           clean_up_vital_sign('WEIGHT', (dryweight * 2.2046226),
                                                         'POUNDS', pat_sex, pat_age, NULL, NULL,
                                                         NULL) AS dryweight,
            -- Note:    If you have to convert the uom here,
            --          I doubt we should keep the original uom fields
           'POUNDS' as dryweightuom,
           clean_up_vital_sign('WEIGHT', (lastweight * 2.2046226),
                                                         'POUNDS', pat_sex, pat_age, NULL, NULL,
                                                         NULL) AS lastweight,
           lastweightuom,
           clean_up_vital_sign('WEIGHT', (preweight * 2.2046226),
                                                         'POUNDS', pat_sex, pat_age, NULL, NULL,
                                                         NULL) AS preweight,
           preweightuom,
           clean_up_vital_sign('WEIGHT', (postweight * 2.2046226),
                                                         'POUNDS', pat_sex, pat_age, NULL, NULL,
                                                         NULL) AS postweight,
           postweightuom,
           litersprocessed,
           litersprocesseduom,
           timedialyzed,
           fluidremoved,
           fluidremoveduom,
           dialysatetemp,
           dialysatetempuom,
           patienttempstart,
           patienttempstartuom,
           patienttempend,
           patienttempenduom,
           patienttemphigh,
           patienttemphighuom,
           machineheaderid,
           txsubtype,
           extract_date(substring(posteddate, 1, 10), '%Y-%m-%d')     as posteddate,
           source,
           latestart,
           completetx,
           disposition,
           ambulatorystatus,
           statustimetx,
           startlyingbp,
           startlyingpulse,
           endlyingbp,
           endlyingpulse,
           shift,
           infectionpresent,
           extract_date(substring(addeddate, 1, 10), '%Y-%m-%d')      as addeddate,
           extract_date(substring(editdate, 1, 10), '%Y-%m-%d')       as editdate,
           extract_date(substring(inactivatedate, 1, 10), '%Y-%m-%d') as inactivatedate,
           statusisactive,
           patientvascularaccesssecondaryidnumber,
           cfresultidnumber,
           patientdialysisrxheaderidnumber,
           treatmenttypecategory,
           treatmenttypesubtype,
           isprimary
    FROM dialysistreatment
    group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,
             31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58,59,
             60,61,62,63,64,65,66,67,68,69,70,71,72,73,74,75
);

DROP TABLE IF EXISTS clean_facilityadmitdischarge;
CREATE TABLE clean_facilityadmitdischarge AS (
    select analyticrowidnumber as record_id,
    clinicorganizationidnumber,
    hvid,
    patientfacilityidnumber,
    clinicidnumber,
    case when lower(dischargereason) like '%death%' then substr(admitdate, 1, 7)
        else admitdate end as admitdate,
    case when admitreason = '_NotMapped: 6B=LOSS: Transfer out to prison/other' then '_NotMapped: 6B=LOSS: Transfer out to other'
        else admitreason end as admitreason,
    case when lower(dischargereason) like '%death%' then substr(dischargedate, 1, 7)
        else dischargedate end as dischargedate,
    dischargereason,
    case when lower(dischargereason) like '%death%' then substr(addeddate, 1, 7)
        else addeddate end as addeddate,
    case when lower(dischargereason) like '%death%' then substr(editdate, 1, 7)
        else editdate end as editdate,
    case when networkevent = '6B=LOSS: Transfer out to prison/other country' then '6B=LOSS: Transfer out to other country'
        when networkevent = '2B=ADDITION: Transfer In - From Another Country or a prison with a Medicare Dialysis Unit' then '2B=ADDITION: Transfer In - From Another Country or a Medicare Dialysis Unit'
        else networkevent end as networkevent,
    patientmasterscheduleheaderid,
    patientmasterscheduleid,
    involuntarydischargereason,
    case when transferdischargereason = 'Incarcerated' then null
        else transferdischargereason end as transferdischargereason,
    transientreason,
    inactivatedate,
    case when lower(dischargereason) like '%death%' then substr(analyticdos, 1, 7)
        else analyticdos end as analyticdos
    FROM (
             SELECT analyticrowidnumber,
                    clinicorganizationidnumber,
                    hvid,
                    patientdataanalyticrowidnumber,
                    patientfacilityidnumber,
                    patientidnumber,
                    clinicidnumber,
                    extract_date(substring(admitdate, 1, 10), '%Y-%m-%d')      as admitdate,
                    admitreason,
                    extract_date(substring(dischargedate, 1, 10), '%Y-%m-%d')  as dischargedate,
                    dischargereason,
                    extract_date(substring(addeddate, 1, 10), '%Y-%m-%d')      as addeddate,
                    extract_date(substring(editdate, 1, 10), '%Y-%m-%d')       as editdate,
                    networkevent,
                    patientmasterscheduleheaderid,
                    patientmasterscheduleid,
                    involuntarydischargereason,
                    transferdischargereason,
                    transientreason,
                    extract_date(substring(inactivatedate, 1, 10), '%Y-%m-%d') as inactivatedate,
                    analyticdos
             FROM facilityadmitdischarge
         )
    group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19

);

DROP TABLE IF EXISTS clean_hospitalization;
CREATE TABLE clean_hospitalization AS (
    SELECT analyticrowidnumber as record_id,
           clinicorganizationidnumber,
           hvid,
           patientdataanalyticrowidnumber,
           patienthospitalizationidnumber,
           patientidnumber,
           admittingphysicianidnumber,
           extract_date(substring(admissiondate, 1, 10), '%Y-%m-%d')  as admissiondate,
           extract_date(substring(dischargedate, 1, 10), '%Y-%m-%d')  as dischargedate,
           extract_date(substring(addeddate, 1, 10), '%Y-%m-%d')      as addeddate,
           extract_date(substring(editdate, 1, 10), '%Y-%m-%d')       as editdate,
           hospconsult,
           unstable,
           staylength,
           CLEAN_UP_DIAGNOSIS_CODE(replace(icd9, '.', ''), '01', NULL)  as icd9,
           patientdialyzed,
           typevisit,
           presumptivediagnosis,
           transplantreferral,
           extract_date(substring(inactivatedate, 1, 10), '%Y-%m-%d') as inactivatedate,
           CLEAN_UP_DIAGNOSIS_CODE(replace(icd10, '.', ''), '02', NULL)  as icd10,
           analyticdos
    FROM hospitalization
    group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22
);

DROP TABLE IF EXISTS clean_immunization;
CREATE TABLE clean_immunization AS (
SELECT
    analyticrowidnumber as record_id,
    clinicorganizationidnumber,
    hvid,
    patientdataanalyticrowidnumber,
    patientimmunizationidnumber,
    patientidnumber,
    analyticdos,
    immunizationtype,
    isbooster,
    notgiven,
    onoffsite,
    immunization,
    extract_date(substring(immunizationdate, 1, 10), '%Y-%m-%d') as immunizationdate,
    extract_date(substring(addeddate, 1, 10), '%Y-%m-%d') as addeddate,
    extract_date(substring(editdate, 1, 10), '%Y-%m-%d') as editdate,
    administeredondialysis,
    justificationforadministered,
    scheduled,
    refused,
    runidnumber,
    administrationcode,
    medicationcode,
    physicianidnumber,
    status,
    lotnumber,
    extract_date(substring(expirationdate, 1, 10), '%Y-%m-%d') as expirationdate,
    immunizationroute,
    extract_date(substring(inactivatedate, 1, 10), '%Y-%m-%d') as inactivatedate,
    treatmentidnumber,
    icd10
FROM immunization
);

DROP TABLE IF EXISTS clean_insurance;
CREATE TABLE clean_insurance AS (
    SELECT analyticrowidnumber as record_id,
           clinicorganizationidnumber,
           hvid,
           patientdataanalyticrowidnumber,
           insuranceidnumber,
           patientidnumber,
           case when insurancecompanyname in ('Chickasaw Nation Tribal','MAHONING COUNTY JAIL','Muscogee Nation','SENECA NATION-LIONEL JOHN HC') then null
                else insurancecompanyname
           end as insurancecompanyname,
           insurancecompanyid,
           payorid,
           extract_date(substring(planeffectivedate, 1, 10), '%Y-%m-%d')  as planeffectivedate,
           extract_date(substring(planexpirationdate, 1, 10), '%Y-%m-%d') as planexpirationdate,
           plantype,
           extract_date(substring(addeddate, 1, 10), '%Y-%m-%d')          as addeddate,
           extract_date(substring(editdate, 1, 10), '%Y-%m-%d')           as editdate,
           medicareparta,
           companyplantype,
           extract_date(substring(inactivatedate, 1, 10), '%Y-%m-%d')     as inactivatedate
    FROM insurance
    group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17
);

DROP TABLE IF EXISTS clean_labidlist;
CREATE TABLE clean_labidlist AS (
SELECT
    analyticrowidnumber as record_id,
    universalserviceid,
    observationidentifier,
    testname,
    cptcode,
    icd9diagnosiscode,
    cptinfo,
    icd9info,
    revised_info,
    differencees,
    extract_date(substring(inactivatedate, 1, 10), '%Y-%m-%d') as inactivatedate
FROM labidlist
);

DROP TABLE IF EXISTS clean_labpanelsdrawn;
CREATE TABLE clean_labpanelsdrawn AS (
SELECT
    analyticrowidnumber as record_id,
    clinicorganizationidnumber,
    hvid,
    patientdataanalyticrowidnumber,
    labpanelsdrawnid,
    analyticdos,
    labscheduleidnumber,
    patientidnumber,
    status,
    panelname,
    justification,
    extract_date(substring(addeddate, 1, 10), '%Y-%m-%d') as addeddate,
    extract_date(substring(editdate, 1, 10), '%Y-%m-%d') as editdate,
    runidnumber,
    postponedto,
    extract_date(substring(thedate, 1, 10), '%Y-%m-%d') as thedate,
    panelidnumber,
    administrationcode,
    procedurecode,
    medicationcode,
    physicianidnumber,
    extract_date(substring(inactivatedate, 1, 10), '%Y-%m-%d') as inactivatedate,
    treatmentidnumber
FROM labpanelsdrawn
);

DROP TABLE IF EXISTS clean_labresult;
CREATE TABLE clean_labresult AS (
    select
        analyticrowidnumber as record_id,
        clinicorganizationidnumber,
        hvid,
        analyticdos,
        labresultidnumber,
        orderedby,
        orderdate,
        completeddate,
        receiveddate,
        testname,
        testresult,
        addeddate,
        editdate,
        calculated,
        scaled,
        case when lower(textresult) like '%donald%' or lower(textresult) like '%amber%' or lower(textresult) like '%amy%' or lower(textresult) like '%angela%' or lower(textresult) like '%chamber%' or lower(textresult) like '%chong%' or lower(textresult) like '%christy%' or lower(textresult) like '%debora%' or lower(textresult) like '%dob%' or lower(textresult) like '%gonzalez%' or lower(textresult) like '%gordon%' or lower(textresult) like '%hailey%' or lower(textresult) like '%heather%' or lower(textresult) like '%jaime%' or lower(textresult) like '%jamie%' or lower(textresult) like '%jeff%' or lower(textresult) like '%jessica%' or lower(textresult) like '%jill%' or lower(textresult) like '%kimberly%' or lower(textresult) like '%kris%' or lower(textresult) like '%lawrence%' or lower(textresult) like '%lee%' or lower(textresult) like '%lindsey%' or lower(textresult) like '%lindsy%' or lower(textresult) like '%melena%' or lower(textresult) like '%michelle%' or lower(textresult) like '%patient%' or lower(textresult) like '%sarah%' or lower(textresult) like '%simpson%' or lower(textresult) like '%stephen%' or lower(textresult) like '%susan%' or lower(textresult) like '%teresa%' or lower(textresult) like '%alex%' or lower(textresult) like '%andrew%' or lower(textresult) like '%angela%' or lower(textresult) like '%anna%' or lower(textresult) like '%barbara%' or upper(textresult) like '%E%HAZELTON AVE%' or lower(textresult) like '%betty%' or lower(textresult) like '%blanche%' or lower(textresult) like '%brenda%' or lower(textresult) like '%chamber%' or lower(textresult) like '%chapman%' or lower(textresult) like '%christina%' or lower(textresult) like '%cynthia%' or lower(textresult) like '%dara%' or lower(textresult) like '%darlene%' or lower(textresult) like '%deb%' or lower(textresult) like '%donald%' or lower(textresult) like '%elizabeth%' or lower(textresult) like '%james%' or lower(textresult) like '%jean%' or lower(textresult) like '%jehova%' or lower(textresult) like '%jennifer%' or lower(textresult) like '%joan%' or lower(textresult) like '%johnnie%' or lower(textresult) like '%kathy%' or lower(textresult) like '%kris%' or lower(textresult) like '%larry%' or lower(textresult) like '%lawrence%' or lower(textresult) like '%lynette%' or lower(textresult) like '%lynn%' or lower(textresult) like '%maria%' or lower(textresult) like '%mary%' or lower(textresult) like '%northern riverview nursing home%' or lower(textresult) like '%nursing home%' or lower(textresult) like '%religious%' or lower(textresult) like '%renee%' or lower(textresult) like '%rhonda%' or lower(textresult) like '%robert%' or lower(textresult) like '%rosemary%' or lower(textresult) like '%royce%' or lower(textresult) like '%sherman%' or lower(textresult) like '%shirley%' or lower(textresult) like '%shoemake%' or lower(textresult) like '%simpson%' or lower(textresult) like '%stacy%' or lower(textresult) like '%sylvia%' or lower(textresult) like '%tammy%' or lower(textresult) like '%tasha%' or lower(textresult) like '%teresa%' or lower(textresult) like '%terry%' or lower(textresult) like '%wife%' or upper(textresult) like '%ALEX%' or upper(textresult) like '%AUBREY%' or UPPER(textresult) LIKE '%BRENDA%' or upper(textresult) like '%BRIAN%' or upper(textresult) like '%BURNETT%' or upper(textresult) like '%CAITLIN%' or upper(textresult) like '%CHARLOTTE%' or upper(textresult) like '%CHILD%HOSPITAL%' or upper(textresult) like '%CHONG%LEE%' or upper(textresult) like '%CHRISTY%' or upper(textresult) like '%CRUZ%' or upper(textresult) like '%DAVID%' or upper(textresult) like '%DOUG%' or upper(textresult) like '%DREW%' or upper(textresult) like '%FRANK%' or upper(textresult) like '%GONZALEZ%EDWIN%' or upper(textresult) like '%HALLIE%' or upper(textresult) like '%HAMMOND%INDIANA%' or upper(textresult) like '%HANNAH%' or upper(textresult) like '%HEATHER%' or UPPER(textresult) LIKE '%HURWITZ%' or upper(textresult) like '%JAIME%' or upper(textresult) like '%JAMIE%' or upper(textresult) like '%JEAN%' or upper(textresult) like '%JEFFREY%' or upper(textresult) like '%JESSICA%' or upper(textresult) like '%JILL%' or upper(textresult) like '%JILL%GA%' or upper(textresult) like '%JOHN%' or upper(textresult) like '%JUAN%' or upper(textresult) like '%KATELYN%' or upper(textresult) like '%KATHY%' or upper(textresult) like '%KEVIN%' or upper(textresult) like '%KIMBERLY%' or upper(textresult) like '%KRISTINA%' or upper(textresult) like '%KRISTY%' or upper(textresult) like '%KURT%' or upper(textresult) like '%LEROY%' or upper(textresult) like '%LINDSEY%' or upper(textresult) like '%LINDSY%' or upper(textresult) like '%LYNN%' or upper(textresult) like '%MARIA%' or upper(textresult) like '%MARNIE%' or upper(textresult) like '%MELENA%' or upper(textresult) like '%MICHELLE%' or upper(textresult) like '%NORTHERN RIVERVIEW NURSING HOME%' or upper(textresult) like '%PHUNG%' or upper(textresult) like '%RAFIYA%' or upper(textresult) like '%RELIGION%' or upper(textresult) like '%RESULT CALLED AND READ BACK%' or upper(textresult) like '%RESULT CONFIRMED BY%' or upper(textresult) like '%RICHELLE%' or upper(textresult) like '%SARAH%' or upper(textresult) like '%SCHULTZ%' or upper(textresult) like '%SHARON%' or upper(textresult) like '%SHARRON%' or upper(textresult) like '%SHEILA%' or upper(textresult) like '%SHERRI%' or upper(textresult) like '%SIOUX%' or upper(textresult) like '%STOCKTON%CA%' or upper(textresult) like '%SUSAN%' or upper(textresult) like '%WALLACE%' or UPPER(textresult) LIKE '%WIFE%' or UPPER(textresult) LIKE '%SENSABAUGH%' or upper(textresult) like '%WILKERSON%' or upper(textresult) like '%HARDMAN%' or upper(textresult) like '%ROBITZSZH%' or upper(textresult) like '%MUNAZZA%' or upper(textresult) like '%TERESE%' or upper(textresult) like '%CINDY%' or upper(textresult) like '%GARIBAY%' or upper(textresult) like '%WHITMYER%' or upper(textresult) like '%SHELLEY%' or upper(textresult) like '%MICHALIE%' or upper(textresult) like '%EMMANUEL%' then NULL
             when (upper(textresult) like '%CALLED%READ%' or upper(textresult) like '%CALLED%TO%' or upper(textresult) like '%CONFIRMED%BY%' or
            upper(textresult) like '%RESULTS%TO%' or upper(textresult) like '%PHONED%TO%' or upper(textresult) like '%ENTERED%BY%' or upper(textresult) like '%COMMENT%TO%' or
            upper(textresult) like '%COMMENT%PHONED%' or upper(textresult) like '%NOTIFIED%AT%' or upper(textresult) like '%ORDERED%BY%' or upper(textresult) like '%CHANGED%BY%') and
            testresult is not null and testresult not in ('',' ') then NULL
             when upper(textresult) like '%SPECIMEN LABELED%' or upper(textresult) like '%LICENSED%CAREGIVER%' or upper(textresult) like '%PATIENT%NAME%' then NULL
             else textresult
        end as textresult,
        referencerange,
        observationidentifier,
        universalserviceid,
        case when lower(fmtresult) like '%donald%' or lower(fmtresult) like '%amber%' or lower(fmtresult) like '%amy%' or lower(fmtresult) like '%angela%' or lower(fmtresult) like '%chamber%' or lower(fmtresult) like '%chong%' or lower(fmtresult) like '%christy%' or lower(fmtresult) like '%debora%' or lower(fmtresult) like '%dob%' or lower(fmtresult) like '%gonzalez%' or lower(fmtresult) like '%gordon%' or lower(fmtresult) like '%hailey%' or lower(fmtresult) like '%heather%' or lower(fmtresult) like '%jaime%' or lower(fmtresult) like '%jamie%' or lower(fmtresult) like '%jeff%' or lower(fmtresult) like '%jessica%' or lower(fmtresult) like '%jill%' or lower(fmtresult) like '%kimberly%' or lower(fmtresult) like '%kris%' or lower(fmtresult) like '%lawrence%' or lower(fmtresult) like '%lee%' or lower(fmtresult) like '%lindsey%' or lower(fmtresult) like '%lindsy%' or lower(fmtresult) like '%melena%' or lower(fmtresult) like '%michelle%' or lower(fmtresult) like '%patient%' or lower(fmtresult) like '%sarah%' or lower(fmtresult) like '%simpson%' or lower(fmtresult) like '%stephen%' or lower(fmtresult) like '%susan%' or lower(fmtresult) like '%teresa%' or lower(fmtresult) like '%alex%' or lower(fmtresult) like '%andrew%' or lower(fmtresult) like '%angela%' or lower(fmtresult) like '%anna%' or lower(fmtresult) like '%barbara%' or upper(fmtresult) like '%E%HAZELTON AVE%' or lower(fmtresult) like '%betty%' or lower(fmtresult) like '%blanche%' or lower(fmtresult) like '%brenda%' or lower(fmtresult) like '%chamber%' or lower(fmtresult) like '%chapman%' or lower(fmtresult) like '%christina%' or lower(fmtresult) like '%cynthia%' or lower(fmtresult) like '%dara%' or lower(fmtresult) like '%darlene%' or lower(fmtresult) like '%deb%' or lower(fmtresult) like '%donald%' or lower(fmtresult) like '%elizabeth%' or lower(fmtresult) like '%james%' or lower(fmtresult) like '%jean%' or lower(fmtresult) like '%jehova%' or lower(fmtresult) like '%jennifer%' or lower(fmtresult) like '%joan%' or lower(fmtresult) like '%johnnie%' or lower(fmtresult) like '%kathy%' or lower(fmtresult) like '%kris%' or lower(fmtresult) like '%larry%' or lower(fmtresult) like '%lawrence%' or lower(fmtresult) like '%lynette%' or lower(fmtresult) like '%lynn%' or lower(fmtresult) like '%maria%' or lower(fmtresult) like '%mary%' or lower(fmtresult) like '%northern riverview nursing home%' or lower(fmtresult) like '%nursing home%' or lower(fmtresult) like '%religious%' or lower(fmtresult) like '%renee%' or lower(fmtresult) like '%rhonda%' or lower(fmtresult) like '%robert%' or lower(fmtresult) like '%rosemary%' or lower(fmtresult) like '%royce%' or lower(fmtresult) like '%sherman%' or lower(fmtresult) like '%shirley%' or lower(fmtresult) like '%shoemake%' or lower(fmtresult) like '%simpson%' or lower(fmtresult) like '%stacy%' or lower(fmtresult) like '%sylvia%' or lower(fmtresult) like '%tammy%' or lower(fmtresult) like '%tasha%' or lower(fmtresult) like '%teresa%' or lower(fmtresult) like '%terry%' or lower(fmtresult) like '%wife%' or upper(fmtresult) like '%ALEX%' or upper(fmtresult) like '%AUBREY%' or UPPER(fmtresult) LIKE '%BRENDA%' or upper(fmtresult) like '%BRIAN%' or upper(fmtresult) like '%BURNETT%' or upper(fmtresult) like '%CAITLIN%' or upper(fmtresult) like '%CHARLOTTE%' or upper(fmtresult) like '%CHILD%HOSPITAL%' or upper(fmtresult) like '%CHONG%LEE%' or upper(fmtresult) like '%CHRISTY%' or upper(fmtresult) like '%CRUZ%' or upper(fmtresult) like '%DAVID%' or upper(fmtresult) like '%DOUG%' or upper(fmtresult) like '%DREW%' or upper(fmtresult) like '%FRANK%' or upper(fmtresult) like '%GONZALEZ%EDWIN%' or upper(fmtresult) like '%HALLIE%' or upper(fmtresult) like '%HAMMOND%INDIANA%' or upper(fmtresult) like '%HANNAH%' or upper(fmtresult) like '%HEATHER%' or UPPER(fmtresult) LIKE '%HURWITZ%' or upper(fmtresult) like '%JAIME%' or upper(fmtresult) like '%JAMIE%' or upper(fmtresult) like '%JEAN%' or upper(fmtresult) like '%JEFFREY%' or upper(fmtresult) like '%JESSICA%' or upper(fmtresult) like '%JILL%' or upper(fmtresult) like '%JILL%GA%' or upper(fmtresult) like '%JOHN%' or upper(fmtresult) like '%JUAN%' or upper(fmtresult) like '%KATELYN%' or upper(fmtresult) like '%KATHY%' or upper(fmtresult) like '%KEVIN%' or upper(fmtresult) like '%KIMBERLY%' or upper(fmtresult) like '%KRISTINA%' or upper(fmtresult) like '%KRISTY%' or upper(fmtresult) like '%KURT%' or upper(fmtresult) like '%LEROY%' or upper(fmtresult) like '%LINDSEY%' or upper(fmtresult) like '%LINDSY%' or upper(fmtresult) like '%LYNN%' or upper(fmtresult) like '%MARIA%' or upper(fmtresult) like '%MARNIE%' or upper(fmtresult) like '%MELENA%' or upper(fmtresult) like '%MICHELLE%' or upper(fmtresult) like '%NORTHERN RIVERVIEW NURSING HOME%' or upper(fmtresult) like '%PHUNG%' or upper(fmtresult) like '%RAFIYA%' or upper(fmtresult) like '%RELIGION%' or upper(fmtresult) like '%RESULT CALLED AND READ BACK%' or upper(fmtresult) like '%RESULT CONFIRMED BY%' or upper(fmtresult) like '%RICHELLE%' or upper(fmtresult) like '%SARAH%' or upper(fmtresult) like '%SCHULTZ%' or upper(fmtresult) like '%SHARON%' or upper(fmtresult) like '%SHARRON%' or upper(fmtresult) like '%SHEILA%' or upper(fmtresult) like '%SHERRI%' or upper(fmtresult) like '%SIOUX%' or upper(fmtresult) like '%STOCKTON%CA%' or upper(fmtresult) like '%SUSAN%' or upper(fmtresult) like '%WALLACE%' or UPPER(fmtresult) LIKE '%WIFE%' or UPPER(fmtresult) LIKE '%SENSABAUGH%' or upper(fmtresult) like '%WILKERSON%' or upper(fmtresult) like '%HARDMAN%' or upper(fmtresult) like '%ROBITZSZH%' or upper(fmtresult) like '%MUNAZZA%' or upper(fmtresult) like '%TERESE%' or upper(fmtresult) like '%CINDY%' or upper(fmtresult) like '%GARIBAY%' or upper(fmtresult) like '%WHITMYER%' or upper(fmtresult) like '%SHELLEY%' or upper(fmtresult) like '%MICHALIE%' or upper(fmtresult) like '%EMMANUEL%'
             then NULL else fmtresult end as fmtresult,
        abnormalflags,
        diagnosiscode,
        esrdrelated,
        runidnumber,
        medicationcode,
        administrationcode,
        procedurecode,
        inactivatedate,
        icd10
    FROM (
         SELECT hvid,
                analyticrowidnumber,
                clinicorganizationidnumber,
                patientdataanalyticrowidnumber,
                analyticdos,
                labresultidnumber,
                patientidnumber,
                orderedby,
                extract_date(substring(orderdate, 1, 10), '%Y-%m-%d')      as orderdate,
                extract_date(substring(completeddate, 1, 10), '%Y-%m-%d')  as completeddate,
                extract_date(substring(receiveddate, 1, 10), '%Y-%m-%d')   as receiveddate,
                testname,
                case
                    when lower(testname) like '%weight%' then
                        case
                            when lower(concat(testname, ' ', coalesce(referencerange, ''))) like
                                 '%kg%'
                                then clean_up_vital_sign('WEIGHT', (testresult * 2.2046226),
                                                         'POUNDS', pat_sex, pat_age, NULL, NULL,
                                                         NULL)
                            when lower(concat(testname, ' ', coalesce(referencerange, ''))) like
                                 '%lb%'
                                then clean_up_vital_sign('WEIGHT', CAST(testresult AS STRING),
                                                         'POUNDS', pat_sex, pat_age, NULL, NULL,
                                                         NULL)
                            else NULL
                            end
                    when lower(testname) like '%height%' then
                        case
                            when lower(concat(testname, ' ', coalesce(referencerange, ''))) like
                                 '%cm%'
                                then clean_up_vital_sign('HEIGHT', (testresult * 0.39370079),
                                                         'INCHES', pat_sex, pat_age, NULL, NULL,
                                                         NULL)
                            when lower(concat(testname, ' ', coalesce(referencerange, ''))) like
                                 '%in%'
                                then clean_up_vital_sign('HEIGHT', CAST(testresult AS STRING),
                                                         'INCHES', pat_sex, pat_age, NULL, NULL,
                                                         NULL)
                            else NULL
                            end
                    else testresult
                    end                                                    as testresult,
                extract_date(substring(addeddate, 1, 10), '%Y-%m-%d')      as addeddate,
                extract_date(substring(editdate, 1, 10), '%Y-%m-%d')       as editdate,
                calculated,
                scaled,
                case
                    when lower(testname) like '%weight%' then
                        case
                            when lower(concat(testname, ' ', coalesce(referencerange, ''))) like
                                 '%kg%'
                                then clean_up_vital_sign('WEIGHT', (textresult * 2.2046226),
                                                         'POUNDS', pat_sex, pat_age, NULL, NULL,
                                                         NULL)
                            when lower(concat(testname, ' ', coalesce(referencerange, ''))) like
                                 '%lb%'
                                then clean_up_vital_sign('WEIGHT', CAST(textresult AS STRING),
                                                         'POUNDS', pat_sex, pat_age, NULL, NULL,
                                                         NULL)
                            else NULL
                            end
                    when lower(testname) like '%height%' then
                        case
                            when lower(concat(testname, ' ', coalesce(referencerange, ''))) like
                                 '%cm%'
                                then clean_up_vital_sign('HEIGHT', (textresult * 0.39370079),
                                                         'INCHES', pat_sex, pat_age, NULL, NULL,
                                                         NULL)
                            when lower(concat(testname, ' ', coalesce(referencerange, ''))) like
                                 '%in%'
                                then clean_up_vital_sign('HEIGHT', CAST(textresult AS STRING),
                                                         'INCHES', pat_sex, pat_age, NULL, NULL,
                                                         NULL)
                            else NULL
                            end
                    else textresult
                    end                                                    as textresult,
                referencerange,
                observationidentifier,
                universalserviceid,
                case
                    when lower(testname) like '%weight%' then
                        case
                            when lower(concat(testname, ' ', coalesce(referencerange, ''))) like
                                 '%kg%'
                                then clean_up_vital_sign('WEIGHT', (fmtresult * 2.2046226),
                                                         'POUNDS', pat_sex, pat_age, NULL, NULL,
                                                         NULL)
                            when lower(concat(testname, ' ', coalesce(referencerange, ''))) like
                                 '%lb%'
                                then clean_up_vital_sign('WEIGHT', CAST(fmtresult AS STRING),
                                                         'POUNDS', pat_sex, pat_age, NULL, NULL,
                                                         NULL)
                            else NULL
                            end
                    when lower(testname) like '%height%' then
                        case
                            when lower(concat(testname, ' ', coalesce(referencerange, ''))) like
                                 '%cm%'
                                then clean_up_vital_sign('HEIGHT', (fmtresult * 0.39370079),
                                                         'INCHES', pat_sex, pat_age, NULL, NULL,
                                                         NULL)
                            when lower(concat(testname, ' ', coalesce(referencerange, ''))) like
                                 '%in%'
                                then clean_up_vital_sign('HEIGHT', CAST(fmtresult AS STRING),
                                                         'INCHES', pat_sex, pat_age, NULL, NULL,
                                                         NULL)
                            else NULL
                            end
                    else fmtresult
                    end                                                    as fmtresult,
                abnormalflags,
                diagnosiscode,
                esrdrelated,
                runidnumber,
                medicationcode,
                administrationcode,
                procedurecode,
                extract_date(substring(inactivatedate, 1, 10), '%Y-%m-%d') as inactivatedate,
                CLEAN_UP_DIAGNOSIS_CODE(replace(icd10, '.', ''), '02', NULL)  as icd10

         FROM labresult lab
    )
    group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29
);

DROP TABLE IF EXISTS clean_medication;
CREATE TABLE clean_medication AS (
SELECT
    medidnumber as record_id,
    extract_date(substring(addeddate, 1, 10), '%Y-%m-%d') as addeddate,
    extract_date(substring(lasteditdate, 1, 10), '%Y-%m-%d') as lasteditdate,
    extract_date(substring(inactivatedate, 1, 10), '%Y-%m-%d') as inactivatedate
FROM medication
);

DROP TABLE IF EXISTS clean_medicationgroup;
CREATE TABLE clean_medicationgroup AS (
SELECT
    medicationgroupidnumber as record_id,
    medicationgroupname,
    medicationname,
    crownmedicationname,
    route,
    original_medgroupiddetailnumb,
    original_medgroupidnumber,
    extract_date(substring(addeddate, 1, 10), '%Y-%m-%d') as addeddate,
    extract_date(substring(lasteditdate, 1, 10), '%Y-%m-%d') as lasteditdate,
    extract_date(substring(inactivatedate, 1, 10), '%Y-%m-%d') as inactivatedate
FROM medicationgroup
);

DROP TABLE IF EXISTS clean_modalitychangehistorycrownweb;
CREATE TABLE clean_modalitychangehistorycrownweb AS (
SELECT
    analyticrowidnumber as record_id,
    modalitychangehistoryanalyticrowidnumber,
    patientdialysisprescriptionanalyticrowidnumber,
    patientidnumber,
    patientdataanalyticrowidnumber,
    defaultclinicorganizationidnumber,
    treatmentclinicorganizationidnumber,
    extract_date(substring(effectivereportingdate, 1, 10), '%Y-%m-%d') as effectivereportingdate,
    physiciananalyticrowidnumber,
    treatmenttype,
    minutespersession,
    sessionsperweek,
    crowndialysissetting
FROM modalitychangehistorycrownweb
);

DROP TABLE IF EXISTS clean_nursinghomehistory;
CREATE TABLE clean_nursinghomehistory AS (
SELECT
    analyticrowidnumber as record_id,
    clinicorganizationidnumber,
    hvid,
    patientdataanalyticrowidnumber,
    patientnursinghomeidnumber,
    patientidnumber,
    nursinghomeid,
    extract_date(substring(startdate, 1, 10), '%Y-%m-%d') as startdate,
    extract_date(substring(enddate, 1, 10), '%Y-%m-%d') as enddate,
    extract_date(substring(addeddate, 1, 10), '%Y-%m-%d') as addeddate,
    extract_date(substring(editdate, 1, 10), '%Y-%m-%d') as editdate,
    extract_date(substring(inactivatedate, 1, 10), '%Y-%m-%d') as inactivatedate,
    analyticdos
FROM nursinghomehistory
);

DROP TABLE IF EXISTS clean_patientaccess;
CREATE TABLE clean_patientaccess AS (
    SELECT analyticrowidnumber as record_id,
           patientdataanalyticrowidnumber,
           hvid,
           clinicorganizationidnumber,
           patientvascularaccessidnumber,
           patientidnumber,
           vascularaccesstype,
           location,
           currentstatus,
           chroniccatheter,
           analyticdos,
           extract_date(substring(startdate, 1, 10), '%Y-%m-%d')      as startdate,
           extract_date(substring(enddate, 1, 10), '%Y-%m-%d')        as enddate,
           extract_date(substring(addeddate, 1, 10), '%Y-%m-%d')      as addeddate,
           extract_date(substring(editdate, 1, 10), '%Y-%m-%d')       as editdate,
           placedrecorded,
           isprimaryaccess,
           isstateactive,
           extract_date(substring(inactivatedate, 1, 10), '%Y-%m-%d') as inactivatedate
    FROM patientaccess
    group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19
);

DROP TABLE IF EXISTS clean_patientaccess_examproc;
CREATE TABLE clean_patientaccess_examproc AS (
SELECT
    analyticrowidnumber as record_id,
    clinicorganizationidnumber,
    hvid,
    patientdataanalyticrowidnumber,
    patientaccessanalyticrowidnumber,
    resultidnumber,
    patientidnumber,
    analyticdos,
    extract_date(substring(addeddate, 1, 10), '%Y-%m-%d') as addeddate,
    extract_date(substring(editdate, 1, 10), '%Y-%m-%d') as editdate,
    extract_date(substring(examproceduredate, 1, 10), '%Y-%m-%d') as examproceduredate,
    examproceduretype,
    examprocedurefrequency,
    performedbytype,
    performedbyoutsidephysicianidnumber,
    performingfacilitytype,
    performingfacilityclinicidnumber,
    performingfacilitycontactid,
    isbillablebyclinic,
    diagnosticcode,
    procedurecode,
    examprocedureisstateactive,
    examprocedurestatusafter,
    examprocedureisprimaryaccess,
    extract_date(substring(inactivatedate, 1, 10), '%Y-%m-%d') as inactivatedate,
    icd10
FROM patientaccess_examproc
);

DROP TABLE IF EXISTS clean_patientaccess_otheraccessevent;
CREATE TABLE clean_patientaccess_otheraccessevent AS (
SELECT
    analyticrowidnumber as record_id,
    clinicorganizationidnumber,
    hvid,
    patientdataanalyticrowidnumber,
    patientaccessanalyticrowidnumber,
    resultidnumber,
    patientidnumber,
    analyticdos,
    extract_date(substring(addeddate, 1, 10), '%Y-%m-%d') as addeddate,
    extract_date(substring(editdate, 1, 10), '%Y-%m-%d') as editdate,
    extract_date(substring(otheraccesseventdate, 1, 10), '%Y-%m-%d') as otheraccesseventdate,
    otheraccesseventisstateactive,
    otheraccesseventstatusafter,
    otheraccesseventisprimaryaccess,
    reasonforchange,
    extract_date(substring(inactivatedate, 1, 10), '%Y-%m-%d') as inactivatedate
FROM patientaccess_otheraccessevent
);

DROP TABLE IF EXISTS clean_patientaccess_placedrecorded;
CREATE TABLE clean_patientaccess_placedrecorded AS (
SELECT
    analyticrowidnumber as record_id,
    clinicorganizationidnumber,
    hvid,
    patientdataanalyticrowidnumber,
    patientaccessanalyticrowidnumber,
    resultidnumber,
    patientidnumber,
    analyticdos,
    extract_date(substring(addeddate, 1, 10), '%Y-%m-%d') as addeddate,
    extract_date(substring(editdate, 1, 10), '%Y-%m-%d') as editdate,
    placedrecorded,
    extract_date(substring(placedrecordeddate, 1, 10), '%Y-%m-%d') as placedrecordeddate,
    accesstype,
    accesslocation,
    cathname,
    ischroniccath,
    placedbytype,
    placedbyoutsidephysicianidnumber,
    placingfacilitytype,
    placingfacilityclinicidnumber,
    placingfacilitycontactid,
    placedrecordedisstateactive,
    placedrecordedstatusafter,
    placedrecordedisprimaryaccess,
    extract_date(substring(inactivatedate, 1, 10), '%Y-%m-%d') as inactivatedate
FROM patientaccess_placedrecorded
);

DROP TABLE IF EXISTS clean_patientaccess_removed;
CREATE TABLE clean_patientaccess_removed AS (
SELECT
    analyticrowidnumber as record_id,
    clinicorganizationidnumber,
    hvid,
    patientdataanalyticrowidnumber,
    patientaccessanalyticrowidnumber,
    resultidnumber,
    patientidnumber,
    analyticdos,
    extract_date(substring(addeddate, 1, 10), '%Y-%m-%d') as addeddate,
    extract_date(substring(editdate, 1, 10), '%Y-%m-%d') as editdate,
    extract_date(substring(removeddate, 1, 10), '%Y-%m-%d') as removeddate,
    removedbytype,
    removedbyoutsidephysicianidnumber,
    removingfacilitytype,
    removingfacilityclinicidnumber,
    removingfacilitycontactid,
    removedreasontype,
    extract_date(substring(inactivatedate, 1, 10), '%Y-%m-%d') as inactivatedate
FROM patientaccess_removed
);

DROP TABLE IF EXISTS clean_patientallergy;
CREATE TABLE clean_patientallergy AS (
SELECT
    analyticrowidnumber as record_id,
    clinicorganizationidnumber,
    hvid,
    patientdataanalyticrowidnumber,
    patientalergyidnumber,
    patientidnumber,
    medidnumber,
    medicationname,
    typeofreaction,
    extract_date(substring(startdate, 1, 10), '%Y-%m-%d') as startdate,
    extract_date(substring(enddate, 1, 10), '%Y-%m-%d') as enddate,
    drug_id,
    category_concept_id,
    categoryname,
    extract_date(substring(addeddate, 1, 10), '%Y-%m-%d') as addeddate,
    extract_date(substring(editdate, 1, 10), '%Y-%m-%d') as editdate,
    extract_date(substring(inactivatedate, 1, 10), '%Y-%m-%d') as inactivatedate
FROM patientallergy
);

DROP TABLE IF EXISTS clean_patientcms2728;
CREATE TABLE clean_patientcms2728 AS (
SELECT
    analyticrowidnumber as record_id,
    clinicorganizationidnumber,
    hvid,
    patientdataanalyticrowidnumber,
    analyticdos,
    patientidnumber,
    form2728type,
    predryweight,
    preheightfeet,
    preheightinches,
    applyingforesrdmedicare,
    medicalcoveragemedicaid,
    medicalcoveragedva,
    medicalcoveragemedicare2728,
    medicalcoveragemedicareadvantage,
    medicalcoverageemployer,
    medicalcoverageother,
    medicalcoveragenone,
    predialysisepoadministered,
    esrd_erythropoteintx,
    esrd_erythropoteinperiod,
    esrd_nephrologistcare,
    esrd_nephrologistperiod,
    esrd_kidneydietitiancare,
    esrd_kidneydietitianperiod,
    esrd_firstopdialysisaccesstype,
    esrd_firstopdialysisotheraccess,
    esrd_firstopdialysismaturingavf,
    esrd_firstopdialysismaturinggraft,
    prelab_albumin,
    extract_date(substring(prelab_albumindate, 1, 10), '%Y-%m-%d') as prelab_albumindate,
    prelab_albuminll,
    extract_date(substring(prelab_albuminlldate, 1, 10), '%Y-%m-%d') as prelab_albuminlldate,
    prelab_bun,
    extract_date(substring(prelab_bundate, 1, 10), '%Y-%m-%d') as prelab_bundate,
    prelab_creatinine,
    extract_date(substring(prelab_creatininedate, 1, 10), '%Y-%m-%d') as prelab_creatininedate,
    prelab_creatinineclearance,
    extract_date(substring(prelab_creatinineclearancedate, 1, 10), '%Y-%m-%d') as prelab_creatinineclearancedate,
    prelab_hba1c,
    extract_date(substring(prelab_hba1cdate, 1, 10), '%Y-%m-%d') as prelab_hba1cdate,
    prelab_hematocrit,
    extract_date(substring(prelab_hematocritdate, 1, 10), '%Y-%m-%d') as prelab_hematocritdate,
    prelab_hemoglobin,
    extract_date(substring(prelab_hemoglobindate, 1, 10), '%Y-%m-%d') as prelab_hemoglobindate,
    prelab_labmethodused,
    extract_date(substring(prelab_labmethoduseddate, 1, 10), '%Y-%m-%d') as prelab_labmethoduseddate,
    prelab_lipidprofilehdl,
    extract_date(substring(prelab_lipidprofilehdldate, 1, 10), '%Y-%m-%d') as prelab_lipidprofilehdldate,
    prelab_lipidprofileldl,
    extract_date(substring(prelab_lipidprofileldldate, 1, 10), '%Y-%m-%d') as prelab_lipidprofileldldate,
    prelab_lipidprofiletc,
    extract_date(substring(prelab_lipidprofiletcdate, 1, 10), '%Y-%m-%d') as prelab_lipidprofiletcdate,
    prelab_lipidprofiletg,
    extract_date(substring(prelab_lipidprofiletgdate, 1, 10), '%Y-%m-%d') as prelab_lipidprofiletgdate,
    prelab_ureaclearance,
    extract_date(substring(prelab_ureaclearancedate, 1, 10), '%Y-%m-%d') as prelab_ureaclearancedate,
    patientgfrmethod,
    extract_date(substring(comorbidreportdate, 1, 10), '%Y-%m-%d') as comorbidreportdate,
    supervisingphysicianidnumber,
    extract_date(substring(supervisingphysiciansignaturedate, 1, 10), '%Y-%m-%d') as supervisingphysiciansignaturedate,
    willselfdialyze,
    trainingphysicianidnumber,
    extract_date(substring(trainingphysiciansignaturedate, 1, 10), '%Y-%m-%d') as trainingphysiciansignaturedate,
    extract_date(substring(patientsignaturedate, 1, 10), '%Y-%m-%d') as patientsignaturedate,
    extract_date(substring(addeddate, 1, 10), '%Y-%m-%d') as addeddate,
    extract_date(substring(editdate, 1, 10), '%Y-%m-%d') as editdate,
    extract_date(substring(inactivatedate, 1, 10), '%Y-%m-%d') as inactivatedate
FROM patientcms2728
);

DROP TABLE IF EXISTS clean_patientcomorbidityandtransplantstate;
CREATE TABLE clean_patientcomorbidityandtransplantstate AS (
    SELECT record_id,
        clinicorganizationidnumber,
        hvid,
        CASE
            WHEN causeofrenalfailure = '7999' THEN null
            WHEN causeofrenalfailure LIKE 'E83%' THEN null
            else causeofrenalfailure
        END AS causeofrenalfailure,
        transplantfunctioningatdeath,
        transplantedkidney,
        transplantdate,
        transplanthospital,
        medicareprovidernumberoftransplanthospital,
        medicareprovidernumberoftransplanthospital2,
        medicareprovidernumberoftransplanthospital3,
        prephospital,
        prephospitalmedicareprovidernumber,
        prephospitalenterdate,
        currentstateoftransplant,
        transplantcandidate,
        transopmedicallyunfit,
        transopnotassessed,
        transopother,
        transoppatientdeclines,
        transoppatientinformed,
        transoppsych,
        transopunsuitableage,
        --note - clean transplanthospital1note manually
        transplanthospital1note,
        transplanthospital2,
        case
            when transplanthospital2note = '2ND TRANSPLANT @DENVER CHILDREN`S HOSPITAL, DENVER, CO' THEN '2ND TRANSPLANT, DENVER, CO'
            else transplanthospital2note
            end as transplanthospital2note,
        CASE
            WHEN transplanthospital3note = '11/28/15 FAMILY PREFERS PT TO BE REFERRED TO PSL FOR TRANSPLANT EVAL AND NOT CHILDREN`S HOSPITAL. PSL CALLED AND MESSAGE LEFT. Pt has eval appt with PSL on 7/5/16 @1330. We will send records.' THEN '11/28/15 FAMILY PREFERS PT TO BE REFERRED TO PSL FOR TRANSPLANT EVAL. PSL CALLED AND MESSAGE LEFT. Pt has eval appt with PSL on 7/5/16. We will send records.'
            ELSE transplanthospital3note end as transplanthospital3note,
        transplantwaitlist,
        editdatetransplant,
        inactivatedate
        FROM (
             SELECT
                analyticrowidnumber as record_id,
                clinicorganizationidnumber,
                hvid,
                patientdataanalyticrowidnumber,
                patientidnumber,
                causeofrenalfailure,
                transplantfunctioningatdeath,
                transplantedkidney,
                extract_date(substring(transplantdate, 1, 10), '%Y-%m-%d') as transplantdate,
                transplanthospital,
                medicareprovidernumberoftransplanthospital,
                medicareprovidernumberoftransplanthospital2,
                medicareprovidernumberoftransplanthospital3,
                prephospital,
                prephospitalmedicareprovidernumber,
                extract_date(substring(prephospitalenterdate, 1, 10), '%Y-%m-%d') as prephospitalenterdate,
                currentstateoftransplant,
                extract_date(substring(transplantcandidate, 1, 10), '%Y-%m-%d') as transplantcandidate,
                transopmedicallyunfit,
                transopnotassessed,
                transopother,
                transoppatientdeclines,
                transoppatientinformed,
                transoppsych,
                transopunsuitableage,
                transplanthospital1note,
                transplanthospital2,
                transplanthospital2note,
                transplanthospital3note,
                transplantwaitlist,
                extract_date(substring(editdatetransplant, 1, 10), '%Y-%m-%d') as editdatetransplant,
                extract_date(substring(inactivatedate, 1, 10), '%Y-%m-%d') as inactivatedate
            FROM patientcomorbidityandtransplantstate
        )
    group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30
);

DROP TABLE IF EXISTS clean_patientdiagcodes;
CREATE TABLE clean_patientdiagcodes AS (
    SELECT analyticrowidnumber as record_id,
           clinicorganizationidnumber,
           hvid,
           patientdataanalyticrowidnumber,
           patientdiagcodeid,
           patientidnumber,
           CLEAN_UP_DIAGNOSIS_CODE(replace(diagnosiscode, '.', ''), '01', NULL)  as diagnosiscode,
           description,
           extract_date(substring(diagnosisdate, 1, 10), '%Y-%m-%d')    as diagnosisdate,
           diagnosistype,
           diagnosispriority,
           admitting,
           extract_date(substring(addeddate, 1, 10), '%Y-%m-%d')        as addeddate,
           extract_date(substring(editdate, 1, 10), '%Y-%m-%d')         as editdate,
           extract_date(substring(diagnosisenddate, 1, 10), '%Y-%m-%d') as diagnosisenddate,
           orderedbyidnumber,
           extract_date(substring(inactivatedate, 1, 10), '%Y-%m-%d')   as inactivatedate,
           CLEAN_UP_DIAGNOSIS_CODE(replace(icd10, '.', ''), '02', NULL)  as icd10,
           analyticdos
    FROM patientdiagcodes
    group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19

);

DROP TABLE IF EXISTS clean_patientdialysisprescription;
CREATE TABLE clean_patientdialysisprescription AS (
SELECT
    analyticrowidnumber as record_id,
    clinicorganizationidnumber,
    hvid,
    patientdataanalyticrowidnumber,
    patientidnumber,
    extract_date(substring(startdate, 1, 10), '%Y-%m-%d') as startdate,
    extract_date(substring(enddate, 1, 10), '%Y-%m-%d') as enddate,
    modalitytype,
    modality,
    primarydialysissetting,
    primarydialysisperiod,
    drygoalweight,
    drygoalweightuom,
    tbd,
    extract_date(substring(addeddate, 1, 10), '%Y-%m-%d') as addeddate,
    extract_date(substring(editdate, 1, 10), '%Y-%m-%d') as editdate,
    physicianidnumber,
    patientdialysisheaderidnumber,
    patientdialysisrxnumber,
    patientdialysisrxinpatientnumber,
    patientrxrxnxstageidnumber,
    patientrxrxotheridnumber,
    patientrxrxcapdidnumber,
    patientrxrxregularccpdidnumber,
    patientrxrxhighdoseccpdidnumber,
    patientrxrxtidalidnumber,
    patientrxrxhighdosetidalidnumber,
    prescriptiontype,
    extract_date(substring(inactivatedate, 1, 10), '%Y-%m-%d') as inactivatedate,
    analyticdos
FROM patientdialysisprescription
);

DROP TABLE IF EXISTS clean_patientdialysisrxhemo;
CREATE TABLE clean_patientdialysisrxhemo AS (
SELECT
    analyticrowidnumber as record_id,
    clinicorganizationidnumber,
    patientdataanalyticrowidnumber,
    patientdialysisprescriptionanalyticrowidnumber,
    patientdialysisheaderidnumber,
    treatmenttime,
    treatmenttimeuom,
    membranetype,
    bloodflowrate,
    arterialneedlesize,
    venousneedlesize,
    dialysateflowrate,
    dialysisbathcalciumlevel,
    dialysisbathpotasiumlevel,
    dialysisbathsodiumleveltype,
    sodiumbegin,
    sodiumend,
    sodiumstepinstructions,
    sodiumprofilenumber,
    totalheparingiven,
    heparininstructions,
    bicarbonatelevel,
    ultrafiltleveltype,
    ultrafiltbeginlevel,
    ultrafiltendlevel,
    ultrafiltlstepinstructions,
    ultrafiltprofilenumber,
    litersprocessed,
    totalbodywater,
    dialysatetemp,
    maxfluidremoved,
    treatmentsetuptime,
    treatmentcleanuptime,
    weightmin,
    weightmax,
    totalvolumedialysate,
    filtration,
    otherfiltration,
    bloodflowratemin,
    bloodflowratemax,
    lactate,
    otherlactate,
    ufrate,
    dialysisfrequencydesc,
    dialysisfrequencyvalue,
    extract_date(substring(inactivatedate, 1, 10), '%Y-%m-%d') as inactivatedate
FROM patientdialysisrxhemo
);

DROP TABLE IF EXISTS clean_patientdialysisrxpd;
CREATE TABLE clean_patientdialysisrxpd AS (
SELECT
    analyticrowidnumber as record_id,
    clinicorganizationidnumber,
    patientdataanalyticrowidnumber,
    patientdialysisprescriptionanalyticrowidnumber,
    patientdialysisheaderidnumber,
    therapytime,
    numberofexchanges,
    totalvolumeexchangesolution,
    fulldrainevery,
    numberofcycles,
    cyclertype,
    tidalpercent,
    ufpercycle,
    totaluf,
    changesolutionprotocol,
    totalvolumecyclersolution,
    finalfillvolume,
    finalfillexchangeidnumber,
    extract_date(substring(inactivatedate, 1, 10), '%Y-%m-%d') as inactivatedate
FROM patientdialysisrxpd
);

DROP TABLE IF EXISTS clean_patientdialysisrxpdexchanges;
CREATE TABLE clean_patientdialysisrxpdexchanges AS (
SELECT
    analyticrowidnumber as record_id,
    clinicorganizationidnumber,
    patientdataanalyticrowidnumber,
    patientdialysisprescriptionanalyticrowidnumber,
    patientdialysispdexchangesidnumber,
    patientdialysisheaderidnumber,
    exchangenumber,
    exchangedialysisperiod,
    dialysisexchangemechanism,
    exchangetime,
    exchangesolution,
    lowcalcium,
    exchangevolume,
    fillvolume,
    dwelltime,
    draintime,
    extract_date(substring(addeddate, 1, 10), '%Y-%m-%d') as addeddate,
    extract_date(substring(editdate, 1, 10), '%Y-%m-%d') as editdate,
    solutiontype,
    extract_date(substring(inactivatedate, 1, 10), '%Y-%m-%d') as inactivatedate
FROM patientdialysisrxpdexchanges
);

DROP TABLE IF EXISTS clean_patientevent;
CREATE TABLE clean_patientevent AS (
SELECT
    analyticrowidnumber as record_id,
    clinicorganizationidnumber,
    hvid,
    patientdataanalyticrowidnumber,
    patienteventidnumber,
    patientidnumber,
    extract_date(substring(eventdate, 1, 10), '%Y-%m-%d') as eventdate,
    eventtype,
    extract_date(substring(addeddate, 1, 10), '%Y-%m-%d') as addeddate,
    extract_date(substring(editdate, 1, 10), '%Y-%m-%d') as editdate,
    extract_date(substring(deleteddate, 1, 10), '%Y-%m-%d') as deleteddate,
    resultidnumber,
    patientvascularaccessidnumber,
    patientinfectionidnumber,
    extract_date(substring(inactivatedate, 1, 10), '%Y-%m-%d') as inactivatedate,
    analyticdos
FROM patientevent
);

DROP TABLE IF EXISTS clean_patientfluidweightmanagement;
CREATE TABLE clean_patientfluidweightmanagement AS (
SELECT
    analyticrowidnumber as record_id,
    clinicorganizationidnumber,
    hvid,
    patientdataanalyticrowidnumber,
    cklresultid,
    patientidnumber,
    analyticdos,
    extract_date(substring(sodiumeducationdate, 1, 10), '%Y-%m-%d') as sodiumeducationdate,
    extract_date(substring(addeddate, 1, 10), '%Y-%m-%d') as addeddate,
    extract_date(substring(editdate, 1, 10), '%Y-%m-%d') as editdate,
    sodiumeducationreceived,
    sodiumprofilingrxed,
    constantdialysatesodiumrxed,
    dialysatesodiumover138,
    extract_date(substring(postdialysiswgtassessmentdate, 1, 10), '%Y-%m-%d') as postdialysiswgtassessmentdate,
    postdialysistargetwgtrxed,
    homebpprovided,
    homebpstatus,
    dryweightrxed,
    hasedema,
    extract_date(substring(echocardiogramdate, 1, 10), '%Y-%m-%d') as echocardiogramdate,
    hasabnormalbreathsounds,
    hasleftventricularhypertrophy,
    leftventricularhypertrophychanged,
    extract_date(substring(inactivatedate, 1, 10), '%Y-%m-%d') as inactivatedate
FROM patientfluidweightmanagement
);

DROP TABLE IF EXISTS clean_patientheighthistory;
CREATE TABLE clean_patientheighthistory AS (
SELECT
    analyticrowidnumber as record_id,
    clinicorganizationidnumber,
    hvid,
    patientdataanalyticrowidnumber,
    analyticdos,
    patientheighthistoryidnumber,
    heightfeet,
    heightinches,
    totalheight,
    totalheightuom,
    patientidnumber,
    doubleamputee,
    extract_date(substring(addeddate, 1, 10), '%Y-%m-%d') as addeddate,
    extract_date(substring(editdate, 1, 10), '%Y-%m-%d') as editdate,
    extract_date(substring(measuredate, 1, 10), '%Y-%m-%d') as measuredate,
    unstable,
    extract_date(substring(inactivatedate, 1, 10), '%Y-%m-%d') as inactivatedate
FROM patientheighthistory
);

DROP TABLE IF EXISTS clean_patientinfection;
CREATE TABLE clean_patientinfection AS (
SELECT
    analyticrowidnumber as record_id,
    clinicorganizationidnumber,
    hvid,
    patientdataanalyticrowidnumber,
    patientinfectionidnumber,
    patientidnumber,
    extract_date(substring(suspectedinfectionstartdate, 1, 10), '%Y-%m-%d') as suspectedinfectionstartdate,
    extract_date(substring(confirmationdate, 1, 10), '%Y-%m-%d') as confirmationdate,
    infectionsourcetype,
    patientvascularaccessidnumber,
    primarylocationother,
    identificationtype,
    symptom_none,
    symptom_accesssite,
    symptom_fever_37_8c,
    symptom_chills,
    symptom_bp,
    symptom_mental,
    symptom_wound,
    symptom_cellulitis,
    symptom_respiratory,
    extract_date(substring(enddate, 1, 10), '%Y-%m-%d') as enddate,
    infectionrequiredhospitalization,
    extract_date(substring(hospitalizationdate, 1, 10), '%Y-%m-%d') as hospitalizationdate,
    deathrelatedtoinfection,
    pdeffluentcellcountsdifferential,
    extract_date(substring(addeddate, 1, 10), '%Y-%m-%d') as addeddate,
    extract_date(substring(editdate, 1, 10), '%Y-%m-%d') as editdate,
    extract_date(substring(inactivatedate, 1, 10), '%Y-%m-%d') as inactivatedate,
    analyticdos,
    lossofvascularaccess,
    accesssitenhsn_fistula,
    accesssitenhsn_graft,
    accesssitenhsn_tunncentralline,
    accesssitenhsn_nontunncentralline,
    accesssitenhsn_otheraccessdevice
FROM patientinfection
);

DROP TABLE IF EXISTS clean_patientinfection_laborganism;
CREATE TABLE clean_patientinfection_laborganism AS (
SELECT
    analyticrowidnumber as record_id,
    clinicorganizationidnumber,
    hvid,
    patientdataanalyticrowidnumber,
    analyticdos,
    patientidnumber,
    patientinfection_laborganismidnumber,
    patientinfection_labresultcultureidnumber,
    organismcode,
    organismrank,
    extract_date(substring(addeddate, 1, 10), '%Y-%m-%d') as addeddate,
    extract_date(substring(editdate, 1, 10), '%Y-%m-%d') as editdate,
    extract_date(substring(inactivatedate, 1, 10), '%Y-%m-%d') as inactivatedate
FROM patientinfection_laborganism
);

DROP TABLE IF EXISTS clean_patientinfection_laborganismdrug;
CREATE TABLE clean_patientinfection_laborganismdrug AS (
SELECT
    analyticrowidnumber as record_id,
    clinicorganizationidnumber,
    hvid,
    patientdataanalyticrowidnumber,
    analyticdos,
    patientidnumber,
    patientinfection_laborganismdrugidnumber,
    patientinfection_laborganismidnumber,
    drugcode,
    susceptibilitycode,
    extract_date(substring(addeddate, 1, 10), '%Y-%m-%d') as addeddate,
    extract_date(substring(editdate, 1, 10), '%Y-%m-%d') as editdate,
    extract_date(substring(inactivatedate, 1, 10), '%Y-%m-%d') as inactivatedate
FROM patientinfection_laborganismdrug
);

DROP TABLE IF EXISTS clean_patientinfection_labresultculture;
CREATE TABLE clean_patientinfection_labresultculture AS (
SELECT
    analyticrowidnumber as record_id,
    clinicorganizationidnumber,
    hvid,
    patientdataanalyticrowidnumber,
    patientinfection_labresultcultureidnumber,
    patientinfectionidnumber,
    patientidnumber,
    extract_date(substring(resulteddate, 1, 10), '%Y-%m-%d') as resulteddate,
    resulttype,
    extract_date(substring(collecteddate, 1, 10), '%Y-%m-%d') as collecteddate,
    culturesource,
    cultureresult,
    organisms,
    extract_date(substring(addeddate, 1, 10), '%Y-%m-%d') as addeddate,
    extract_date(substring(editdate, 1, 10), '%Y-%m-%d') as editdate,
    extract_date(substring(inactivatedate, 1, 10), '%Y-%m-%d') as inactivatedate,
    analyticdos
FROM patientinfection_labresultculture
);

DROP TABLE IF EXISTS clean_patientinfection_medication;
CREATE TABLE clean_patientinfection_medication AS (
SELECT
    analyticrowidnumber as record_id,
    clinicorganizationidnumber,
    hvid,
    patientdataanalyticrowidnumber,
    patientinfection_medicationidnumber,
    patientinfectionidnumber,
    patientprescriptionmedsidnumber,
    patientidnumber,
    extract_date(substring(addeddate, 1, 10), '%Y-%m-%d') as addeddate,
    extract_date(substring(editdate, 1, 10), '%Y-%m-%d') as editdate,
    extract_date(substring(inactivatedate, 1, 10), '%Y-%m-%d') as inactivatedate
FROM patientinfection_medication
);

DROP TABLE IF EXISTS clean_patientinstabilityhistory;
CREATE TABLE clean_patientinstabilityhistory AS (
SELECT
    analyticrowidnumber as record_id,
    clinicorganizationidnumber,
    hvid,
    patientdataanalyticrowidnumber,
    patientinstabilityhistoryidnumber,
    patientidnumber,
    extract_date(substring(eventdate, 1, 10), '%Y-%m-%d') as eventdate,
    event,
    eventnotes,
    extract_date(substring(dateofcia, 1, 10), '%Y-%m-%d') as dateofcia,
    cklresultid,
    relationid,
    relationtype,
    extract_date(substring(addeddate, 1, 10), '%Y-%m-%d') as addeddate,
    extract_date(substring(editdate, 1, 10), '%Y-%m-%d') as editdate,
    deleted,
    extract_date(substring(deleteddate, 1, 10), '%Y-%m-%d') as deleteddate,
    extract_date(substring(inactivatedate, 1, 10), '%Y-%m-%d') as inactivatedate
FROM patientinstabilityhistory
);


DROP TABLE IF EXISTS clean_patientmasterscheduleheader;
CREATE TABLE clean_patientmasterscheduleheader AS (
    SELECT analyticrowidnumber as record_id,
           patientdataanalyticrowidnumber,
           hvid,
           clinicorganizationidnumber,
           patientmasterscheduleheaderid,
           patientidnumber,
           patientidnumber2,
           clinicid,
           scheduletype,
           extract_date(substring(startdate, 1, 10), '%Y-%m-%d') as startdate,
           extract_date(substring(enddate, 1, 10), '%Y-%m-%d') as enddate,
           '' as starttime,
           '' as endtime,
           scheduleshift,
           patientstatus,
           patientstatus2,
           patientstatus_original,
           statusisactive,
           txtypeidnumber,
           reoccurrencetype,
           recurevery,
           mon,
           tue,
           wed,
           thu,
           fri,
           sat,
           sun,
           disabled,
           extract_date(substring(addeddate, 1, 10), '%Y-%m-%d') as addeddate,
           extract_date(substring(editdate, 1, 10), '%Y-%m-%d') as editdate,
           clinicscheduleid,
           extract_date(substring(originaldate, 1, 10), '%Y-%m-%d') as originaldate,
           reasontransferred,
           referringphysician,
           networkevent,
           extract_date(substring(inactivatedate, 1, 10), '%Y-%m-%d') as inactivatedate,
           sessionsperweek
    FROM patientmasterscheduleheader
    group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38
);

DROP TABLE IF EXISTS clean_patientmedadministered;
CREATE TABLE clean_patientmedadministered AS (
    SELECT analyticrowidnumber as record_id,
         patientdataanalyticrowidnumber,
         hvid,
         clinicorganizationidnumber,
         patientadministeredmedsidnumber,
         patientidnumber,
         analyticdos,
         runidnumber,
         medidnumber,
         medication,
         route,
         dose,
         frequency,
         duration,
         --note: clean prescription manually
         prescription,
         extract_date(substring(startdate, 1, 10), '%Y-%m-%d')         as startdate,
         extract_date(substring(enddate, 1, 10), '%Y-%m-%d')           as enddate,
         prn,
         adminduringrun,
         extract_date(substring(addeddate, 1, 10), '%Y-%m-%d')         as addeddate,
         extract_date(substring(editdate, 1, 10), '%Y-%m-%d')          as editdate,
         '' as administrationtime,
         CLEAN_UP_DIAGNOSIS_CODE(replace(runjustification, '.', ''), '01', NULL)  as runjustification,
         medprescidnumber,
         totaldoses,
         dosesgiven,
         usedoses,
         doseqty,
         doseunit,
         doseroute,
         dosefreq,
         dosefreqmonday,
         dosefreqtuesday,
         dosefreqwednesday,
         dosefreqthursday,
         dosefreqfriday,
         dosefreqsaturday,
         dosefreqsunday,
         hold,
         holduntil,
         extract_date(substring(lastdoseon, 1, 10), '%Y-%m-%d')      as lastdoseon,
         patientnottaking,
         physicianidnumber,
         fixedweekinterval,
         extract_date(substring(datenextdose, 1, 10), '%Y-%m-%d')      as datenextdose,
         extract_date(substring(datedoselastgiven, 1, 10), '%Y-%m-%d') as datedoselastgiven,
         administrationcode,
         procedurecode,
         medicationcode,
         patientprovided,
         esrdrelated,
         extract_date(substring(inactivatedate, 1, 10), '%Y-%m-%d')    as inactivatedate,
         selfadmin,
         adminduringfacility,
         bulksupply,
         treatmentidnumber,
         CLEAN_UP_DIAGNOSIS_CODE(replace(icd10, '.', ''), '02', NULL)  as icd10,
         genproduct_id,
         drug_id
    FROM patientmedadministered
    WHERE CLEAN_UP_DIAGNOSIS_CODE(replace(icd10, '.', ''), '02', NULL)  IS NOT NULL
    group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,
             31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58,59
);

DROP TABLE IF EXISTS clean_patientmednotgiven;
CREATE TABLE clean_patientmednotgiven AS (
    SELECT analyticrowidnumber as record_id,
           patientdataanalyticrowidnumber,
           hvid,
           clinicorganizationidnumber,
           runmedsactualadministereddeleteidnumber,
           patientadministeredmedsidnumber,
           patientidnumber,
           analyticdos,
           runidnumber,
           medidnumber,
           medication,
           route,
           dose,
           frequency,
           duration,
           case
                WHEN lower(prescription) = '0.25 mcg orally (0.25 mcg capsule)  once a day order sent to veracare 6/8/18' then '0.25 mcg orally (0.25 mcg capsule)  once a day order sent 6/8/18'
                WHEN lower(prescription) = '1 ea transdermally (0.3 mg/24 hr film, extended release)  each sat to be changed at dialysis pt to wear 2 per sarac ****' then '1 ea transdermally (0.3 mg/24 hr film, extended release)  each sat to be changed at dialysis pt to wear 2 ****'
                WHEN lower(prescription) = '1 ea transdermally (0.3 mg/24 hr film, extended release)  each tue to be changed at dialysis pt to wear 2 per sarac ****' then '1 ea transdermally (0.3 mg/24 hr film, extended release)  each tue to be changed at dialysis pt to wear 2 ****'
                WHEN lower(prescription) = '1 g orally (500 mg powder for injection)  3 to 4 times a week x 3 doses (0 given of 3) x3 doses per pandya post vac procedure' then '1 g orally (500 mg powder for injection)  3 to 4 times a week x 3 doses (0 given of 3) x3 doses post vac procedure'
                WHEN lower(prescription) = '100 mcg subcutaneously (100 mcg/ml solution)  every 4 weeks pt in gsk study discuss esa & iron with research' then '100 mcg subcutaneously (100 mcg/ml solution)  every 4 weeks pt in study discuss esa & iron with research'
                WHEN lower(prescription) = '2000 units intravenously each wed gsk study any esa/iron dose question call research' then '2000 units intravenously each wed any esa/iron dose question call research'
                WHEN lower(prescription) = '2000 units subcutaneously (20000 units/ml solution)  3 times may combine doses for missed treatments-kathy jo 5' then '2000 units subcutaneously (20000 units/ml solution)  3 times may combine doses for missed treatments'
                WHEN lower(prescription) = '3000 units intravenously ( )  each wed gsk study any esa/iron dose question call research' then '3000 units intravenously ( )  each wed any esa/iron dose question call research'
                WHEN lower(prescription) = '3000 units intravenously ( )  each wed gsk study any question call research' then '3000 units intravenously ( )  each wed any question call research'
                WHEN lower(prescription) = '50 mg intravenously each mon fri x 2 doses (0 given of 2) supplied by nursing home anna maria of aurora' then '50 mg intravenously each mon fri x 2 doses (0 given of 2) supplied by nursing home'
                WHEN lower(prescription) = '60 mcg intravenously (25 mcg/ml solution)  each mon bob is willing to have it administered iv' then '60 mcg intravenously (25 mcg/ml solution)  each mon willing to have it administered iv'
                WHEN lower(prescription) = '0 mg intravenously ( )  3 to 4 times a week medication provided by anna marie of aurora ****' then '0 mg intravenously ( )  3 to 4 times a week medication ****'
                WHEN lower(prescription) = '0 mg intravenously 3 to 4 times a week x 1 doses (0 given of 1) medication provided by anna marie of aurora' then '0 mg intravenously 3 to 4 times a week x 1 doses (0 given of 1) medication'
                WHEN lower(prescription) = '100 mcg subcutaneously (100 mcg/ml solution)  every 2 weeks pt in gsk study discuss esa & iron with research' then '100 mcg subcutaneously (100 mcg/ml solution)  every 2 weeks pt in study discuss esa & iron with research'
                WHEN lower(prescription) = '40 mcg subcutaneously (60 mcg/0.3 ml solution)  once a week pt in gsk study discuss anemia with research' then '40 mcg subcutaneously (60 mcg/0.3 ml solution)  once a week pt in study discuss anemia with research'
                WHEN lower(prescription) = '80 mcg intravenously (25 mcg/0.42 ml solution)  each mon bob is willing to have it administered iv' then '80 mcg intravenously (25 mcg/0.42 ml solution)  each mon willing to have it administered iv'
                WHEN lower(prescription) = '800 mg intravenously (500 mg powder for injection)  each mon wed fri x 18 doses (0 given of 18) provided by northern riverview nursing home' then '800 mg intravenously (500 mg powder for injection)  each mon wed fri x 18 doses (0 given of 18) provided by nursing home'
                else prescription
           end as prescription,
           extract_date(substring(startdate, 1, 10), '%Y-%m-%d')         as startdate,
           extract_date(substring(enddate, 1, 10), '%Y-%m-%d')           as enddate,
           prn,
           adminduringrun,
           extract_date(substring(addeddate, 1, 10), '%Y-%m-%d')         as addeddate,
           extract_date(substring(editdate, 1, 10), '%Y-%m-%d')          as editdate,
           '' as administrationtime,
           CLEAN_UP_DIAGNOSIS_CODE(replace(runjustification, '.', ''), '01', NULL)  as runjustification,
           medprescidnumber,
           totaldoses,
           dosesgiven,
           usedoses,
           doseqty,
           doseunit,
           doseroute,
           dosefreq,
           dosefreqmonday,
           dosefreqtuesday,
           dosefreqwednesday,
           dosefreqthursday,
           dosefreqfriday,
           dosefreqsaturday,
           dosefreqsunday,
           hold,
           holduntil,
           substr(lastdoseon, 1, 10) as lastdoseon,
           patientnottaking,
           physicianidnumber,
           fixedweekinterval,
           extract_date(substring(datenextdose, 1, 10), '%Y-%m-%d')      as datenextdose,
           extract_date(substring(datedoselastgiven, 1, 10), '%Y-%m-%d') as datedoselastgiven,
           administrationcode,
           procedurecode,
           medicationcode,
           patientprovided,
           esrdrelated,
           patientprescriptionmedsparentid,
           extract_date(substring(inactivatedate, 1, 10), '%Y-%m-%d')    as inactivatedate,
           selfadmin,
           adminduringfacility,
           bulksupply,
           treatmentidnumber,
           CLEAN_UP_DIAGNOSIS_CODE(replace(icd10, '.', ''), '02', NULL)  as icd10,
           genproduct_id,
           drug_id
    FROM patientmednotgiven
    group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,
             31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58,59,
             60,61

);

DROP TABLE IF EXISTS clean_patientmedprescription;
CREATE TABLE clean_patientmedprescription AS (
    SELECT analyticrowidnumber as record_id,
         patientdataanalyticrowidnumber,
         hvid,
         clinicorganizationidnumber,
         patientadministeredmedsidnumber,
         patientidnumber,
         medidnumber,
         medication,
         --note: clean prescription manually
         prescription,
         hold,
         holduntil,
         extract_date(substring(startdate, 1, 10), '%Y-%m-%d')         as startdate,
         extract_date(substring(enddate, 1, 10), '%Y-%m-%d')           as enddate,
         prn,
         adminduringrun,
         extract_date(substring(addeddate, 1, 10), '%Y-%m-%d')         as addeddate,
         extract_date(substring(editdate, 1, 10), '%Y-%m-%d')          as editdate,
         lastdoseon,
         patientnottaking,
         CLEAN_UP_DIAGNOSIS_CODE(replace(runjustification, '.', ''), '01', NULL)  as runjustification,
         physicianidnumber,
         totaldoses,
         dosesgiven,
         usedoses,
         doseqty,
         doseunit,
         doseroute,
         dosefreq,
         dosefreqmonday,
         dosefreqtuesday,
         dosefreqwednesday,
         dosefreqthursday,
         dosefreqfriday,
         dosefreqsaturday,
         dosefreqsunday,
         fixedweekinterval,
         extract_date(substring(datenextdose, 1, 10), '%Y-%m-%d')      as datenextdose,
         extract_date(substring(datedoselastgiven, 1, 10),
                      '%Y-%m-%d')                                      as datedoselastgiven,
         administrationcode,
         patientprovided,
         patientprescriptionmedsparentid,
         protocolmed,
         patientmasterscheduleheaderid,
         extract_date(substring(updatereason, 1, 10), '%Y-%m-%d')      as updatereason,
         esrdrelated,
         analyticdos,
         extract_date(substring(inactivatedate, 1, 10),
                      '%Y-%m-%d')                                      as inactivatedate,
         monthlydose,
         selfadmin,
         adminduringfacility,
         bulksupply,
         CLEAN_UP_DIAGNOSIS_CODE(replace(icd10, '.', ''), '02', NULL)  as icd10,
         genproduct_id,
         drug_id,
         donotsubstitute,
         startingdosesgiven,
         dosestrength,
         doseform,
         eprescribed,
         eprescribedquantity,
         eprescribedrefill,
         extract_date(substring(eprescribeddate, 1, 10),
                      '%Y-%m-%d')                                      as eprescribeddate
    FROM patientmedprescription
);

DROP TABLE IF EXISTS clean_patientstatushistory;
CREATE TABLE clean_patientstatushistory AS (
    SELECT
        record_id,
        clinicorganizationidnumber,
        hvid,
        patientstatushistoryidnumber,
        status_original,
        patientstatus,
        statusisactive,
        --anonymize death date
        case when status_original = 'Deceased' then substr(datelaststatuschange, 1, 7)
            else datelaststatuschange end as datelaststatuschange,
        case when status_original = 'Deceased' then substr(addeddate, 1, 7)
            else addeddate end as addeddate,
        case when status_original = 'Deceased' then substr(editdate, 1, 7)
            else editdate end as editdate,
        editdatehistory,
        inactivatedate,
        case when status_original = 'Deceased' then substr(analyticdos, 1, 7)
            else analyticdos end as analyticdos
    FROM (
             SELECT analyticrowidnumber as record_id,
                    clinicorganizationidnumber,
                    hvid,
                    patientdataanalyticrowidnumber,
                    patientstatushistoryidnumber,
                    patientidnumber,
                    status_original,
                    patientstatus,
                    statusisactive,
                    extract_date(substring(datelaststatuschange, 1, 10),
                                 '%Y-%m-%d')                                    as datelaststatuschange,
                    extract_date(substring(addeddate, 1, 10), '%Y-%m-%d')       as addeddate,
                    extract_date(substring(editdate, 1, 10), '%Y-%m-%d')        as editdate,
                    extract_date(substring(editdatehistory, 1, 10), '%Y-%m-%d') as editdatehistory,
                    extract_date(substring(inactivatedate, 1, 10), '%Y-%m-%d')  as inactivatedate,
                    analyticdos
             FROM patientstatushistory
         )
    group by 1,2,3,4,5,6,7,8,9,10,11,12,13
);

DROP TABLE IF EXISTS clean_problemlist;
CREATE TABLE clean_problemlist AS (
    SELECT analyticrowidnumber as record_id,
           patientdataanalyticrowidnumber,
           hvid,
           clinicorganizationidnumber,
           problemlistidnumber,
           patientidnumber,
           analyticdos,
           extract_date(substring(startdate, 1, 10), '%Y-%m-%d')      as startdate,
           extract_date(substring(enddate, 1, 10), '%Y-%m-%d')        as enddate,
           active,
           category,
           displayonreport,
           CLEAN_UP_DIAGNOSIS_CODE(replace(icd9, '.', ''), '01', NULL)  as icd9,
           icd9text,
           CLEAN_UP_DIAGNOSIS_CODE(replace(icd10, '.', ''), '02', NULL)  as icd10,
           icd10text,
           patientcms2728idnumber,
           cms2728code,
           extract_date(substring(addeddate, 1, 10), '%Y-%m-%d')      as addeddate,
           extract_date(substring(editdate, 1, 10), '%Y-%m-%d')       as editdate,
           extract_date(substring(inactivatedate, 1, 10), '%Y-%m-%d') as inactivatedate
    FROM problemlist
    group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21
);

DROP TABLE IF EXISTS clean_sodiumufprofile;
CREATE TABLE clean_sodiumufprofile AS (
SELECT
    analyticrowidnumber as record_id,
    profileidnumber,
    profilenumber,
    profiletype,
    begininglevel,
    endinglevel,
    extract_date(substring(addeddate, 1, 10), '%Y-%m-%d') as addeddate,
    extract_date(substring(editdate, 1, 10), '%Y-%m-%d') as editdate,
    extract_date(substring(inactivatedate, 1, 10), '%Y-%m-%d') as inactivatedate
FROM sodiumufprofile
);

DROP TABLE IF EXISTS clean_stategeo;
CREATE TABLE clean_stategeo AS (
SELECT
    stateabbreviation,
    statename,
    statefipscode,
    divisionname,
    censusbureaudivisionnumber,
    regionname,
    censusbureauregionnumber
FROM stategeo
);

DROP TABLE IF EXISTS clean_zipgeo;
CREATE TABLE clean_zipgeo AS (
SELECT
    geo_id,
    zipcode,
    sumlevel,
    geo_name,
    totalpopulation,
    urbanpopulation,
    insideurbanarea,
    insideurbancluster,
    ruralpopulation,
    na,
    ruralvsurban
FROM zipgeo
);

