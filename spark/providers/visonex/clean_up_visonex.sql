INSERT INTO clean_address
SELECT
    analyticrowidnumber,
    clinicorganizationidnumber,
    patientdataanalyticrowidnumber,
    state,
    mask_zip_code(substring(zipcode, 1, 3)) as zipcode,
    patientaddressidnumber,
    patientidnumber,
    extract_date(substring(inactivatedate, 1, 10), '%Y-%m-%d') as inactivatedate,
    associationidnumber
FROM address
;

INSERT INTO clean_clinicpreference
SELECT
    analyticrowidnumber,
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
;

INSERT INTO clean_dialysistraining
SELECT
    analyticrowidnumber,
    clinicorganizationidnumber,
    patientdataanalyticrowidnumber,
    cklresultid,
    patientidnumber,
    analyticdos,
    extract_date(substring(addeddate, 1, 10), '%Y-%m-%d') as addeddate,
    extract_date(substring(editdate, 1, 10), '%Y-%m-%d') as editdate,
    trainingtype,
    othertraining,
    expectedselfcare,
    dialysisperiod,
    dialysistrainingstart,
    dialysistrainingend,
    extract_date(substring(inactivatedate, 1, 10), '%Y-%m-%d') as inactivatedate
FROM dialysistraining
;

INSERT INTO clean_dialysistreatment
SELECT
    analyticrowidnumber,
    clinicorganizationidnumber,
    patientdataanalyticrowidnumber,
    runidnumber,
    patientidnumber,
    analyticdos,
    status_original,
    patientstatus,
    primarydialysissetting,
    clinicidnumber,
    locationid,
    outpatientrun,
    treatmentstart,
    treatmentend,
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
    dryweight,
    dryweightuom,
    lastweight,
    lastweightuom,
    preweight,
    preweightuom,
    postweight,
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
    extract_date(substring(posteddate, 1, 10), '%Y-%m-%d') as posteddate,
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
    extract_date(substring(addeddate, 1, 10), '%Y-%m-%d') as addeddate,
    extract_date(substring(editdate, 1, 10), '%Y-%m-%d') as editdate,
    extract_date(substring(inactivatedate, 1, 10), '%Y-%m-%d') as inactivatedate,
    statusisactive,
    patientvascularaccesssecondaryidnumber,
    cfresultidnumber,
    patientdialysisrxheaderidnumber,
    treatmenttypecategory,
    treatmenttypesubtype,
    isprimary
FROM dialysistreatment
;

INSERT INTO clean_facilityadmitdischarge
SELECT
    analyticrowidnumber,
    clinicorganizationidnumber,
    patientdataanalyticrowidnumber,
    patientfacilityidnumber,
    patientidnumber,
    clinicidnumber,
    extract_date(substring(admitdate, 1, 10), '%Y-%m-%d') as admitdate,
    admitreason,
    extract_date(substring(dischargedate, 1, 10), '%Y-%m-%d') as dischargedate,
    dischargereason,
    extract_date(substring(addeddate, 1, 10), '%Y-%m-%d') as addeddate,
    extract_date(substring(editdate, 1, 10), '%Y-%m-%d') as editdate,
    networkevent,
    patientmasterscheduleheaderid,
    patientmasterscheduleid,
    involuntarydischargereason,
    transferdischargereason,
    transientreason,
    extract_date(substring(inactivatedate, 1, 10), '%Y-%m-%d') as inactivatedate,
    analyticdos
FROM facilityadmitdischarge
;

INSERT INTO clean_hospitalization
SELECT
    analyticrowidnumber,
    clinicorganizationidnumber,
    patientdataanalyticrowidnumber,
    patienthospitalizationidnumber,
    patientidnumber,
    admittingphysicianidnumber,
    extract_date(substring(admissiondate, 1, 10), '%Y-%m-%d') as admissiondate,
    extract_date(substring(dischargedate, 1, 10), '%Y-%m-%d') as dischargedate,
    extract_date(substring(addeddate, 1, 10), '%Y-%m-%d') as addeddate,
    extract_date(substring(editdate, 1, 10), '%Y-%m-%d') as editdate,
    hospconsult,
    unstable,
    staylength,
    icd9,
    patientdialyzed,
    typevisit,
    presumptivediagnosis,
    transplantreferral,
    extract_date(substring(inactivatedate, 1, 10), '%Y-%m-%d') as inactivatedate,
    icd10,
    analyticdos
FROM hospitalization
;

INSERT INTO clean_immunization
SELECT
    analyticrowidnumber,
    clinicorganizationidnumber,
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
;

INSERT INTO clean_insurance
SELECT
    analyticrowidnumber,
    clinicorganizationidnumber,
    patientdataanalyticrowidnumber,
    insuranceidnumber,
    patientidnumber,
    insurancecompanyidnumber,
    insurancecompanyname,
    insurancecompanyid,
    insurancecompanyaddressline1,
    insurancecompanyaddressline2,
    insurancecompanycity,
    insurancecompanystate,
    insurancecompanyzipcode,
    insurancecompanyprimaryphone,
    payorid,
    extract_date(substring(planeffectivedate, 1, 10), '%Y-%m-%d') as planeffectivedate,
    extract_date(substring(planexpirationdate, 1, 10), '%Y-%m-%d') as planexpirationdate,
    authorizationnumber,
    plantype,
    extract_date(substring(addeddate, 1, 10), '%Y-%m-%d') as addeddate,
    extract_date(substring(editdate, 1, 10), '%Y-%m-%d') as editdate,
    insurednumberid,
    medicareparta,
    companyplantype,
    extract_date(substring(inactivatedate, 1, 10), '%Y-%m-%d') as inactivatedate
FROM insurance
;

INSERT INTO clean_labidlist
SELECT
    analyticrowidnumber,
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
;

INSERT INTO clean_labpanelsdrawn
SELECT
    analyticrowidnumber,
    clinicorganizationidnumber,
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
;

INSERT INTO clean_labresult
SELECT
    analyticrowidnumber,
    clinicorganizationidnumber,
    patientdataanalyticrowidnumber,
    analyticdos,
    labresultidnumber,
    patientidnumber,
    orderedby,
    extract_date(substring(orderdate, 1, 10), '%Y-%m-%d') as orderdate,
    extract_date(substring(completeddate, 1, 10), '%Y-%m-%d') as completeddate,
    extract_date(substring(receiveddate, 1, 10), '%Y-%m-%d') as receiveddate,
    testname,
    testresult,
    extract_date(substring(addeddate, 1, 10), '%Y-%m-%d') as addeddate,
    extract_date(substring(editdate, 1, 10), '%Y-%m-%d') as editdate,
    calculated,
    scaled,
    textresult,
    referencerange,
    observationidentifier,
    universalserviceid,
    fmtresult,
    abnormalflags,
    diagnosiscode,
    esrdrelated,
    runidnumber,
    medicationcode,
    administrationcode,
    procedurecode,
    extract_date(substring(inactivatedate, 1, 10), '%Y-%m-%d') as inactivatedate,
    icd10
FROM labresult
;

INSERT INTO clean_medication
SELECT
    medidnumber,
    extract_date(substring(addeddate, 1, 10), '%Y-%m-%d') as addeddate,
    extract_date(substring(lasteditdate, 1, 10), '%Y-%m-%d') as lasteditdate,
    extract_date(substring(inactivatedate, 1, 10), '%Y-%m-%d') as inactivatedate
FROM medication
;

INSERT INTO clean_medicationgroup
SELECT
    medicationgroupidnumber,
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
;

INSERT INTO clean_modalitychangehistorycrownweb
SELECT
    analyticrowidnumber,
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
;

INSERT INTO clean_nursinghomehistory
SELECT
    analyticrowidnumber,
    clinicorganizationidnumber,
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
;

INSERT INTO clean_patientaccess
SELECT
    analyticrowidnumber,
    patientdataanalyticrowidnumber,
    clinicorganizationidnumber,
    patientvascularaccessidnumber,
    patientidnumber,
    vascularaccesstype,
    location,
    currentstatus,
    chroniccatheter,
    analyticdos,
    extract_date(substring(startdate, 1, 10), '%Y-%m-%d') as startdate,
    extract_date(substring(enddate, 1, 10), '%Y-%m-%d') as enddate,
    extract_date(substring(addeddate, 1, 10), '%Y-%m-%d') as addeddate,
    extract_date(substring(editdate, 1, 10), '%Y-%m-%d') as editdate,
    placedrecorded,
    isprimaryaccess,
    isstateactive,
    extract_date(substring(inactivatedate, 1, 10), '%Y-%m-%d') as inactivatedate
FROM patientaccess
;

INSERT INTO clean_patientaccess_examproc
SELECT
    analyticrowidnumber,
    clinicorganizationidnumber,
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
;

INSERT INTO clean_patientaccess_otheraccessevent
SELECT
    analyticrowidnumber,
    clinicorganizationidnumber,
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
;

INSERT INTO clean_patientaccess_placedrecorded
SELECT
    analyticrowidnumber,
    clinicorganizationidnumber,
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
;

INSERT INTO clean_patientaccess_removed
SELECT
    analyticrowidnumber,
    clinicorganizationidnumber,
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
;

INSERT INTO clean_patientallergy
SELECT
    analyticrowidnumber,
    clinicorganizationidnumber,
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
;

INSERT INTO clean_patientcms2728
SELECT
    analyticrowidnumber,
    clinicorganizationidnumber,
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
;

INSERT INTO clean_patientcomorbidityandtransplantstate
SELECT
    analyticrowidnumber,
    clinicorganizationidnumber,
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
;

INSERT INTO clean_patientdata
SELECT
    analyticrowidnumber,
    clinicorganizationidnumber,
    defaultclinicidnumber,
    patientidnumber,
    patientid,
    hvid,
    medicalrecordnumber,
    labidnumber,
    patientdata.state,
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
    resuscitationcode,
    advdirresuscitationcode,
    advdirpowerofatty,
    advdirlivingwill,
    advdirpatientdeclines,
    extract_date(substring(advrevdate, 1, 10), '%Y-%m-%d') as advrevdate,
    tribecode,
    extract_date(substring(inactivatedate, 1, 10), '%Y-%m-%d') as inactivatedate,
    cap_age(patientdata.age) as age,
    monthsindialysis,
    transplantwaitlist,
    medicalcoveragemedicare,
    extract_date(substring(medicalcoveragemedicareeffectivedate, 1, 10), '%Y-%m-%d') as medicalcoveragemedicareeffectivedate,
    masterpatientidnumber,
    extract_date(substring(datefirstdialysiscurrentunit, 1, 10), '%Y-%m-%d') as datefirstdialysiscurrentunit
FROM patientdata
LEFT JOIN matching_payload ON analyticrowidnumber = claimId
;

INSERT INTO clean_patientdiagcodes
SELECT
    analyticrowidnumber,
    clinicorganizationidnumber,
    patientdataanalyticrowidnumber,
    patientdiagcodeid,
    patientidnumber,
    diagnosiscode,
    description,
    extract_date(substring(diagnosisdate, 1, 10), '%Y-%m-%d') as diagnosisdate,
    diagnosistype,
    diagnosispriority,
    admitting,
    extract_date(substring(addeddate, 1, 10), '%Y-%m-%d') as addeddate,
    extract_date(substring(editdate, 1, 10), '%Y-%m-%d') as editdate,
    extract_date(substring(diagnosisenddate, 1, 10), '%Y-%m-%d') as diagnosisenddate,
    orderedbyidnumber,
    extract_date(substring(inactivatedate, 1, 10), '%Y-%m-%d') as inactivatedate,
    icd10,
    analyticdos
FROM patientdiagcodes
;

INSERT INTO clean_patientdialysisprescription
SELECT
    analyticrowidnumber,
    clinicorganizationidnumber,
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
;

INSERT INTO clean_patientdialysisrxhemo
SELECT
    analyticrowidnumber,
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
;

INSERT INTO clean_patientdialysisrxpd
SELECT
    analyticrowidnumber,
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
;

INSERT INTO clean_patientdialysisrxpdexchanges
SELECT
    analyticrowidnumber,
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
;

INSERT INTO clean_patientevent
SELECT
    analyticrowidnumber,
    clinicorganizationidnumber,
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
;

INSERT INTO clean_patientfluidweightmanagement
SELECT
    analyticrowidnumber,
    clinicorganizationidnumber,
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
;

INSERT INTO clean_patientheighthistory
SELECT
    analyticrowidnumber,
    clinicorganizationidnumber,
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
;

INSERT INTO clean_patientinfection
SELECT
    analyticrowidnumber,
    clinicorganizationidnumber,
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
;

INSERT INTO clean_patientinfection_laborganism
SELECT
    analyticrowidnumber,
    clinicorganizationidnumber,
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
;

INSERT INTO clean_patientinfection_laborganismdrug
SELECT
    analyticrowidnumber,
    clinicorganizationidnumber,
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
;

INSERT INTO clean_patientinfection_labresultculture
SELECT
    analyticrowidnumber,
    clinicorganizationidnumber,
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
;

INSERT INTO clean_patientinfection_medication
SELECT
    analyticrowidnumber,
    clinicorganizationidnumber,
    patientdataanalyticrowidnumber,
    patientinfection_medicationidnumber,
    patientinfectionidnumber,
    patientprescriptionmedsidnumber,
    patientidnumber,
    extract_date(substring(addeddate, 1, 10), '%Y-%m-%d') as addeddate,
    extract_date(substring(editdate, 1, 10), '%Y-%m-%d') as editdate,
    extract_date(substring(inactivatedate, 1, 10), '%Y-%m-%d') as inactivatedate
FROM patientinfection_medication
;

INSERT INTO clean_patientinstabilityhistory
SELECT
    analyticrowidnumber,
    clinicorganizationidnumber,
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
;

INSERT INTO clean_patientmasterscheduleheader
SELECT
    analyticrowidnumber,
    patientdataanalyticrowidnumber,
    clinicorganizationidnumber,
    patientmasterscheduleheaderid,
    patientidnumber,
    patientidnumber2,
    clinicid,
    scheduletype,
    extract_date(substring(startdate, 1, 10), '%Y-%m-%d') as startdate,
    extract_date(substring(enddate, 1, 10), '%Y-%m-%d') as enddate,
    starttime,
    endtime,
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
;

INSERT INTO clean_patientmedadministered
SELECT
    analyticrowidnumber,
    patientdataanalyticrowidnumber,
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
    prescription,
    extract_date(substring(startdate, 1, 10), '%Y-%m-%d') as startdate,
    extract_date(substring(enddate, 1, 10), '%Y-%m-%d') as enddate,
    prn,
    adminduringrun,
    extract_date(substring(addeddate, 1, 10), '%Y-%m-%d') as addeddate,
    extract_date(substring(editdate, 1, 10), '%Y-%m-%d') as editdate,
    administrationtime,
    runjustification,
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
    lastdoseon,
    patientnottaking,
    physicianidnumber,
    fixedweekinterval,
    extract_date(substring(datenextdose, 1, 10), '%Y-%m-%d') as datenextdose,
    extract_date(substring(datedoselastgiven, 1, 10), '%Y-%m-%d') as datedoselastgiven,
    administrationcode,
    procedurecode,
    medicationcode,
    patientprovided,
    esrdrelated,
    extract_date(substring(inactivatedate, 1, 10), '%Y-%m-%d') as inactivatedate,
    selfadmin,
    adminduringfacility,
    bulksupply,
    treatmentidnumber,
    icd10,
    genproduct_id,
    drug_id
FROM patientmedadministered
;

INSERT INTO clean_patientmednotgiven
SELECT
    analyticrowidnumber,
    patientdataanalyticrowidnumber,
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
    prescription,
    extract_date(substring(startdate, 1, 10), '%Y-%m-%d') as startdate,
    extract_date(substring(enddate, 1, 10), '%Y-%m-%d') as enddate,
    prn,
    adminduringrun,
    extract_date(substring(addeddate, 1, 10), '%Y-%m-%d') as addeddate,
    extract_date(substring(editdate, 1, 10), '%Y-%m-%d') as editdate,
    administrationtime,
    runjustification,
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
    lastdoseon,
    patientnottaking,
    physicianidnumber,
    fixedweekinterval,
    extract_date(substring(datenextdose, 1, 10), '%Y-%m-%d') as datenextdose,
    extract_date(substring(datedoselastgiven, 1, 10), '%Y-%m-%d') as datedoselastgiven,
    administrationcode,
    procedurecode,
    medicationcode,
    patientprovided,
    esrdrelated,
    patientprescriptionmedsparentid,
    extract_date(substring(inactivatedate, 1, 10), '%Y-%m-%d') as inactivatedate,
    selfadmin,
    adminduringfacility,
    bulksupply,
    treatmentidnumber,
    icd10,
    genproduct_id,
    drug_id
FROM patientmednotgiven
;

INSERT INTO clean_patientmedprescription
SELECT
    analyticrowidnumber,
    patientdataanalyticrowidnumber,
    clinicorganizationidnumber,
    patientadministeredmedsidnumber,
    patientidnumber,
    medidnumber,
    medication,
    prescription,
    hold,
    holduntil,
    extract_date(substring(startdate, 1, 10), '%Y-%m-%d') as startdate,
    extract_date(substring(enddate, 1, 10), '%Y-%m-%d') as enddate,
    prn,
    adminduringrun,
    extract_date(substring(addeddate, 1, 10), '%Y-%m-%d') as addeddate,
    extract_date(substring(editdate, 1, 10), '%Y-%m-%d') as editdate,
    lastdoseon,
    patientnottaking,
    runjustification,
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
    extract_date(substring(datenextdose, 1, 10), '%Y-%m-%d') as datenextdose,
    extract_date(substring(datedoselastgiven, 1, 10), '%Y-%m-%d') as datedoselastgiven,
    administrationcode,
    patientprovided,
    patientprescriptionmedsparentid,
    protocolmed,
    patientmasterscheduleheaderid,
    extract_date(substring(updatereason, 1, 10), '%Y-%m-%d') as updatereason,
    esrdrelated,
    analyticdos,
    extract_date(substring(inactivatedate, 1, 10), '%Y-%m-%d') as inactivatedate,
    monthlydose,
    selfadmin,
    adminduringfacility,
    bulksupply,
    icd10,
    genproduct_id,
    drug_id,
    donotsubstitute,
    startingdosesgiven,
    dosestrength,
    doseform,
    eprescribed,
    eprescribedquantity,
    eprescribedrefill,
    extract_date(substring(eprescribeddate, 1, 10), '%Y-%m-%d') as eprescribeddate	
FROM patientmedprescription
;

INSERT INTO clean_patientstatushistory
SELECT
    analyticrowidnumber,
    clinicorganizationidnumber,
    patientdataanalyticrowidnumber,
    patientstatushistoryidnumber,
    patientidnumber,
    status_original,
    patientstatus,
    statusisactive,
    extract_date(substring(datelaststatuschange, 1, 10), '%Y-%m-%d') as datelaststatuschange,
    extract_date(substring(addeddate, 1, 10), '%Y-%m-%d') as addeddate,
    extract_date(substring(editdate, 1, 10), '%Y-%m-%d') as editdate,
    extract_date(substring(editdatehistory, 1, 10), '%Y-%m-%d') as editdatehistory,
    extract_date(substring(inactivatedate, 1, 10), '%Y-%m-%d') as inactivatedate,
    analyticdos
FROM patientstatushistory
;

INSERT INTO clean_problemlist
SELECT
    analyticrowidnumber,
    patientdataanalyticrowidnumber,
    clinicorganizationidnumber,
    problemlistidnumber,
    patientidnumber,
    analyticdos,
    extract_date(substring(startdate, 1, 10), '%Y-%m-%d') as startdate,
    extract_date(substring(enddate, 1, 10), '%Y-%m-%d') as enddate,
    active,
    category,
    displayonreport,
    icd9,
    icd9text,
    icd10,
    icd10text,
    patientcms2728idnumber,
    cms2728code,
    extract_date(substring(addeddate, 1, 10), '%Y-%m-%d') as addeddate,
    extract_date(substring(editdate, 1, 10), '%Y-%m-%d') as editdate,
    extract_date(substring(inactivatedate, 1, 10), '%Y-%m-%d') as inactivatedate
FROM problemlist
;

INSERT INTO clean_sodiumufprofile
SELECT
    analyticrowidnumber,
    profileidnumber,
    profilenumber,
    profiletype,
    begininglevel,
    endinglevel,
    extract_date(substring(addeddate, 1, 10), '%Y-%m-%d') as addeddate,
    extract_date(substring(editdate, 1, 10), '%Y-%m-%d') as editdate,
    extract_date(substring(inactivatedate, 1, 10), '%Y-%m-%d') as inactivatedate
FROM sodiumufprofile
;

INSERT INTO clean_stategeo
SELECT
    stateabbreviation,
    statename,
    statefipscode,
    divisionname,
    censusbureaudivisionnumber,
    regionname,
    censusbureauregionnumber
FROM stategeo
;

INSERT INTO clean_zipgeo
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
;

