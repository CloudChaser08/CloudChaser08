-- DROP TABLE IF EXISTS transactional_allergies;
-- CREATE TABLE transactional_allergies (
--         rectype         text ENCODE lzo,
--         rectypeversion  text ENCODE lzo,
--         genclientID     text ENCODE lzo,
--         gen2clientID    text ENCODE lzo,
--         genpatientID    text ENCODE lzo,
--         gen2patientID   text ENCODE lzo,
--         encounterID     text ENCODE lzo,
--         allergyID       text ENCODE lzo,
--         versionID       text ENCODE lzo,
--         auditdataflag   text ENCODE lzo,
--         name            text ENCODE lzo,
--         type            text ENCODE lzo,
--         status          text ENCODE lzo,
--         reactions       text ENCODE lzo,
--         ndc             text ENCODE lzo,
--         ddi             text ENCODE lzo,
--         gpi             text ENCODE lzo,
--         rxnorm          text ENCODE lzo,
--         snomed          text ENCODE lzo,
--         unverifiedflag  text ENCODE lzo,
--         reactionDTTM    text ENCODE lzo,
--         recordedDTTM    text ENCODE lzo,
--         genproviderID   text ENCODE lzo,
--         gen2providerID  text ENCODE lzo,
--         primarykey      text ENCODE lzo
--         ) DISTKEY(encounterID) SORTKEY(encounterID);

-- COPY transactional_allergies FROM :transactional_allergies_input_path CREDENTIALS :credentials EMPTYASNULL BZIP2 DELIMITER '|';
		
-- DROP TABLE IF EXISTS transactional_appointments;
-- CREATE TABLE transactional_appointments (
--     rectype                       text ENCODE lzo,
--     rectypeversion                text ENCODE lzo,
--     genclientID                   text ENCODE lzo,
--     gen2clientID                  text ENCODE lzo,
--     genpatientID                  text ENCODE lzo,
--     gen2patientID                 text ENCODE lzo,
--     encounterID                   text ENCODE lzo,
--     appointmentID                 text ENCODE lzo,
--     versionID                     text ENCODE lzo,
--     auditdataflag                 text ENCODE lzo,
--     status                        text ENCODE lzo,
--     primaryinsurancemedicareflag  text ENCODE lzo,
--     startdttm                     text ENCODE lzo,
--     enddttm                       text ENCODE lzo,
--     recordedDTTM                  text ENCODE lzo,
--     genproviderID                 text ENCODE lzo,
--     gen2ProviderID                text ENCODE lzo,
--     primarykey                    text ENCODE lzo
--     ) DISTKEY(encounterID) SORTKEY(encounterID);

-- COPY transactional_appointments FROM :transactional_appointments_input_path CREDENTIALS :credentials EMPTYASNULL BZIP2 DELIMITER '|';
		
DROP TABLE IF EXISTS transactional_encounters;
CREATE TABLE transactional_encounters (
    rectype                  text ENCODE lzo,
    rectypeversion           text ENCODE lzo,
    genclientID              text ENCODE lzo,
    gen2clientID             text ENCODE lzo,
    genpatientID             text ENCODE lzo,
    gen2patientID            text ENCODE lzo,
    encounterID              text ENCODE lzo,
    versionID                text ENCODE lzo,
    auditdataflag            text ENCODE lzo,
    type                     text ENCODE lzo,
    encounterDTTM            text ENCODE lzo,
    recordedDTTM             text ENCODE lzo,
    genbillingproviderID     text ENCODE lzo,
    billinggen2providerID    text ENCODE lzo,
    genrenderingproviderID   text ENCODE lzo,
    renderinggen2providerID  text ENCODE lzo,
    genproviderID            text ENCODE lzo,
    gen2providerID           text ENCODE lzo,
    primarykey               text ENCODE lzo
    ) DISTKEY(encounterID) SORTKEY(encounterID);

COPY transactional_encounters FROM :transactional_encounters_input_path CREDENTIALS :credentials EMPTYASNULL BZIP2 DELIMITER '|';
		
DROP TABLE IF EXISTS transactional_medications;
CREATE TABLE transactional_medications (
    rectype                       text ENCODE lzo,
    rectypeversion                text ENCODE lzo,
    genclientID                   text ENCODE lzo,
    gen2clientID                  text ENCODE lzo,
    genpatientID                  text ENCODE lzo,
    gen2patientID                 text ENCODE lzo,
    encounterID                   text ENCODE lzo,
    medID                         text ENCODE lzo,
    versionID                     text ENCODE lzo,
    auditdataflag                 text ENCODE lzo,
    administeredbygen2providerID  text ENCODE lzo,
    prescribedbygen2providerID    text ENCODE lzo,
    problemID                     text ENCODE lzo,
    name                          text ENCODE lzo,
    status                        text ENCODE lzo,
    routeofadmin                  text ENCODE lzo,
    ndc                           text ENCODE lzo,
    ddi                           text ENCODE lzo,
    gpi                           text ENCODE lzo,
    rxnorm                        text ENCODE lzo,
    cvx                           text ENCODE lzo,
    sig                           text ENCODE lzo,
    dose                          text ENCODE lzo,
    admindose                     text ENCODE lzo,
    strength                      text ENCODE lzo,
    form                          text ENCODE lzo,
    units                         text ENCODE lzo,
    quantitytodispense            text ENCODE lzo,
    frequency                     text ENCODE lzo,
    frequencyperday               text ENCODE lzo,
    daystotake                    text ENCODE lzo,
    daysupply                     text ENCODE lzo,
    refills                       text ENCODE lzo,
    genericflag                   text ENCODE lzo,
    genericavailableflag          text ENCODE lzo,
    DAWflag                       text ENCODE lzo,
    prescribeaction               text ENCODE lzo,
    MedGroup1                     text ENCODE lzo,
    TherClass1                    text ENCODE lzo,
    SubClass1                     text ENCODE lzo,
    MedGroup2                     text ENCODE lzo,
    TherClass2                    text ENCODE lzo,
    SubClass2                     text ENCODE lzo,
    MedGroup3                     text ENCODE lzo,
    TherClass3                    text ENCODE lzo,
    SubClass3                     text ENCODE lzo,
    MedGroup4                     text ENCODE lzo,
    TherClass4                    text ENCODE lzo,
    SubClass4                     text ENCODE lzo,
    MedGroup5                     text ENCODE lzo,
    TherClass5                    text ENCODE lzo,
    SubClass5                     text ENCODE lzo,
    MedGroup6                     text ENCODE lzo,
    TherClass6                    text ENCODE lzo,
    SubClass6                     text ENCODE lzo,
    errorflag                     text ENCODE lzo,
    eRxtransmittedflag            text ENCODE lzo,
    sampleflag                    text ENCODE lzo,
    unverifiedflag                text ENCODE lzo,
    startDTTM                     text ENCODE lzo,
    endDTTM                       text ENCODE lzo,
    performeddate                 text ENCODE lzo,
    recordedDTTM                  text ENCODE lzo,
    genproviderID                 text ENCODE lzo,
    gen2providerID                text ENCODE lzo,
    primarykey                    text ENCODE lzo
    ) DISTKEY(encounterID) SORTKEY(encounterID);

COPY transactional_medications FROM :transactional_medications_input_path CREDENTIALS :credentials EMPTYASNULL BZIP2 DELIMITER '|';
		
DROP TABLE IF EXISTS transactional_orders;
CREATE TABLE transactional_orders (
    rectype                   text ENCODE lzo,
    rectypeversion            text ENCODE lzo,
    genclientID               text ENCODE lzo,
    gen2clientID              text ENCODE lzo,
    genpatientID              text ENCODE lzo,
    gen2patientID             text ENCODE lzo,
    encounterID               text ENCODE lzo,
    orderID                   text ENCODE lzo,
    versionID                 text ENCODE lzo,
    auditdataflag             text ENCODE lzo,
    name                      text ENCODE lzo,
    type                      text ENCODE lzo,
    status                    text ENCODE lzo,
    cpt4                      text ENCODE lzo,
    cptmod                    text ENCODE lzo,
    cptpos                    text ENCODE lzo,
    billingICD9code           text ENCODE lzo,
    billingICD10code          text ENCODE lzo,
    hcpcs                     text ENCODE lzo,
    source                    text ENCODE lzo,
    specimen                  text ENCODE lzo,
    orderDTTM                 text ENCODE lzo,
    recordedDTTM              text ENCODE lzo,
    approvinggenproviderID    text ENCODE lzo,
    approvinggen2providerID   text ENCODE lzo,
    orderinggenproviderID     text ENCODE lzo,
    orderinggen2providerID    text ENCODE lzo,
    performinggenproviderID   text ENCODE lzo,
    performinggen2providerID  text ENCODE lzo,
    genproviderID             text ENCODE lzo,
    gen2providerID            text ENCODE lzo,
    primarykey                text ENCODE lzo
    ) DISTKEY(encounterID) SORTKEY(encounterID);

COPY transactional_orders FROM :transactional_orders_input_path CREDENTIALS :credentials EMPTYASNULL BZIP2 DELIMITER '|';
		
DROP TABLE IF EXISTS transactional_patients;
CREATE TABLE transactional_patients (
    rectype            text ENCODE lzo,
    rectypeVersion     text ENCODE lzo,
    genclientID        text ENCODE lzo,
    gen2clientID       text ENCODE lzo,
    genpatientID       text ENCODE lzo,
    gen2patientID      text ENCODE lzo,
    dobyear            text ENCODE lzo,
    deceasedFlag       text ENCODE lzo,
    gender             text ENCODE lzo,
    race               text ENCODE lzo,
    ethnicity1         text ENCODE lzo,
    zip3               text ENCODE lzo,
    state              text ENCODE lzo,
    smokingstatusflag  text ENCODE lzo,
    lastupdateDTTM     text ENCODE lzo,
    genproviderID      text ENCODE lzo,
    gen2providerID     text ENCODE lzo,
    primarykey         text ENCODE lzo
    ) DISTKEY(genpatientID) SORTKEY(genpatientID);

COPY transactional_patients FROM :transactional_patients_input_path CREDENTIALS :credentials EMPTYASNULL BZIP2 DELIMITER '|';
		
DROP TABLE IF EXISTS transactional_problems;
CREATE TABLE transactional_problems (
    rectype         text ENCODE lzo,
    rectypeversion  text ENCODE lzo,
    genclientID     text ENCODE lzo,
    gen2clientID    text ENCODE lzo,
    genpatientID    text ENCODE lzo,
    gen2patientID   text ENCODE lzo,
    encounterID     text ENCODE lzo,
    problemID       text ENCODE lzo,
    versionID       text ENCODE lzo,
    auditdataflag   text ENCODE lzo,
    ICD9            text ENCODE lzo,
    ICD10           text ENCODE lzo,
    snomed          text ENCODE lzo,
    medcinID        text ENCODE lzo,
    cptcode         text ENCODE lzo,
    name            text ENCODE lzo,
    type            text ENCODE lzo,
    category        text ENCODE lzo,
    status          text ENCODE lzo,
    level1          text ENCODE lzo,
    level2          text ENCODE lzo,
    level3          text ENCODE lzo,
    errorFlag       text ENCODE lzo,
    diagnosisDTTM   text ENCODE lzo,
    onsetDTTM       text ENCODE lzo,
    resolvedDTTM    text ENCODE lzo,
    recordedDTTM    text ENCODE lzo,
    genproviderID   text ENCODE lzo,
    gen2providerID  text ENCODE lzo,
    primarykey      text ENCODE lzo
    ) DISTKEY(encounterID) SORTKEY(encounterID);

COPY transactional_problems FROM :transactional_problems_input_path CREDENTIALS :credentials EMPTYASNULL BZIP2 DELIMITER '|';
		
-- DROP TABLE IF EXISTS transactional_providers;
-- CREATE TABLE transactional_providers (
--     rectype                  text ENCODE lzo,
--     rectypeversion           text ENCODE lzo,
--     genclientID              text ENCODE lzo,
--     gen2clientID             text ENCODE lzo,
--     genproviderID            text ENCODE lzo,
--     gen2providerID           text ENCODE lzo,
--     NPI_Title                text ENCODE lzo,
--     NPI_Gender               text ENCODE lzo,
--     NPI_TxnCode              text ENCODE lzo,
--     NPI_TxnClass             text ENCODE lzo,
--     NPI_TxnType              text ENCODE lzo,
--     NPI_TxnSpecialty         text ENCODE lzo,
--     NPI_TPVerifiedSpecialty  text ENCODE lzo,
--     NPI_DOByear              text ENCODE lzo,
--     NPI_State                text ENCODE lzo,
--     specialty                text ENCODE lzo,
--     credential               text ENCODE lzo,
--     type                     text ENCODE lzo,
--     NPI                      text ENCODE lzo,
--     pcpflag                  text ENCODE lzo,
--     state                    text ENCODE lzo,
--     inactiveflag             text ENCODE lzo,
--     lastupdateDTTM           text ENCODE lzo,
--     primarykey               text ENCODE lzo
--     ) DISTKEY(genproviderID) SORTKEY(genproviderID);

-- COPY transactional_providers FROM :transactional_providers_input_path CREDENTIALS :credentials EMPTYASNULL BZIP2 DELIMITER '|';
		
DROP TABLE IF EXISTS transactional_results;
CREATE TABLE transactional_results (
    rectype         text ENCODE lzo,
    rectypeversion  text ENCODE lzo,
    genclientID     text ENCODE lzo,
    gen2clientID    text ENCODE lzo,
    genpatientID    text ENCODE lzo,
    gen2patientID   text ENCODE lzo,
    encounterID     text ENCODE lzo,
    resultID        text ENCODE lzo,
    versionID       text ENCODE lzo,
    auditdataflag   text ENCODE lzo,
    orderID         text ENCODE lzo,
    panel           text ENCODE lzo,
    test            text ENCODE lzo,
    value           text ENCODE lzo,
    units           text ENCODE lzo,
    refrange        text ENCODE lzo,
    abnormalflag    text ENCODE lzo,
    resultstatus    text ENCODE lzo,
    loinc           text ENCODE lzo,
    ocdid           text ENCODE lzo,
    errorflag       text ENCODE lzo,
    resultDTTM      text ENCODE lzo,
    performedDTTM   text ENCODE lzo,
    recordedDTTM    text ENCODE lzo,
    genproviderID   text ENCODE lzo,
    gen2providerID  text ENCODE lzo,
    primarykey      text ENCODE lzo
    ) DISTKEY(encounterID) SORTKEY(encounterID);

COPY transactional_results FROM :transactional_results_input_path CREDENTIALS :credentials EMPTYASNULL BZIP2 DELIMITER '|';
		
DROP TABLE IF EXISTS transactional_vaccines;
CREATE TABLE transactional_vaccines (
    rectype           text ENCODE lzo,
    rectypeversion    text ENCODE lzo,
    genclientID       text ENCODE lzo,
    gen2clientID      text ENCODE lzo,
    genpatientID      text ENCODE lzo,
    gen2patientID     text ENCODE lzo,
    encounterID       text ENCODE lzo,
    vaccineID         text ENCODE lzo,
    versionID         text ENCODE lzo,
    auditdataflag     text ENCODE lzo,
    name              text ENCODE lzo,
    status            text ENCODE lzo,
    ndc               text ENCODE lzo,
    ddi               text ENCODE lzo,
    gpi               text ENCODE lzo,
    rxnorm            text ENCODE lzo,
    cvx               text ENCODE lzo,
    series            text ENCODE lzo,
    dose              text ENCODE lzo,
    routeofadmin      text ENCODE lzo,
    bodysite          text ENCODE lzo,
    manufacturer      text ENCODE lzo,
    administeredDTTM  text ENCODE lzo,
    genproviderID     text ENCODE lzo,
    gen2providerID    text ENCODE lzo,
    recordedDTTM      text ENCODE lzo,
    primarykey        text ENCODE lzo
    ) DISTKEY(encounterID) SORTKEY(encounterID);

COPY transactional_vaccines FROM :transactional_vaccines_input_path CREDENTIALS :credentials EMPTYASNULL BZIP2 DELIMITER '|';
		
-- DROP TABLE IF EXISTS transactional_vitals;
-- CREATE TABLE transactional_vitals (
--     rectype         text ENCODE lzo,
--     rectypeversion  text ENCODE lzo,
--     genclientID     text ENCODE lzo,
--     gen2clientID    text ENCODE lzo,
--     genpatientID    text ENCODE lzo,
--     gen2patientID   text ENCODE lzo,
--     encounterID     text ENCODE lzo,
--     vitalID         text ENCODE lzo,
--     versionID       text ENCODE lzo,
--     auditdataflag   text ENCODE lzo,
--     name            text ENCODE lzo,
--     status          text ENCODE lzo,
--     value           text ENCODE lzo,
--     units           text ENCODE lzo,
--     refrange        text ENCODE lzo,
--     errorflag       text ENCODE lzo,
--     clinicalDTTM    text ENCODE lzo,
--     performedDTTM   text ENCODE lzo,
--     recordedDTTM    text ENCODE lzo,
--     genproviderID   text ENCODE lzo,
--     gen2providerID  text ENCODE lzo,
--     primarykey      text ENCODE lzo
--     ) DISTKEY(encounterID) SORTKEY(encounterID);

-- COPY transactional_vitals FROM :transactional_vitals_input_path CREDENTIALS :credentials EMPTYASNULL BZIP2 DELIMITER '|';
