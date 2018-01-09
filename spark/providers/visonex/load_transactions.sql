DROP TABLE IF EXISTS address;
CREATE EXTERNAL TABLE address (
        analyticrowidnumber             string,
        clinicorganizationidnumber      string,
        patientdataanalyticrowidnumber  string,
        state                           string,
        zipcode                         string,
        patientaddressidnumber          string,
        patientidnumber                 string,
        inactivatedate                  string,
        associationidnumber             string
        )
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
    STORED AS TEXTFILE
    LOCATION '{input_path}address/'
    ;

-- DROP TABLE IF EXISTS clinicpreference;
-- CREATE EXTERNAL TABLE clinicpreference (
--         analyticrowidnumber         string,
--         clinicorganizationidnumber  string,
--         associationidnumber         string,
--         labmethodalbumin            string,
--         serumalbuminlower           string,
--         hemodialysismethod          string,
--         peritonealdialysismethod    string,
--         pnameasure                  string,
--         bsamethod                   string,
--         creatineclearancemethod     string,
--         inactivatedate              string
--         )
--     ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
--     STORED AS TEXTFILE
--     LOCATION '{input_path}clinicpreference/'

DROP TABLE IF EXISTS dialysistraining;
CREATE EXTERNAL TABLE dialysistraining (
        analyticrowidnumber             string,
        clinicorganizationidnumber      string,
        patientdataanalyticrowidnumber  string,
        cklresultid                     string,
        patientidnumber                 string,
        analyticdos                     string,
        addeddate                       string,
        editdate                        string,
        trainingtype                    string,
        othertraining                   string,
        expectedselfcare                string,
        dialysisperiod                  string,
        dialysistrainingstart           string,
        dialysistrainingend             string,
        inactivatedate                  string
        )
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
    STORED AS TEXTFILE
    LOCATION '{input_path}dialysistraining/'
    ;

DROP TABLE IF EXISTS dialysistreatment;
CREATE EXTERNAL TABLE dialysistreatment (
        analyticrowidnumber                     string,
        clinicorganizationidnumber              string,
        patientdataanalyticrowidnumber          string,
        runidnumber                             string,
        patientidnumber                         string,
        analyticdos                             string,
        status_original                         string,
        patientstatus                           string,
        primarydialysissetting                  string,
        clinicidnumber                          string,
        locationid                              string,
        outpatientrun                           string,
        treatmentstart                          string,
        treatmentend                            string,
        averagebloodflowrate                    string,
        averagebloodflowrateuom                 string,
        patientvascularaccessidnumber           string,
        startsittingbp                          string,
        startsittingpulse                       string,
        endsittingbp                            string,
        endsittingpulse                         string,
        startstandingbp                         string,
        startstandingpulse                      string,
        endstandingbp                           string,
        endstandingpulse                        string,
        runhighbp                               string,
        runlowbp                                string,
        runactualdialysisrxidnumber             string,
        dryweight                               string,
        dryweightuom                            string,
        lastweight                              string,
        lastweightuom                           string,
        preweight                               string,
        preweightuom                            string,
        postweight                              string,
        postweightuom                           string,
        litersprocessed                         string,
        litersprocesseduom                      string,
        timedialyzed                            string,
        fluidremoved                            string,
        fluidremoveduom                         string,
        dialysatetemp                           string,
        dialysatetempuom                        string,
        patienttempstart                        string,
        patienttempstartuom                     string,
        patienttempend                          string,
        patienttempenduom                       string,
        patienttemphigh                         string,
        patienttemphighuom                      string,
        machineheaderid                         string,
        txsubtype                               string,
        posteddate                              string,
        source                                  string,
        latestart                               string,
        completetx                              string,
        disposition                             string,
        ambulatorystatus                        string,
        statustimetx                            string,
        startlyingbp                            string,
        startlyingpulse                         string,
        endlyingbp                              string,
        endlyingpulse                           string,
        shift                                   string,
        infectionpresent                        string,
        addeddate                               string,
        editdate                                string,
        inactivatedate                          string,
        statusisactive                          string,
        patientvascularaccesssecondaryidnumber  string,
        cfresultidnumber                        string,
        patientdialysisrxheaderidnumber         string,
        treatmenttypecategory                   string,
        treatmenttypesubtype                    string,
        isprimary                               string
        )
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
    STORED AS TEXTFILE
    LOCATION '{input_path}dialysistreatment/'
    ;

DROP TABLE IF EXISTS facilityadmitdischarge;
CREATE EXTERNAL TABLE facilityadmitdischarge (
        analyticrowidnumber             string,
        clinicorganizationidnumber      string,
        patientdataanalyticrowidnumber  string,
        patientfacilityidnumber         string,
        patientidnumber                 string,
        clinicidnumber                  string,
        admitdate                       string,
        admitreason                     string,
        dischargedate                   string,
        dischargereason                 string,
        addeddate                       string,
        editdate                        string,
        networkevent                    string,
        patientmasterscheduleheaderid   string,
        patientmasterscheduleid         string,
        involuntarydischargereason      string,
        transferdischargereason         string,
        transientreason                 string,
        inactivatedate                  string,
        analyticdos                     string
        )
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
    STORED AS TEXTFILE
    LOCATION '{input_path}facilityadmitdischarge/'
    ;

DROP TABLE IF EXISTS hospitalization;
CREATE EXTERNAL TABLE hospitalization (
        analyticrowidnumber             string,
        clinicorganizationidnumber      string,
        patientdataanalyticrowidnumber  string,
        patienthospitalizationidnumber  string,
        patientidnumber                 string,
        admittingphysicianidnumber      string,
        admissiondate                   string,
        dischargedate                   string,
        addeddate                       string,
        editdate                        string,
        hospconsult                     string,
        unstable                        string,
        staylength                      string,
        icd9                            string,
        patientdialyzed                 string,
        typevisit                       string,
        presumptivediagnosis            string,
        transplantreferral              string,
        inactivatedate                  string,
        icd10                           string,
        analyticdos                     string
        )
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
    STORED AS TEXTFILE
    LOCATION '{input_path}hospitalization/'
    ;

DROP TABLE IF EXISTS immunization;
CREATE EXTERNAL TABLE immunization (
        analyticrowidnumber             string,
        clinicorganizationidnumber      string,
        patientdataanalyticrowidnumber  string,
        patientimmunizationidnumber     string,
        patientidnumber                 string,
        analyticdos                     string,
        immunizationtype                string,
        isbooster                       string,
        notgiven                        string,
        onoffsite                       string,
        immunization                    string,
        immunizationdate                string,
        addeddate                       string,
        editdate                        string,
        administeredondialysis          string,
        justificationforadministered    string,
        scheduled                       string,
        refused                         string,
        runidnumber                     string,
        administrationcode              string,
        medicationcode                  string,
        physicianidnumber               string,
        status                          string,
        lotnumber                       string,
        expirationdate                  string,
        immunizationroute               string,
        inactivatedate                  string,
        treatmentidnumber               string,
        icd10                           string
        )
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
    STORED AS TEXTFILE
    LOCATION '{input_path}immunization/'
    ;

DROP TABLE IF EXISTS insurance;
CREATE EXTERNAL TABLE insurance (
        analyticrowidnumber             string,
        clinicorganizationidnumber      string,
        patientdataanalyticrowidnumber  string,
        insuranceidnumber               string,
        patientidnumber                 string,
        insurancecompanyidnumber        string,
        insurancecompanyname            string,
        insurancecompanyid              string,
        insurancecompanyaddressline1    string,
        insurancecompanyaddressline2    string,
        insurancecompanycity            string,
        insurancecompanystate           string,
        insurancecompanyzipcode         string,
        insurancecompanyprimaryphone    string,
        payorid                         string,
        planeffectivedate               string,
        planexpirationdate              string,
        authorizationnumber             string,
        plantype                        string,
        addeddate                       string,
        editdate                        string,
        insurednumberid                 string,
        medicareparta                   string,
        companyplantype                 string,
        inactivatedate                  string
        )
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
    STORED AS TEXTFILE
    LOCATION '{input_path}insurance/'
    ;

DROP TABLE IF EXISTS labidlist;
CREATE EXTERNAL TABLE labidlist (
        analyticrowidnumber    string,
        universalserviceid     string,
        observationidentifier  string,
        testname               string,
        cptcode                string,
        icd9diagnosiscode      string,
        cptinfo                string,
        icd9info               string,
        revised_info           string,
        differencees           string,
        inactivatedate         string
        )
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
    STORED AS TEXTFILE
    LOCATION '{input_path}labidlist/'
    ;

DROP TABLE IF EXISTS labpanelsdrawn;
CREATE EXTERNAL TABLE labpanelsdrawn (
        analyticrowidnumber             string,
        clinicorganizationidnumber      string,
        patientdataanalyticrowidnumber  string,
        labpanelsdrawnid                string,
        analyticdos                     string,
        labscheduleidnumber             string,
        patientidnumber                 string,
        status                          string,
        panelname                       string,
        justification                   string,
        addeddate                       string,
        editdate                        string,
        runidnumber                     string,
        postponedto                     string,
        thedate                         string,
        panelidnumber                   string,
        administrationcode              string,
        procedurecode                   string,
        medicationcode                  string,
        physicianidnumber               string,
        inactivatedate                  string,
        treatmentidnumber               string
        )
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
    STORED AS TEXTFILE
    LOCATION '{input_path}labpanelsdrawn/'
    ;

DROP TABLE IF EXISTS labresult;
CREATE EXTERNAL TABLE labresult (
        analyticrowidnumber             string,
        clinicorganizationidnumber      string,
        patientdataanalyticrowidnumber  string,
        analyticdos                     string,
        labresultidnumber               string,
        patientidnumber                 string,
        orderedby                       string,
        orderdate                       string,
        completeddate                   string,
        receiveddate                    string,
        testname                        string,
        testresult                      string,
        addeddate                       string,
        editdate                        string,
        calculated                      string,
        scaled                          string,
        textresult                      string,
        referencerange                  string,
        observationidentifier           string,
        universalserviceid              string,
        fmtresult                       string,
        abnormalflags                   string,
        diagnosiscode                   string,
        esrdrelated                     string,
        runidnumber                     string,
        medicationcode                  string,
        administrationcode              string,
        procedurecode                   string,
        inactivatedate                  string,
        icd10                           string
        )
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
    STORED AS TEXTFILE
    LOCATION '{input_path}labresult/'
    ;

-- DROP TABLE IF EXISTS medication;
-- CREATE EXTERNAL TABLE medication (
--         medidnumber     string,
--         addeddate       string,
--         lasteditdate    string,
--         inactivatedate  string
--         )
--     ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
--     STORED AS TEXTFILE
--     LOCATION '{input_path}medication/'

-- DROP TABLE IF EXISTS medicationgroup;
-- CREATE EXTERNAL TABLE medicationgroup (
--         medicationgroupidnumber         string,
--         medicationgroupname             string,
--         medicationname                  string,
--         crownmedicationname             string,
--         route                           string,
--         original_medgroupiddetailnumb   string,
--         original_medgroupidnumber       string,
--         addeddate                       string,
--         lasteditdate                    string,
--         inactivatedate                  string
--         )
--     ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
--     STORED AS TEXTFILE
--     LOCATION '{input_path}medicationgroup/'

DROP TABLE IF EXISTS modalitychangehistorycrownweb;
CREATE EXTERNAL TABLE modalitychangehistorycrownweb (
        analyticrowidnumber                             string,
        modalitychangehistoryanalyticrowidnumber        string,
        patientdialysisprescriptionanalyticrowidnumber  string,
        patientidnumber                                 string,
        patientdataanalyticrowidnumber                  string,
        defaultclinicorganizationidnumber               string,
        treatmentclinicorganizationidnumber             string,
        effectivereportingdate                          string,
        physiciananalyticrowidnumber                    string,
        treatmenttype                                   string,
        minutespersession                               string,
        sessionsperweek                                 string,
        crowndialysissetting                            string
        )
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
    STORED AS TEXTFILE
    LOCATION '{input_path}modalitychangehistorycrownweb/'
    ;

DROP TABLE IF EXISTS nursinghomehistory;
CREATE EXTERNAL TABLE nursinghomehistory (
        analyticrowidnumber             string,
        clinicorganizationidnumber      string,
        patientdataanalyticrowidnumber  string,
        patientnursinghomeidnumber      string,
        patientidnumber                 string,
        nursinghomeid                   string,
        startdate                       string,
        enddate                         string,
        addeddate                       string,
        editdate                        string,
        inactivatedate                  string,
        analyticdos                     string
        )
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
    STORED AS TEXTFILE
    LOCATION '{input_path}nursinghomehistory/'
    ;

DROP TABLE IF EXISTS patientaccess;
CREATE EXTERNAL TABLE patientaccess (
        analyticrowidnumber             string,
        patientdataanalyticrowidnumber  string,
        clinicorganizationidnumber      string,
        patientvascularaccessidnumber   string,
        patientidnumber                 string,
        vascularaccesstype              string,
        location                        string,
        currentstatus                   string,
        chroniccatheter                 string,
        analyticdos                     string,
        startdate                       string,
        enddate                         string,
        addeddate                       string,
        editdate                        string,
        placedrecorded                  string,
        isprimaryaccess                 string,
        isstateactive                   string,
        inactivatedate                  string
        )
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
    STORED AS TEXTFILE
    LOCATION '{input_path}patientaccess/'
    ;

DROP TABLE IF EXISTS patientaccess_examproc;
CREATE EXTERNAL TABLE patientaccess_examproc (
        analyticrowidnumber                  string,
        clinicorganizationidnumber           string,
        patientdataanalyticrowidnumber       string,
        patientaccessanalyticrowidnumber     string,
        resultidnumber                       string,
        patientidnumber                      string,
        analyticdos                          string,
        addeddate                            string,
        editdate                             string,
        examproceduredate                    string,
        examproceduretype                    string,
        examprocedurefrequency               string,
        performedbytype                      string,
        performedbyoutsidephysicianidnumber  string,
        performingfacilitytype               string,
        performingfacilityclinicidnumber     string,
        performingfacilitycontactid          string,
        isbillablebyclinic                   string,
        diagnosticcode                       string,
        procedurecode                        string,
        examprocedureisstateactive           string,
        examprocedurestatusafter             string,
        examprocedureisprimaryaccess         string,
        inactivatedate                       string,
        icd10                                string
        )
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
    STORED AS TEXTFILE
    LOCATION '{input_path}patientaccess_examproc/'
    ;

DROP TABLE IF EXISTS patientaccess_otheraccessevent;
CREATE EXTERNAL TABLE patientaccess_otheraccessevent (
        analyticrowidnumber                 string,
        clinicorganizationidnumber          string,
        patientdataanalyticrowidnumber      string,
        patientaccessanalyticrowidnumber    string,
        resultidnumber                      string,
        patientidnumber                     string,
        analyticdos                         string,
        addeddate                           string,
        editdate                            string,
        otheraccesseventdate                string,
        otheraccesseventisstateactive       string,
        otheraccesseventstatusafter         string,
        otheraccesseventisprimaryaccess     string,
        reasonforchange                     string,
        inactivatedate                      string
        )
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
    STORED AS TEXTFILE
    LOCATION '{input_path}patientaccess_otheraccessevent/'
    ;

DROP TABLE IF EXISTS patientaccess_placedrecorded;
CREATE EXTERNAL TABLE patientaccess_placedrecorded (
        analyticrowidnumber                 string,
        clinicorganizationidnumber          string,
        patientdataanalyticrowidnumber      string,
        patientaccessanalyticrowidnumber    string,
        resultidnumber                      string,
        patientidnumber                     string,
        analyticdos                         string,
        addeddate                           string,
        editdate                            string,
        placedrecorded                      string,
        placedrecordeddate                  string,
        accesstype                          string,
        accesslocation                      string,
        cathname                            string,
        ischroniccath                       string,
        placedbytype                        string,
        placedbyoutsidephysicianidnumber    string,
        placingfacilitytype                 string,
        placingfacilityclinicidnumber       string,
        placingfacilitycontactid            string,
        placedrecordedisstateactive         string,
        placedrecordedstatusafter           string,
        placedrecordedisprimaryaccess       string,
        inactivatedate                      string
        )
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
    STORED AS TEXTFILE
    LOCATION '{input_path}patientaccess_placedrecorded/'
    ;

DROP TABLE IF EXISTS patientaccess_removed;
CREATE EXTERNAL TABLE patientaccess_removed (
        analyticrowidnumber                 string,
        clinicorganizationidnumber          string,
        patientdataanalyticrowidnumber      string,
        patientaccessanalyticrowidnumber    string,
        resultidnumber                      string,
        patientidnumber                     string,
        analyticdos                         string,
        addeddate                           string,
        editdate                            string,
        removeddate                         string,
        removedbytype                       string,
        removedbyoutsidephysicianidnumber   string,
        removingfacilitytype                string,
        removingfacilityclinicidnumber      string,
        removingfacilitycontactid           string,
        removedreasontype                   string,
        inactivatedate                      string
        )
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
    STORED AS TEXTFILE
    LOCATION '{input_path}patientaccess_removed/'
    ;

DROP TABLE IF EXISTS patientallergy;
CREATE EXTERNAL TABLE patientallergy (
        analyticrowidnumber             string,
        clinicorganizationidnumber      string,
        patientdataanalyticrowidnumber  string,
        patientalergyidnumber           string,
        patientidnumber                 string,
        medidnumber                     string,
        medicationname                  string,
        typeofreaction                  string,
        startdate                       string,
        enddate                         string,
        drug_id                         string,
        category_concept_id             string,
        categoryname                    string,
        addeddate                       string,
        editdate                        string,
        inactivatedate                  string
        )
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
    STORED AS TEXTFILE
    LOCATION '{input_path}patientallergy/'
    ;

DROP TABLE IF EXISTS patientcms2728;
CREATE EXTERNAL TABLE patientcms2728 (
        analyticrowidnumber                 string,
        clinicorganizationidnumber          string,
        patientdataanalyticrowidnumber      string,
        analyticdos                         string,
        patientidnumber                     string,
        form2728type                        string,
        predryweight                        string,
        preheightfeet                       string,
        preheightinches                     string,
        applyingforesrdmedicare             string,
        medicalcoveragemedicaid             string,
        medicalcoveragedva                  string,
        medicalcoveragemedicare2728         string,
        medicalcoveragemedicareadvantage    string,
        medicalcoverageemployer             string,
        medicalcoverageother                string,
        medicalcoveragenone                 string,
        predialysisepoadministered          string,
        esrd_erythropoteintx                string,
        esrd_erythropoteinperiod            string,
        esrd_nephrologistcare               string,
        esrd_nephrologistperiod             string,
        esrd_kidneydietitiancare            string,
        esrd_kidneydietitianperiod          string,
        esrd_firstopdialysisaccesstype      string,
        esrd_firstopdialysisotheraccess     string,
        esrd_firstopdialysismaturingavf     string,
        esrd_firstopdialysismaturinggraft   string,
        prelab_albumin                      string,
        prelab_albumindate                  string,
        prelab_albuminll                    string,
        prelab_albuminlldate                string,
        prelab_bun                          string,
        prelab_bundate                      string,
        prelab_creatinine                   string,
        prelab_creatininedate               string,
        prelab_creatinineclearance          string,
        prelab_creatinineclearancedate      string,
        prelab_hba1c                        string,
        prelab_hba1cdate                    string,
        prelab_hematocrit                   string,
        prelab_hematocritdate               string,
        prelab_hemoglobin                   string,
        prelab_hemoglobindate               string,
        prelab_labmethodused                string,
        prelab_labmethoduseddate            string,
        prelab_lipidprofilehdl              string,
        prelab_lipidprofilehdldate          string,
        prelab_lipidprofileldl              string,
        prelab_lipidprofileldldate          string,
        prelab_lipidprofiletc               string,
        prelab_lipidprofiletcdate           string,
        prelab_lipidprofiletg               string,
        prelab_lipidprofiletgdate           string,
        prelab_ureaclearance                string,
        prelab_ureaclearancedate            string,
        patientgfrmethod                    string,
        comorbidreportdate                  string,
        supervisingphysicianidnumber        string,
        supervisingphysiciansignaturedate   string,
        willselfdialyze                     string,
        trainingphysicianidnumber           string,
        trainingphysiciansignaturedate      string,
        patientsignaturedate                string,
        addeddate                           string,
        editdate                            string,
        inactivatedate                      string
        )
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
    STORED AS TEXTFILE
    LOCATION '{input_path}patientcms2728/'
    ;

DROP TABLE IF EXISTS patientcomorbidityandtransplantstate;
CREATE EXTERNAL TABLE patientcomorbidityandtransplantstate (
        analyticrowidnumber                         string,
        clinicorganizationidnumber                  string,
        patientdataanalyticrowidnumber              string,
        patientidnumber                             string,
        causeofrenalfailure                         string,
        transplantfunctioningatdeath                string,
        transplantedkidney                          string,
        transplantdate                              string,
        transplanthospital                          string,
        medicareprovidernumberoftransplanthospital  string,
        medicareprovidernumberoftransplanthospital2 string,
        medicareprovidernumberoftransplanthospital3 string,
        prephospital                                string,
        prephospitalmedicareprovidernumber          string,
        prephospitalenterdate                       string,
        currentstateoftransplant                    string,
        transplantcandidate                         string,
        transopmedicallyunfit                       string,
        transopnotassessed                          string,
        transopother                                string,
        transoppatientdeclines                      string,
        transoppatientinformed                      string,
        transoppsych                                string,
        transopunsuitableage                        string,
        transplanthospital1note                     string,
        transplanthospital2                         string,
        transplanthospital2note                     string,
        transplanthospital3note                     string,
        transplantwaitlist                          string,
        editdatetransplant                          string,
        inactivatedate                              string
        )
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
    STORED AS TEXTFILE
    LOCATION '{input_path}patientcomorbidityandtransplantstate/'
    ;

DROP TABLE IF EXISTS patientdata;
CREATE EXTERNAL TABLE patientdata (
        analyticrowidnumber                     string,
        clinicorganizationidnumber              string,
        defaultclinicidnumber                   string,
        patientidnumber                         string,
        patientid                               string,
        medicalrecordnumber                     string,
        labidnumber                             string,
        state                                   string,
        zipcode                                 string,
        sex                                     string,
        status_original                         string,
        patientstatus                           string,
        statusisactive                          string,
        primarymodality                         string,
        primarymodality_original                string,
        primarydialysissetting                  string,
        datefirstdialysis                       string,
        laststatuschangedate                    string,
        resuscitationcode                       string,
        advdirresuscitationcode                 string,
        advdirpowerofatty                       string,
        advdirlivingwill                        string,
        advdirpatientdeclines                   string,
        advrevdate                              string,
        tribecode                               string,
        inactivatedate                          string,
        age                                     string,
        monthsindialysis                        string,
        transplantwaitlist                      string,
        medicalcoveragemedicare                 string,
        medicalcoveragemedicareeffectivedate    string,
        masterpatientidnumber                   string,
        datefirstdialysiscurrentunit            string
        )
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
    STORED AS TEXTFILE
    LOCATION '{input_path}patientdata/'
    ;

DROP TABLE IF EXISTS patientdiagcodes;
CREATE EXTERNAL TABLE patientdiagcodes (
        analyticrowidnumber             string,
        clinicorganizationidnumber      string,
        patientdataanalyticrowidnumber  string,
        patientdiagcodeid               string,
        patientidnumber                 string,
        diagnosiscode                   string,
        description                     string,
        diagnosisdate                   string,
        diagnosistype                   string,
        diagnosispriority               string,
        admitting                       string,
        addeddate                       string,
        editdate                        string,
        diagnosisenddate                string,
        orderedbyidnumber               string,
        inactivatedate                  string,
        icd10                           string,
        analyticdos                     string
        )
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
    STORED AS TEXTFILE
    LOCATION '{input_path}patientdiagcodes/'
    ;

DROP TABLE IF EXISTS patientdialysisprescription;
CREATE EXTERNAL TABLE patientdialysisprescription (
        analyticrowidnumber                 string,
        clinicorganizationidnumber          string,
        patientdataanalyticrowidnumber      string,
        patientidnumber                     string,
        startdate                           string,
        enddate                             string,
        modalitytype                        string,
        modality                            string,
        primarydialysissetting              string,
        primarydialysisperiod               string,
        drygoalweight                       string,
        drygoalweightuom                    string,
        tbd                                 string,
        addeddate                           string,
        editdate                            string,
        physicianidnumber                   string,
        patientdialysisheaderidnumber       string,
        patientdialysisrxnumber             string,
        patientdialysisrxinpatientnumber    string,
        patientrxrxnxstageidnumber          string,
        patientrxrxotheridnumber            string,
        patientrxrxcapdidnumber             string,
        patientrxrxregularccpdidnumber      string,
        patientrxrxhighdoseccpdidnumber     string,
        patientrxrxtidalidnumber            string,
        patientrxrxhighdosetidalidnumber    string,
        prescriptiontype                    string,
        inactivatedate                      string,
        analyticdos                         string
        )
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
    STORED AS TEXTFILE
    LOCATION '{input_path}patientdialysisprescription/'
    ;

DROP TABLE IF EXISTS patientdialysisrxhemo;
CREATE EXTERNAL TABLE patientdialysisrxhemo (
        analyticrowidnumber                             string,
        clinicorganizationidnumber                      string,
        patientdataanalyticrowidnumber                  string,
        patientdialysisprescriptionanalyticrowidnumber  string,
        patientdialysisheaderidnumber                   string,
        treatmenttime                                   string,
        treatmenttimeuom                                string,
        membranetype                                    string,
        bloodflowrate                                   string,
        arterialneedlesize                              string,
        venousneedlesize                                string,
        dialysateflowrate                               string,
        dialysisbathcalciumlevel                        string,
        dialysisbathpotasiumlevel                       string,
        dialysisbathsodiumleveltype                     string,
        sodiumbegin                                     string,
        sodiumend                                       string,
        sodiumstepinstructions                          string,
        sodiumprofilenumber                             string,
        totalheparingiven                               string,
        heparininstructions                             string,
        bicarbonatelevel                                string,
        ultrafiltleveltype                              string,
        ultrafiltbeginlevel                             string,
        ultrafiltendlevel                               string,
        ultrafiltlstepinstructions                      string,
        ultrafiltprofilenumber                          string,
        litersprocessed                                 string,
        totalbodywater                                  string,
        dialysatetemp                                   string,
        maxfluidremoved                                 string,
        treatmentsetuptime                              string,
        treatmentcleanuptime                            string,
        weightmin                                       string,
        weightmax                                       string,
        totalvolumedialysate                            string,
        filtration                                      string,
        otherfiltration                                 string,
        bloodflowratemin                                string,
        bloodflowratemax                                string,
        lactate                                         string,
        otherlactate                                    string,
        ufrate                                          string,
        dialysisfrequencydesc                           string,
        dialysisfrequencyvalue                          string,
        inactivatedate                                  string
        )
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
    STORED AS TEXTFILE
    LOCATION '{input_path}patientdialysisrxhemo/'
    ;

DROP TABLE IF EXISTS patientdialysisrxpd;
CREATE EXTERNAL TABLE patientdialysisrxpd (
        analyticrowidnumber                             string,
        clinicorganizationidnumber                      string,
        patientdataanalyticrowidnumber                  string,
        patientdialysisprescriptionanalyticrowidnumber  string,
        patientdialysisheaderidnumber                   string,
        therapytime                                     string,
        numberofexchanges                               string,
        totalvolumeexchangesolution                     string,
        fulldrainevery                                  string,
        numberofcycles                                  string,
        cyclertype                                      string,
        tidalpercent                                    string,
        ufpercycle                                      string,
        totaluf                                         string,
        changesolutionprotocol                          string,
        totalvolumecyclersolution                       string,
        finalfillvolume                                 string,
        finalfillexchangeidnumber                       string,
        inactivatedate                                  string
        )
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
    STORED AS TEXTFILE
    LOCATION '{input_path}patientdialysisrxpd/'
    ;

DROP TABLE IF EXISTS patientdialysisrxpdexchanges;
CREATE EXTERNAL TABLE patientdialysisrxpdexchanges (
        analyticrowidnumber                             string,
        clinicorganizationidnumber                      string,
        patientdataanalyticrowidnumber                  string,
        patientdialysisprescriptionanalyticrowidnumber  string,
        patientdialysispdexchangesidnumber              string,
        patientdialysisheaderidnumber                   string,
        exchangenumber                                  string,
        exchangedialysisperiod                          string,
        dialysisexchangemechanism                       string,
        exchangetime                                    string,
        exchangesolution                                string,
        lowcalcium                                      string,
        exchangevolume                                  string,
        fillvolume                                      string,
        dwelltime                                       string,
        draintime                                       string,
        addeddate                                       string,
        editdate                                        string,
        solutiontype                                    string,
        inactivatedate                                  string
        )
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
    STORED AS TEXTFILE
    LOCATION '{input_path}patientdialysisrxpdexchanges/'
    ;

DROP TABLE IF EXISTS patientevent;
CREATE EXTERNAL TABLE patientevent (
        analyticrowidnumber             string,
        clinicorganizationidnumber      string,
        patientdataanalyticrowidnumber  string,
        patienteventidnumber            string,
        patientidnumber                 string,
        eventdate                       string,
        eventtype                       string,
        addeddate                       string,
        editdate                        string,
        deleteddate                     string,
        resultidnumber                  string,
        patientvascularaccessidnumber   string,
        patientinfectionidnumber        string,
        inactivatedate                  string,
        analyticdos                     string
        )
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
    STORED AS TEXTFILE
    LOCATION '{input_path}patientevent/'
    ;

DROP TABLE IF EXISTS patientfluidweightmanagement;
CREATE EXTERNAL TABLE patientfluidweightmanagement (
        analyticrowidnumber                 string,
        clinicorganizationidnumber          string,
        patientdataanalyticrowidnumber      string,
        cklresultid                         string,
        patientidnumber                     string,
        analyticdos                         string,
        sodiumeducationdate                 string,
        addeddate                           string,
        editdate                            string,
        sodiumeducationreceived             string,
        sodiumprofilingrxed                 string,
        constantdialysatesodiumrxed         string,
        dialysatesodiumover138              string,
        postdialysiswgtassessmentdate       string,
        postdialysistargetwgtrxed           string,
        homebpprovided                      string,
        homebpstatus                        string,
        dryweightrxed                       string,
        hasedema                            string,
        echocardiogramdate                  string,
        hasabnormalbreathsounds             string,
        hasleftventricularhypertrophy       string,
        leftventricularhypertrophychanged   string,
        inactivatedate                      string
        )
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
    STORED AS TEXTFILE
    LOCATION '{input_path}patientfluidweightmanagement/'
    ;

DROP TABLE IF EXISTS patientheighthistory;
CREATE EXTERNAL TABLE patientheighthistory (
        analyticrowidnumber             string,
        clinicorganizationidnumber      string,
        patientdataanalyticrowidnumber  string,
        analyticdos                     string,
        patientheighthistoryidnumber    string,
        heightfeet                      string,
        heightinches                    string,
        totalheight                     string,
        totalheightuom                  string,
        patientidnumber                 string,
        doubleamputee                   string,
        addeddate                       string,
        editdate                        string,
        measuredate                     string,
        unstable                        string,
        inactivatedate                  string
        )
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
    STORED AS TEXTFILE
    LOCATION '{input_path}patientheighthistory/'
    ;

DROP TABLE IF EXISTS patientinfection;
CREATE EXTERNAL TABLE patientinfection (
        analyticrowidnumber                 string,
        clinicorganizationidnumber          string,
        patientdataanalyticrowidnumber      string,
        patientinfectionidnumber            string,
        patientidnumber                     string,
        suspectedinfectionstartdate         string,
        confirmationdate                    string,
        infectionsourcetype                 string,
        patientvascularaccessidnumber       string,
        primarylocationother                string,
        identificationtype                  string,
        symptom_none                        string,
        symptom_accesssite                  string,
        symptom_fever_37_8c                 string,
        symptom_chills                      string,
        symptom_bp                          string,
        symptom_mental                      string,
        symptom_wound                       string,
        symptom_cellulitis                  string,
        symptom_respiratory                 string,
        enddate                             string,
        infectionrequiredhospitalization    string,
        hospitalizationdate                 string,
        deathrelatedtoinfection             string,
        pdeffluentcellcountsdifferential    string,
        addeddate                           string,
        editdate                            string,
        inactivatedate                      string,
        analyticdos                         string,
        lossofvascularaccess                string,
        accesssitenhsn_fistula              string,
        accesssitenhsn_graft                string,
        accesssitenhsn_tunncentralline      string,
        accesssitenhsn_nontunncentralline   string,
        accesssitenhsn_otheraccessdevice    string
        )
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
    STORED AS TEXTFILE
    LOCATION '{input_path}patientinfection/'
    ;

DROP TABLE IF EXISTS patientinfection_laborganism;
CREATE EXTERNAL TABLE patientinfection_laborganism (
        analyticrowidnumber                         string,
        clinicorganizationidnumber                  string,
        patientdataanalyticrowidnumber              string,
        analyticdos                                 string,
        patientidnumber                             string,
        patientinfection_laborganismidnumber        string,
        patientinfection_labresultcultureidnumber   string,
        organismcode                                string,
        organismrank                                string,
        addeddate                                   string,
        editdate                                    string,
        inactivatedate                              string
        )
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
    STORED AS TEXTFILE
    LOCATION '{input_path}patientinfection_laborganism/'
    ;

DROP TABLE IF EXISTS patientinfection_laborganismdrug;
CREATE EXTERNAL TABLE patientinfection_laborganismdrug (
        analyticrowidnumber                         string,
        clinicorganizationidnumber                  string,
        patientdataanalyticrowidnumber              string,
        analyticdos                                 string,
        patientidnumber                             string,
        patientinfection_laborganismdrugidnumber    string,
        patientinfection_laborganismidnumber        string,
        drugcode                                    string,
        susceptibilitycode                          string,
        addeddate                                   string,
        editdate                                    string,
        inactivatedate                              string
        )
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
    STORED AS TEXTFILE
    LOCATION '{input_path}patientinfection_laborganismdrug/'
    ;

DROP TABLE IF EXISTS patientinfection_labresultculture;
CREATE EXTERNAL TABLE patientinfection_labresultculture (
        analyticrowidnumber                         string,
        clinicorganizationidnumber                  string,
        patientdataanalyticrowidnumber              string,
        patientinfection_labresultcultureidnumber   string,
        patientinfectionidnumber                    string,
        patientidnumber                             string,
        resulteddate                                string,
        resulttype                                  string,
        collecteddate                               string,
        culturesource                               string,
        cultureresult                               string,
        organisms                                   string,
        addeddate                                   string,
        editdate                                    string,
        inactivatedate                              string,
        analyticdos                                 string
        )
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
    STORED AS TEXTFILE
    LOCATION '{input_path}patientinfection_labresultculture/'
    ;

DROP TABLE IF EXISTS patientinfection_medication;
CREATE EXTERNAL TABLE patientinfection_medication (
        analyticrowidnumber                     string,
        clinicorganizationidnumber              string,
        patientdataanalyticrowidnumber          string,
        patientinfection_medicationidnumber     string,
        patientinfectionidnumber                string,
        patientprescriptionmedsidnumber         string,
        patientidnumber                         string,
        addeddate                               string,
        editdate                                string,
        inactivatedate                          string
        )
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
    STORED AS TEXTFILE
    LOCATION '{input_path}patientinfection_medication/'
    ;

DROP TABLE IF EXISTS patientinstabilityhistory;
CREATE EXTERNAL TABLE patientinstabilityhistory (
        analyticrowidnumber                 string,
        clinicorganizationidnumber          string,
        patientdataanalyticrowidnumber      string,
        patientinstabilityhistoryidnumber   string,
        patientidnumber                     string,
        eventdate                           string,
        event                               string,
        eventnotes                          string,
        dateofcia                           string,
        cklresultid                         string,
        relationid                          string,
        relationtype                        string,
        addeddate                           string,
        editdate                            string,
        deleted                             string,
        deleteddate                         string,
        inactivatedate                      string
        )
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
    STORED AS TEXTFILE
    LOCATION '{input_path}patientinstabilityhistory/'
    ;

DROP TABLE IF EXISTS patientmasterscheduleheader;
CREATE EXTERNAL TABLE patientmasterscheduleheader (
        analyticrowidnumber             string,
        patientdataanalyticrowidnumber  string,
        clinicorganizationidnumber      string,
        patientmasterscheduleheaderid   string,
        patientidnumber                 string,
        patientidnumber2                string,
        clinicid                        string,
        scheduletype                    string,
        startdate                       string,
        enddate                         string,
        starttime                       string,
        endtime                         string,
        scheduleshift                   string,
        patientstatus                   string,
        patientstatus2                  string,
        patientstatus_original          string,
        statusisactive                  string,
        txtypeidnumber                  string,
        reoccurrencetype                string,
        recurevery                      string,
        mon                             string,
        tue                             string,
        wed                             string,
        thu                             string,
        fri                             string,
        sat                             string,
        sun                             string,
        disabled                        string,
        addeddate                       string,
        editdate                        string,
        clinicscheduleid                string,
        originaldate                    string,
        reasontransferred               string,
        referringphysician              string,
        networkevent                    string,
        inactivatedate                  string,
        sessionsperweek                 string
        )
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
    STORED AS TEXTFILE
    LOCATION '{input_path}patientmasterscheduleheader/'
    ;

DROP TABLE IF EXISTS patientmedadministered;
CREATE EXTERNAL TABLE patientmedadministered (
        analyticrowidnumber              string,
        patientdataanalyticrowidnumber   string,
        clinicorganizationidnumber       string,
        patientadministeredmedsidnumber  string,
        patientidnumber                  string,
        analyticdos                      string,
        runidnumber                      string,
        medidnumber                      string,
        medication                       string,
        route                            string,
        dose                             string,
        frequency                        string,
        duration                         string,
        prescription                     string,
        startdate                        string,
        enddate                          string,
        prn                              string,
        adminduringrun                   string,
        addeddate                        string,
        editdate                         string,
        administrationtime               string,
        runjustification                 string,
        medprescidnumber                 string,
        totaldoses                       string,
        dosesgiven                       string,
        usedoses                         string,
        doseqty                          string,
        doseunit                         string,
        doseroute                        string,
        dosefreq                         string,
        dosefreqmonday                   string,
        dosefreqtuesday                  string,
        dosefreqwednesday                string,
        dosefreqthursday                 string,
        dosefreqfriday                   string,
        dosefreqsaturday                 string,
        dosefreqsunday                   string,
        hold                             string,
        holduntil                        string,
        lastdoseon                       string,
        patientnottaking                 string,
        physicianidnumber                string,
        fixedweekinterval                string,
        datenextdose                     string,
        datedoselastgiven                string,
        administrationcode               string,
        procedurecode                    string,
        medicationcode                   string,
        patientprovided                  string,
        esrdrelated                      string,
        inactivatedate                   string,
        selfadmin                        string,
        adminduringfacility              string,
        bulksupply                       string,
        treatmentidnumber                string,
        icd10                            string,
        genproduct_id                    string,
        drug_id                          string
        )
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
    STORED AS TEXTFILE
    LOCATION '{input_path}patientmedadministered/'
    ;

DROP TABLE IF EXISTS patientmednotgiven;
CREATE EXTERNAL TABLE patientmednotgiven (
        analyticrowidnumber                     string,
        patientdataanalyticrowidnumber          string,
        clinicorganizationidnumber              string,
        runmedsactualadministereddeleteidnumber string,
        patientadministeredmedsidnumber         string,
        patientidnumber                         string,
        analyticdos                             string,
        runidnumber                             string,
        medidnumber                             string,
        medication                              string,
        route                                   string,
        dose                                    string,
        frequency                               string,
        duration                                string,
        prescription                            string,
        startdate                               string,
        enddate                                 string,
        prn                                     string,
        adminduringrun                          string,
        addeddate                               string,
        editdate                                string,
        administrationtime                      string,
        runjustification                        string,
        medprescidnumber                        string,
        totaldoses                              string,
        dosesgiven                              string,
        usedoses                                string,
        doseqty                                 string,
        doseunit                                string,
        doseroute                               string,
        dosefreq                                string,
        dosefreqmonday                          string,
        dosefreqtuesday                         string,
        dosefreqwednesday                       string,
        dosefreqthursday                        string,
        dosefreqfriday                          string,
        dosefreqsaturday                        string,
        dosefreqsunday                          string,
        hold                                    string,
        holduntil                               string,
        lastdoseon                              string,
        patientnottaking                        string,
        physicianidnumber                       string,
        fixedweekinterval                       string,
        datenextdose                            string,
        datedoselastgiven                       string,
        administrationcode                      string,
        procedurecode                           string,
        medicationcode                          string,
        patientprovided                         string,
        esrdrelated                             string,
        patientprescriptionmedsparentid         string,
        inactivatedate                          string,
        selfadmin                               string,
        adminduringfacility                     string,
        bulksupply                              string,
        treatmentidnumber                       string,
        icd10                                   string,
        genproduct_id                           string,
        drug_id                                 string
        )
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
    STORED AS TEXTFILE
    LOCATION '{input_path}patientmednotgiven/'
    ;

DROP TABLE IF EXISTS patientmedprescription;
CREATE EXTERNAL TABLE patientmedprescription (
        analyticrowidnumber              string,
        patientdataanalyticrowidnumber   string,
        clinicorganizationidnumber       string,
        patientadministeredmedsidnumber  string,
        patientidnumber                  string,
        medidnumber                      string,
        medication                       string,
        prescription                     string,
        hold                             string,
        holduntil                        string,
        startdate                        string,
        enddate                          string,
        prn                              string,
        adminduringrun                   string,
        addeddate                        string,
        editdate                         string,
        lastdoseon                       string,
        patientnottaking                 string,
        runjustification                 string,
        physicianidnumber                string,
        totaldoses                       string,
        dosesgiven                       string,
        usedoses                         string,
        doseqty                          string,
        doseunit                         string,
        doseroute                        string,
        dosefreq                         string,
        dosefreqmonday                   string,
        dosefreqtuesday                  string,
        dosefreqwednesday                string,
        dosefreqthursday                 string,
        dosefreqfriday                   string,
        dosefreqsaturday                 string,
        dosefreqsunday                   string,
        fixedweekinterval                string,
        datenextdose                     string,
        datedoselastgiven                string,
        administrationcode               string,
        patientprovided                  string,
        patientprescriptionmedsparentid  string,
        protocolmed                      string,
        patientmasterscheduleheaderid    string,
        updatereason                     string,
        esrdrelated                      string,
        analyticdos                      string,
        inactivatedate                   string,
        monthlydose                      string,
        selfadmin                        string,
        adminduringfacility              string,
        bulksupply                       string,
        icd10                            string,
        genproduct_id                    string,
        drug_id                          string,
        donotsubstitute                  string,
        startingdosesgiven               string,
        dosestrength                     string,
        doseform                         string,
        eprescribed                      string,
        eprescribedquantity              string,
        eprescribedrefill                string,
        eprescribeddate                  string
        )
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
    STORED AS TEXTFILE
    LOCATION '{input_path}patientmedprescription/'
    ;

DROP TABLE IF EXISTS patientstatushistory;
CREATE EXTERNAL TABLE patientstatushistory (
        analyticrowidnumber             string,
        clinicorganizationidnumber      string,
        patientdataanalyticrowidnumber  string,
        patientstatushistoryidnumber    string,
        patientidnumber                 string,
        status_original                 string,
        patientstatus                   string,
        statusisactive                  string,
        datelaststatuschange            string,
        addeddate                       string,
        editdate                        string,
        editdatehistory                 string,
        inactivatedate                  string,
        analyticdos                     string
        )
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
    STORED AS TEXTFILE
    LOCATION '{input_path}patientstatushistory/'
    ;

DROP TABLE IF EXISTS problemlist;
CREATE EXTERNAL TABLE problemlist (
        analyticrowidnumber             string,
        patientdataanalyticrowidnumber  string,
        clinicorganizationidnumber      string,
        problemlistidnumber             string,
        patientidnumber                 string,
        analyticdos                     string,
        startdate                       string,
        enddate                         string,
        active                          string,
        category                        string,
        displayonreport                 string,
        icd9                            string,
        icd9text                        string,
        icd10                           string,
        icd10text                       string,
        patientcms2728idnumber          string,
        cms2728code                     string,
        addeddate                       string,
        editdate                        string,
        inactivatedate                  string
        )
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
    STORED AS TEXTFILE
    LOCATION '{input_path}problemlist/'
    ;

-- DROP TABLE IF EXISTS sodiumufprofile;
-- CREATE EXTERNAL TABLE sodiumufprofile (
--         analyticrowidnumber string,
--         profileidnumber     string,
--         profilenumber       string,
--         profiletype         string,
--         begininglevel       string,
--         endinglevel         string,
--         addeddate           string,
--         editdate            string,
--         inactivatedate      string
--         )
--     ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
--     STORED AS TEXTFILE
--     LOCATION '{input_path}sodiumufprofile/'

-- DROP TABLE IF EXISTS stategeo;
-- CREATE EXTERNAL TABLE stategeo (
--         stateabbreviation           string,
--         statename                   string,
--         statefipscode               string,
--         divisionname                string,
--         censusbureaudivisionnumber  string,
--         regionname                  string,
--         censusbureauregionnumber    string
--         )
--     ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
--     STORED AS TEXTFILE
--     LOCATION '{input_path}stategeo/'

-- DROP TABLE IF EXISTS zipgeo;
-- CREATE EXTERNAL TABLE zipgeo (
--         geo_id              string,
--         zipcode             string,
--         sumlevel            string,
--         geo_name            string,
--         totalpopulation     string,
--         urbanpopulation     string,
--         insideurbanarea     string,
--         insideurbancluster  string,
--         ruralpopulation     string,
--         na                  string,
--         ruralvsurban        string
--         )
--     ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
--     STORED AS TEXTFILE
--     LOCATION '{input_path}zipgeo/'
