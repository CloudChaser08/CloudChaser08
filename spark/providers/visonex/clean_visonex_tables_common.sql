DROP TABLE IF EXISTS clean_address;
CREATE TABLE clean_address (
        analyticrowidnumber             string,
        clinicorganizationidnumber      string,
        patientdataanalyticrowidnumber  string,
        state                           string,
        zipcode                         string,
        patientaddressidnumber          string,
        patientidnumber                 string,
        inactivatedate                  date,
        associationidnumber             string
        )
    ;

DROP TABLE IF EXISTS clean_clinicpreference;
CREATE TABLE clean_clinicpreference (
        analyticrowidnumber         string,
        clinicorganizationidnumber  string,
        associationidnumber         string,
        labmethodalbumin            string,
        serumalbuminlower           string,
        hemodialysismethod          string,
        peritonealdialysismethod    string,
        pnameasure                  string,
        bsamethod                   string,
        creatineclearancemethod     string,
        inactivatedate              date
        )
    ;

DROP TABLE IF EXISTS clean_dialysistraining;
CREATE TABLE clean_dialysistraining (
        analyticrowidnumber             string,
        clinicorganizationidnumber      string,
        patientdataanalyticrowidnumber  string,
        cklresultid                     string,
        patientidnumber                 string,
        analyticdos                     string,
        addeddate                       date,
        editdate                        date,
        trainingtype                    string,
        othertraining                   string,
        expectedselfcare                string,
        dialysisperiod                  string,
        dialysistrainingstart           string,
        dialysistrainingend             string,
        inactivatedate                  date
        )
    ;

DROP TABLE IF EXISTS clean_dialysistreatment;
CREATE TABLE clean_dialysistreatment (
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
        posteddate                              date,
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
        addeddate                               date,
        editdate                                date,
        inactivatedate                          date,
        statusisactive                          string,
        patientvascularaccesssecondaryidnumber  string,
        cfresultidnumber                        string,
        patientdialysisrxheaderidnumber         string,
        treatmenttypecategory                   string,
        treatmenttypesubtype                    string,
        isprimary                               string
        )
    ;

DROP TABLE IF EXISTS clean_facilityadmitdischarge;
CREATE TABLE clean_facilityadmitdischarge (
        analyticrowidnumber             string,
        clinicorganizationidnumber      string,
        patientdataanalyticrowidnumber  string,
        patientfacilityidnumber         string,
        patientidnumber                 string,
        clinicidnumber                  string,
        admitdate                       date,
        admitreason                     string,
        dischargedate                   date,
        dischargereason                 string,
        addeddate                       date,
        editdate                        date,
        networkevent                    string,
        patientmasterscheduleheaderid   string,
        patientmasterscheduleid         string,
        involuntarydischargereason      string,
        transferdischargereason         string,
        transientreason                 string,
        inactivatedate                  date,
        analyticdos                     string
        )
    ;

DROP TABLE IF EXISTS clean_hospitalization;
CREATE TABLE clean_hospitalization (
        analyticrowidnumber             string,
        clinicorganizationidnumber      string,
        patientdataanalyticrowidnumber  string,
        patienthospitalizationidnumber  string,
        patientidnumber                 string,
        admittingphysicianidnumber      string,
        admissiondate                   date,
        dischargedate                   date,
        addeddate                       date,
        editdate                        date,
        hospconsult                     string,
        unstable                        string,
        staylength                      string,
        icd9                            string,
        patientdialyzed                 string,
        typevisit                       string,
        presumptivediagnosis            string,
        transplantreferral              string,
        inactivatedate                  date,
        icd10                           string,
        analyticdos                     string
        )
    ;

DROP TABLE IF EXISTS clean_immunization;
CREATE TABLE clean_immunization (
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
        immunizationdate                date,
        addeddate                       date,
        editdate                        date,
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
        expirationdate                  date,
        immunizationroute               string,
        inactivatedate                  date,
        treatmentidnumber               string,
        icd10                           string
        )
    ;

DROP TABLE IF EXISTS clean_insurance;
CREATE TABLE clean_insurance (
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
        planeffectivedate               date,
        planexpirationdate              date,
        authorizationnumber             string,
        plantype                        string,
        addeddate                       date,
        editdate                        date,
        insurednumberid                 string,
        medicareparta                   string,
        companyplantype                 string,
        inactivatedate                  date
        )
    ;

DROP TABLE IF EXISTS clean_labidlist;
CREATE TABLE clean_labidlist (
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
        inactivatedate         date
        )
    ;

DROP TABLE IF EXISTS clean_labpanelsdrawn;
CREATE TABLE clean_labpanelsdrawn (
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
        addeddate                       date,
        editdate                        date,
        runidnumber                     string,
        postponedto                     string,
        thedate                         date,
        panelidnumber                   string,
        administrationcode              string,
        procedurecode                   string,
        medicationcode                  string,
        physicianidnumber               string,
        inactivatedate                  date,
        treatmentidnumber               string
        )
    ;

DROP TABLE IF EXISTS clean_labresult;
CREATE TABLE clean_labresult (
        analyticrowidnumber             string,
        clinicorganizationidnumber      string,
        patientdataanalyticrowidnumber  string,
        analyticdos                     string,
        labresultidnumber               string,
        patientidnumber                 string,
        orderedby                       string,
        orderdate                       date,
        completeddate                   date,
        receiveddate                    date,
        testname                        string,
        testresult                      string,
        addeddate                       date,
        editdate                        date,
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
        inactivatedate                  date,
        icd10                           string
        )
    ;

DROP TABLE IF EXISTS clean_medication;
CREATE TABLE clean_medication (
        medidnumber     string,
        addeddate       date,
        lasteditdate    date,
        inactivatedate  date
        )
    ;

DROP TABLE IF EXISTS clean_medicationgroup;
CREATE TABLE clean_medicationgroup (
        medicationgroupidnumber         string,
        medicationgroupname             string,
        medicationname                  string,
        crownmedicationname             string,
        route                           string,
        original_medgroupiddetailnumb   string,
        original_medgroupidnumber       string,
        addeddate                       date,
        lasteditdate                    date,
        inactivatedate                  date
        )
    ;

DROP TABLE IF EXISTS clean_modalitychangehistorycrownweb;
CREATE TABLE clean_modalitychangehistorycrownweb (
        analyticrowidnumber                             string,
        modalitychangehistoryanalyticrowidnumber        string,
        patientdialysisprescriptionanalyticrowidnumber  string,
        patientidnumber                                 string,
        patientdataanalyticrowidnumber                  string,
        defaultclinicorganizationidnumber               string,
        treatmentclinicorganizationidnumber             string,
        effectivereportingdate                          date,
        physiciananalyticrowidnumber                    string,
        treatmenttype                                   string,
        minutespersession                               string,
        sessionsperweek                                 string,
        crowndialysissetting                            string
        )
    ;

DROP TABLE IF EXISTS clean_nursinghomehistory;
CREATE TABLE clean_nursinghomehistory (
        analyticrowidnumber             string,
        clinicorganizationidnumber      string,
        patientdataanalyticrowidnumber  string,
        patientnursinghomeidnumber      string,
        patientidnumber                 string,
        nursinghomeid                   string,
        startdate                       date,
        enddate                         date,
        addeddate                       date,
        editdate                        date,
        inactivatedate                  date,
        analyticdos                     string
        )
    ;

DROP TABLE IF EXISTS clean_patientaccess;
CREATE TABLE clean_patientaccess (
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
        startdate                       date,
        enddate                         date,
        addeddate                       date,
        editdate                        date,
        placedrecorded                  string,
        isprimaryaccess                 string,
        isstateactive                   string,
        inactivatedate                  date
        )
    ;

DROP TABLE IF EXISTS clean_patientaccess_examproc;
CREATE TABLE clean_patientaccess_examproc (
        analyticrowidnumber                  string,
        clinicorganizationidnumber           string,
        patientdataanalyticrowidnumber       string,
        patientaccessanalyticrowidnumber     string,
        resultidnumber                       string,
        patientidnumber                      string,
        analyticdos                          string,
        addeddate                            date,
        editdate                             date,
        examproceduredate                    date,
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
        inactivatedate                       date,
        icd10                                string
        )
    ;

DROP TABLE IF EXISTS clean_patientaccess_otheraccessevent;
CREATE TABLE clean_patientaccess_otheraccessevent (
        analyticrowidnumber                 string,
        clinicorganizationidnumber          string,
        patientdataanalyticrowidnumber      string,
        patientaccessanalyticrowidnumber    string,
        resultidnumber                      string,
        patientidnumber                     string,
        analyticdos                         string,
        addeddate                           date,
        editdate                            date,
        otheraccesseventdate                date,
        otheraccesseventisstateactive       string,
        otheraccesseventstatusafter         string,
        otheraccesseventisprimaryaccess     string,
        reasonforchange                     string,
        inactivatedate                      date
        )
    ;

DROP TABLE IF EXISTS clean_patientaccess_placedrecorded;
CREATE TABLE clean_patientaccess_placedrecorded (
        analyticrowidnumber                 string,
        clinicorganizationidnumber          string,
        patientdataanalyticrowidnumber      string,
        patientaccessanalyticrowidnumber    string,
        resultidnumber                      string,
        patientidnumber                     string,
        analyticdos                         string,
        addeddate                           date,
        editdate                            date,
        placedrecorded                      string,
        placedrecordeddate                  date,
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
        inactivatedate                      date
        )
    ;

DROP TABLE IF EXISTS clean_patientaccess_removed;
CREATE TABLE clean_patientaccess_removed (
        analyticrowidnumber                 string,
        clinicorganizationidnumber          string,
        patientdataanalyticrowidnumber      string,
        patientaccessanalyticrowidnumber    string,
        resultidnumber                      string,
        patientidnumber                     string,
        analyticdos                         string,
        addeddate                           date,
        editdate                            date,
        removeddate                         date,
        removedbytype                       string,
        removedbyoutsidephysicianidnumber   string,
        removingfacilitytype                string,
        removingfacilityclinicidnumber      string,
        removingfacilitycontactid           string,
        removedreasontype                   string,
        inactivatedate                      date
        )
    ;

DROP TABLE IF EXISTS clean_patientallergy;
CREATE TABLE clean_patientallergy (
        analyticrowidnumber             string,
        clinicorganizationidnumber      string,
        patientdataanalyticrowidnumber  string,
        patientalergyidnumber           string,
        patientidnumber                 string,
        medidnumber                     string,
        medicationname                  string,
        typeofreaction                  string,
        startdate                       date,
        enddate                         date,
        drug_id                         string,
        category_concept_id             string,
        categoryname                    string,
        addeddate                       date,
        editdate                        date,
        inactivatedate                  date
        )
    ;

DROP TABLE IF EXISTS clean_patientcms2728;
CREATE TABLE clean_patientcms2728 (
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
        prelab_albumindate                  date,
        prelab_albuminll                    string,
        prelab_albuminlldate                date,
        prelab_bun                          string,
        prelab_bundate                      date,
        prelab_creatinine                   string,
        prelab_creatininedate               date,
        prelab_creatinineclearance          string,
        prelab_creatinineclearancedate      date,
        prelab_hba1c                        string,
        prelab_hba1cdate                    date,
        prelab_hematocrit                   string,
        prelab_hematocritdate               date,
        prelab_hemoglobin                   string,
        prelab_hemoglobindate               date,
        prelab_labmethodused                string,
        prelab_labmethoduseddate            date,
        prelab_lipidprofilehdl              string,
        prelab_lipidprofilehdldate          date,
        prelab_lipidprofileldl              string,
        prelab_lipidprofileldldate          date,
        prelab_lipidprofiletc               string,
        prelab_lipidprofiletcdate           date,
        prelab_lipidprofiletg               string,
        prelab_lipidprofiletgdate           date,
        prelab_ureaclearance                string,
        prelab_ureaclearancedate            date,
        patientgfrmethod                    string,
        comorbidreportdate                  date,
        supervisingphysicianidnumber        string,
        supervisingphysiciansignaturedate   date,
        willselfdialyze                     string,
        trainingphysicianidnumber           string,
        trainingphysiciansignaturedate      date,
        patientsignaturedate                date,
        addeddate                           date,
        editdate                            date,
        inactivatedate                      date
        )
    ;

DROP TABLE IF EXISTS clean_patientcomorbidityandtransplantstate;
CREATE TABLE clean_patientcomorbidityandtransplantstate (
        analyticrowidnumber                         string,
        clinicorganizationidnumber                  string,
        patientdataanalyticrowidnumber              string,
        patientidnumber                             string,
        causeofrenalfailure                         string,
        transplantfunctioningatdeath                string,
        transplantedkidney                          string,
        transplantdate                              date,
        transplanthospital                          string,
        medicareprovidernumberoftransplanthospital  string,
        medicareprovidernumberoftransplanthospital2 string,
        medicareprovidernumberoftransplanthospital3 string,
        prephospital                                string,
        prephospitalmedicareprovidernumber          string,
        prephospitalenterdate                       date,
        currentstateoftransplant                    string,
        transplantcandidate                         date,
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
        editdatetransplant                          date,
        inactivatedate                              date
        )
    ;

DROP TABLE IF EXISTS clean_patientdiagcodes;
CREATE TABLE clean_patientdiagcodes (
        analyticrowidnumber             string,
        clinicorganizationidnumber      string,
        patientdataanalyticrowidnumber  string,
        patientdiagcodeid               string,
        patientidnumber                 string,
        diagnosiscode                   string,
        description                     string,
        diagnosisdate                   date,
        diagnosistype                   string,
        diagnosispriority               string,
        admitting                       string,
        addeddate                       date,
        editdate                        date,
        diagnosisenddate                date,
        orderedbyidnumber               string,
        inactivatedate                  date,
        icd10                           string,
        analyticdos                     string
        )
    ;

DROP TABLE IF EXISTS clean_patientdialysisprescription;
CREATE TABLE clean_patientdialysisprescription (
        analyticrowidnumber                 string,
        clinicorganizationidnumber          string,
        patientdataanalyticrowidnumber      string,
        patientidnumber                     string,
        startdate                           date,
        enddate                             date,
        modalitytype                        string,
        modality                            string,
        primarydialysissetting              string,
        primarydialysisperiod               string,
        drygoalweight                       string,
        drygoalweightuom                    string,
        tbd                                 string,
        addeddate                           date,
        editdate                            date,
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
        inactivatedate                      date,
        analyticdos                         string
        )
    ;

DROP TABLE IF EXISTS clean_patientdialysisrxhemo;
CREATE TABLE clean_patientdialysisrxhemo (
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
        inactivatedate                                  date
        )
    ;

DROP TABLE IF EXISTS clean_patientdialysisrxpd;
CREATE TABLE clean_patientdialysisrxpd (
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
        inactivatedate                                  date
        )
    ;

DROP TABLE IF EXISTS clean_patientdialysisrxpdexchanges;
CREATE TABLE clean_patientdialysisrxpdexchanges (
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
        addeddate                                       date,
        editdate                                        date,
        solutiontype                                    string,
        inactivatedate                                  date
        )
    ;

DROP TABLE IF EXISTS clean_patientevent;
CREATE TABLE clean_patientevent (
        analyticrowidnumber             string,
        clinicorganizationidnumber      string,
        patientdataanalyticrowidnumber  string,
        patienteventidnumber            string,
        patientidnumber                 string,
        eventdate                       date,
        eventtype                       string,
        addeddate                       date,
        editdate                        date,
        deleteddate                     date,
        resultidnumber                  string,
        patientvascularaccessidnumber   string,
        patientinfectionidnumber        string,
        inactivatedate                  date,
        analyticdos                     string
        )
    ;

DROP TABLE IF EXISTS clean_patientfluidweightmanagement;
CREATE TABLE clean_patientfluidweightmanagement (
        analyticrowidnumber                 string,
        clinicorganizationidnumber          string,
        patientdataanalyticrowidnumber      string,
        cklresultid                         string,
        patientidnumber                     string,
        analyticdos                         string,
        sodiumeducationdate                 date,
        addeddate                           date,
        editdate                            date,
        sodiumeducationreceived             string,
        sodiumprofilingrxed                 string,
        constantdialysatesodiumrxed         string,
        dialysatesodiumover138              string,
        postdialysiswgtassessmentdate       date,
        postdialysistargetwgtrxed           string,
        homebpprovided                      string,
        homebpstatus                        string,
        dryweightrxed                       string,
        hasedema                            string,
        echocardiogramdate                  date,
        hasabnormalbreathsounds             string,
        hasleftventricularhypertrophy       string,
        leftventricularhypertrophychanged   string,
        inactivatedate                      date
        )
    ;

DROP TABLE IF EXISTS clean_patientheighthistory;
CREATE TABLE clean_patientheighthistory (
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
        addeddate                       date,
        editdate                        date,
        measuredate                     date,
        unstable                        string,
        inactivatedate                  date
        )
    ;

DROP TABLE IF EXISTS clean_patientinfection;
CREATE TABLE clean_patientinfection (
        analyticrowidnumber                 string,
        clinicorganizationidnumber          string,
        patientdataanalyticrowidnumber      string,
        patientinfectionidnumber            string,
        patientidnumber                     string,
        suspectedinfectionstartdate         date,
        confirmationdate                    date,
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
        enddate                             date,
        infectionrequiredhospitalization    string,
        hospitalizationdate                 date,
        deathrelatedtoinfection             string,
        pdeffluentcellcountsdifferential    string,
        addeddate                           date,
        editdate                            date,
        inactivatedate                      date,
        analyticdos                         string,
        lossofvascularaccess                string,
        accesssitenhsn_fistula              string,
        accesssitenhsn_graft                string,
        accesssitenhsn_tunncentralline      string,
        accesssitenhsn_nontunncentralline   string,
        accesssitenhsn_otheraccessdevice    string
        )
    ;

DROP TABLE IF EXISTS clean_patientinfection_laborganism;
CREATE TABLE clean_patientinfection_laborganism (
        analyticrowidnumber                         string,
        clinicorganizationidnumber                  string,
        patientdataanalyticrowidnumber              string,
        analyticdos                                 string,
        patientidnumber                             string,
        patientinfection_laborganismidnumber        string,
        patientinfection_labresultcultureidnumber   string,
        organismcode                                string,
        organismrank                                string,
        addeddate                                   date,
        editdate                                    date,
        inactivatedate                              date
        )
    ;

DROP TABLE IF EXISTS clean_patientinfection_laborganismdrug;
CREATE TABLE clean_patientinfection_laborganismdrug (
        analyticrowidnumber                         string,
        clinicorganizationidnumber                  string,
        patientdataanalyticrowidnumber              string,
        analyticdos                                 string,
        patientidnumber                             string,
        patientinfection_laborganismdrugidnumber    string,
        patientinfection_laborganismidnumber        string,
        drugcode                                    string,
        susceptibilitycode                          string,
        addeddate                                   date,
        editdate                                    date,
        inactivatedate                              date
        )
    ;

DROP TABLE IF EXISTS clean_patientinfection_labresultculture;
CREATE TABLE clean_patientinfection_labresultculture (
        analyticrowidnumber                         string,
        clinicorganizationidnumber                  string,
        patientdataanalyticrowidnumber              string,
        patientinfection_labresultcultureidnumber   string,
        patientinfectionidnumber                    string,
        patientidnumber                             string,
        resulteddate                                date,
        resulttype                                  string,
        collecteddate                               date,
        culturesource                               string,
        cultureresult                               string,
        organisms                                   string,
        addeddate                                   date,
        editdate                                    date,
        inactivatedate                              date,
        analyticdos                                 string
        )
    ;

DROP TABLE IF EXISTS clean_patientinfection_medication;
CREATE TABLE clean_patientinfection_medication (
        analyticrowidnumber                     string,
        clinicorganizationidnumber              string,
        patientdataanalyticrowidnumber          string,
        patientinfection_medicationidnumber     string,
        patientinfectionidnumber                string,
        patientprescriptionmedsidnumber         string,
        patientidnumber                         string,
        addeddate                               date,
        editdate                                date,
        inactivatedate                          date
        )
    ;

DROP TABLE IF EXISTS clean_patientinstabilityhistory;
CREATE TABLE clean_patientinstabilityhistory (
        analyticrowidnumber                 string,
        clinicorganizationidnumber          string,
        patientdataanalyticrowidnumber      string,
        patientinstabilityhistoryidnumber   string,
        patientidnumber                     string,
        eventdate                           date,
        event                               string,
        eventnotes                          string,
        dateofcia                           date,
        cklresultid                         string,
        relationid                          string,
        relationtype                        string,
        addeddate                           date,
        editdate                            date,
        deleted                             string,
        deleteddate                         date,
        inactivatedate                      date
        )
    ;

DROP TABLE IF EXISTS clean_patientmedadministered;
CREATE TABLE clean_patientmedadministered (
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
        startdate                        date,
        enddate                          date,
        prn                              string,
        adminduringrun                   string,
        addeddate                        date,
        editdate                         date,
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
        datenextdose                     date,
        datedoselastgiven                date,
        administrationcode               string,
        procedurecode                    string,
        medicationcode                   string,
        patientprovided                  string,
        esrdrelated                      string,
        inactivatedate                   date,
        selfadmin                        string,
        adminduringfacility              string,
        bulksupply                       string,
        treatmentidnumber                string,
        icd10                            string,
        genproduct_id                    string,
        drug_id                          string
        )
    ;

DROP TABLE IF EXISTS clean_patientmednotgiven;
CREATE TABLE clean_patientmednotgiven (
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
        startdate                               date,
        enddate                                 date,
        prn                                     string,
        adminduringrun                          string,
        addeddate                               date,
        editdate                                date,
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
        datenextdose                            date,
        datedoselastgiven                       date,
        administrationcode                      string,
        procedurecode                           string,
        medicationcode                          string,
        patientprovided                         string,
        esrdrelated                             string,
        patientprescriptionmedsparentid         string,
        inactivatedate                          date,
        selfadmin                               string,
        adminduringfacility                     string,
        bulksupply                              string,
        treatmentidnumber                       string,
        icd10                                   string,
        genproduct_id                           string,
        drug_id                                 string
        )
    ;

DROP TABLE IF EXISTS clean_patientmedprescription;
CREATE TABLE clean_patientmedprescription (
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
        startdate                        date,
        enddate                          date,
        prn                              string,
        adminduringrun                   string,
        addeddate                        date,
        editdate                         date,
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
        datenextdose                     date,
        datedoselastgiven                date,
        administrationcode               string,
        patientprovided                  string,
        patientprescriptionmedsparentid  string,
        protocolmed                      string,
        patientmasterscheduleheaderid    string,
        updatereason                     date,
        esrdrelated                      string,
        analyticdos                      string,
        inactivatedate                   date,
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
        eprescribeddate                  date	
        )
    ;

DROP TABLE IF EXISTS clean_patientstatushistory;
CREATE TABLE clean_patientstatushistory (
        analyticrowidnumber             string,
        clinicorganizationidnumber      string,
        patientdataanalyticrowidnumber  string,
        patientstatushistoryidnumber    string,
        patientidnumber                 string,
        status_original                 string,
        patientstatus                   string,
        statusisactive                  string,
        datelaststatuschange            date,
        addeddate                       date,
        editdate                        date,
        editdatehistory                 date,
        inactivatedate                  date,
        analyticdos                     string
        )
    ;

DROP TABLE IF EXISTS clean_problemlist;
CREATE TABLE clean_problemlist (
        analyticrowidnumber             string,
        patientdataanalyticrowidnumber  string,
        clinicorganizationidnumber      string,
        problemlistidnumber             string,
        patientidnumber                 string,
        analyticdos                     string,
        startdate                       date,
        enddate                         date,
        active                          string,
        category                        string,
        displayonreport                 string,
        icd9                            string,
        icd9text                        string,
        icd10                           string,
        icd10text                       string,
        patientcms2728idnumber          string,
        cms2728code                     string,
        addeddate                       date,
        editdate                        date,
        inactivatedate                  date
        )
    ;

DROP TABLE IF EXISTS clean_sodiumufprofile;
CREATE TABLE clean_sodiumufprofile (
        analyticrowidnumber string,
        profileidnumber     string,
        profilenumber       string,
        profiletype         string,
        begininglevel       string,
        endinglevel         string,
        addeddate           date,
        editdate            date,
        inactivatedate      date
        )
    ;

DROP TABLE IF EXISTS clean_stategeo;
CREATE TABLE clean_stategeo (
        stateabbreviation           string,
        statename                   string,
        statefipscode               string,
        divisionname                string,
        censusbureaudivisionnumber  string,
        regionname                  string,
        censusbureauregionnumber    string
        )
    ;

DROP TABLE IF EXISTS clean_zipgeo;
CREATE TABLE clean_zipgeo (
        geo_id              string,
        zipcode             string,
        sumlevel            string,
        geo_name            string,
        totalpopulation     string,
        urbanpopulation     string,
        insideurbanarea     string,
        insideurbancluster  string,
        ruralpopulation     string,
        na                  string,
        ruralvsurban        string
        )
    ;

