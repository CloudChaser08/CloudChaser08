DROP TABLE IF EXISTS Hospitalization;
CREATE EXTERNAL TABLE Hospitalization (
        AnalyticRowIDNumber             string,
        ClinicOrganizationIDNumber      string,
        PatientDataAnalyticRowIDNumber  string,
        PatientHospitalizationIDNumber  string,
        PatientIDNumber                 string,
        AdmittingPhysicianIDNumber      string,
        AdmissionDate                   string,
        DischargeDate                   string,
        AddedDate                       string,
        EditDate                        string,
        HospConsult                     string,
        Unstable                        string,
        StayLength                      string,
        ICD9                            string,
        PatientDialyzed                 string,
        TypeVisit                       string,
        PresumptiveDiagnosis            string,
        TransplantReferral              string,
        InactivateDate                  string,
        ICD10                           string,
        AnalyticDOS                     string
        )
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
    STORED AS TEXTFILE
    LOCATION {hospitalization_input}
    ;

DROP TABLE IF EXISTS Immunization;
CREATE EXTERNAL TABLE Immunization (
        AnalyticRowIDNumber             string,
        ClinicOrganizationIDNumber      string,
        PatientDataAnalyticRowIDNumber  string,
        PatientImmunizationIDNumber     string,
        PatientIDNumber                 string,
        AnalyticDOS                     string,
        ImmunizationType                string,
        IsBooster                       string,
        NotGiven                        string,
        OnOffSite                       string,
        Immunization                    string,
        ImmunizationDate                string,
        AddedDate                       string,
        EditDate                        string,
        AdministeredOnDialysis          string,
        JustificationForAdministered    string,
        Scheduled                       string,
        Refused                         string,
        RunIDNumber                     string,
        AdministrationCode              string,
        MedicationCode                  string,
        PhysicianIDNumber               string,
        Status                          string,
        LotNumber                       string,
        ExpirationDate                  string,
        ImmunizationRoute               string,
        InactivateDate                  string,
        TreatmentIDNumber               string,
        ICD10                           string
        )
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
    STORED AS TEXTFILE
    LOCATION {immunization_input}
    ;

DROP TABLE IF EXISTS LabPanelsDrawn;
CREATE EXTERNAL TABLE LabPanelsDrawn (
        AnalyticRowIDNumber             string,
        ClinicOrganizationIDNumber      string,
        PatientDataAnalyticRowIDNumber  string,
        LabPanelsDrawnID                string,
        AnalyticDOS                     string,
        LabScheduleIDNumber             string,
        PatientIDNumber                 string,
        Status                          string,
        PanelName                       string,
        Justification                   string,
        AddedDate                       string,
        EditDate                        string,
        RunIDNumber                     string,
        PostPonedTo                     string,
        theDate                         string,
        PanelIDNumber                   string,
        AdministrationCode              string,
        ProcedureCode                   string,
        MedicationCode                  string,
        PhysicianIDNumber               string,
        InactivateDate                  string,
        TreatmentIDNumber               string
        )
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
    STORED AS TEXTFILE
    LOCATION {labpanelsdrawn_input}
    ;

DROP TABLE IF EXISTS LabResult;
CREATE EXTERNAL TABLE LabResult (
        AnalyticRowIDNumber             string,
        ClinicOrganizationIDNumber      string,
        PatientDataAnalyticRowIDNumber  string,
        AnalyticDOS                     string,
        LabResultIDNumber               string,
        PatientIDNumber                 string,
        OrderedBy                       string,
        OrderDate                       string,
        CompletedDate                   string,
        ReceivedDate                    string,
        TestName                        string,
        TestResult                      string,
        AddedDate                       string,
        EditDate                        string,
        calculated                      string,
        scaled                          string,
        textresult                      string,
        referencerange                  string,
        ObservationIdentifier           string,
        UniversalServiceID              string,
        FmtResult                       string,
        AbnormalFlags                   string,
        DiagnosisCode                   string,
        ESRDRelated                     string,
        RunIDNumber                     string,
        MedicationCode                  string,
        AdministrationCode              string,
        ProcedureCode                   string,
        InactivateDate                  string,
        ICD10                           string
        )
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
    STORED AS TEXTFILE
    LOCATION {labresult_input}
    ;

DROP TABLE IF EXISTS PatientAccess_ExamProc;
CREATE EXTERNAL TABLE PatientAccess_ExamProc (
        AnalyticRowIDNumber                  string,
        ClinicOrganizationIDNumber           string,
        PatientDataAnalyticRowIDNumber       string,
        PatientAccessAnalyticRowIDNumber     string,
        ResultIDNumber                       string,
        PatientIDNumber                      string,
        AnalyticDOS                          string,
        AddedDate                            string,
        EditDate                             string,
        ExamProcedureDate                    string,
        ExamProcedureType                    string,
        ExamProcedureFrequency               string,
        PerformedByType                      string,
        PerformedByOutsidePhysicianIDNumber  string,
        PerformingFacilityType               string,
        PerformingFacilityClinicIDNumber     string,
        PerformingFacilityContactID          string,
        IsBillableByClinic                   string,
        DiagnosticCode                       string,
        ProcedureCode                        string,
        ExamProcedureIsStateActive           string,
        ExamProcedureStatusAfter             string,
        ExamProcedureIsPrimaryAccess         string,
        InactivateDate                       string,
        ICD10                                string
        )
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
    STORED AS TEXTFILE
    LOCATION {patientaccess_examproc_input}
    ;

DROP TABLE IF EXISTS PatientDiagCodes;
CREATE EXTERNAL TABLE PatientDiagCodes (
        AnalyticRowIDNumber             string,
        ClinicOrganizationIDNumber      string,
        PatientDataAnalyticRowIDNumber  string,
        PatientDiagCodeID               string,
        PatientIDNumber                 string,
        DiagnosisCode                   string,
        Description                     string,
        DiagnosisDate                   string,
        DiagnosisType                   string,
        DiagnosisPriority               string,
        Admitting                       string,
        AddedDate                       string,
        EditDate                        string,
        DiagnosisEndDate                string,
        OrderedByIDNumber               string,
        InactivateDate                  string,
        ICD10                           string,
        AnalyticDOS                     string
        )
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
    STORED AS TEXTFILE
    LOCATION {patientdiagcodes_input}
    ;

DROP TABLE IF EXISTS PatientMedAdministered;
CREATE EXTERNAL TABLE PatientMedAdministered (
        AnalyticRowIDNumber              string,
        PatientDataAnalyticRowIDNumber   string,
        ClinicOrganizationIDNumber       string,
        PatientAdministeredMedsIDNumber  string,
        PatientIDNumber                  string,
        AnalyticDOS                      string,
        RunIDNumber                      string,
        MedIDNumber                      string,
        Medication                       string,
        Route                            string,
        Dose                             string,
        Frequency                        string,
        Duration                         string,
        Prescription                     string,
        StartDate                        string,
        EndDate                          string,
        PRN                              string,
        AdminDuringRun                   string,
        AddedDate                        string,
        EditDate                         string,
        AdministrationTime               string,
        RunJustification                 string,
        MedPrescIDNumber                 string,
        TotalDoses                       string,
        DosesGiven                       string,
        UseDoses                         string,
        DoseQty                          string,
        DoseUnit                         string,
        DoseRoute                        string,
        DoseFreq                         string,
        DoseFreqMonday                   string,
        DoseFreqTuesday                  string,
        DoseFreqWednesday                string,
        DoseFreqThursday                 string,
        DoseFreqFriday                   string,
        DoseFreqSaturday                 string,
        DoseFreqSunday                   string,
        Hold                             string,
        HoldUntil                        string,
        LastDoseOn                       string,
        PatientNotTaking                 string,
        PhysicianIDNumber                string,
        FixedWeekInterval                string,
        DateNextDose                     string,
        DateDoseLastGiven                string,
        AdministrationCode               string,
        ProcedureCode                    string,
        MedicationCode                   string,
        PatientProvided                  string,
        ESRDRelated                      string,
        InactivateDate                   string,
        SelfAdmin                        string,
        AdminDuringFacility              string,
        BulkSupply                       string,
        TreatmentIDNumber                string,
        ICD10                            string,
        GENPRODUCT_ID                    string,
        DRUG_ID                          string
        )
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
    STORED AS TEXTFILE
    LOCATION {patientmedadministered_input}
    ;

DROP TABLE IF EXISTS PatientMedPrescription;
CREATE EXTERNAL TABLE PatientMedPrescription (
        AnalyticRowIDNumber              string,
        PatientDataAnalyticRowIDNumber   string,
        ClinicOrganizationIDNumber       string,
        PatientAdministeredMedsIDNumber  string,
        PatientIDNumber                  string,
        MedIDNumber                      string,
        Medication                       string,
        Prescription                     string,
        Hold                             string,
        HoldUntil                        string,
        StartDate                        string,
        EndDate                          string,
        PRN                              string,
        AdminDuringRun                   string,
        AddedDate                        string,
        EditDate                         string,
        LastDoseOn                       string,
        PatientNotTaking                 string,
        RunJustification                 string,
        PhysicianIDNumber                string,
        TotalDoses                       string,
        DosesGiven                       string,
        UseDoses                         string,
        DoseQty                          string,
        DoseUnit                         string,
        DoseRoute                        string,
        DoseFreq                         string,
        DoseFreqMonday                   string,
        DoseFreqTuesday                  string,
        DoseFreqWednesday                string,
        DoseFreqThursday                 string,
        DoseFreqFriday                   string,
        DoseFreqSaturday                 string,
        DoseFreqSunday                   string,
        FixedWeekInterval                string,
        DateNextDose                     string,
        DateDoseLastGiven                string,
        AdministrationCode               string,
        PatientProvided                  string,
        PatientPrescriptionMedsParentID  string,
        ProtocolMed                      string,
        PatientMasterScheduleHeaderID    string,
        UpdateReason                     string,
        ESRDRelated                      string,
        AnalyticDOS                      string,
        InactivateDate                   string,
        MonthlyDose                      string,
        SelfAdmin                        string,
        AdminDuringFacility              string,
        BulkSupply                       string,
        ICD10                            string,
        GENPRODUCT_ID                    string,
        DRUG_ID                          string,
        DoNOTSubstitute                  string,
        StartingDosesGiven               string,
        DoseStrength                     string,
        DoseForm                         string,
        EPrescribed                      string,
        EPrescribedQuantity              string,
        EPrescribedRefill                string,
        EPrescribedDate                  string	
        )
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
    STORED AS TEXTFILE	
    LOCATION {patientmedprescription_input}
    ;

DROP TABLE IF EXISTS ProblemList;
CREATE EXTERNAL TABLE ProblemList (
        AnalyticRowIDNumber             string,
        PatientDataAnalyticRowIDNumber  string,
        ClinicOrganizationIDNumber      string,
        ProblemListIDNumber             string,
        PatientIDNumber                 string,
        AnalyticDOS                     string,
        StartDate                       string,
        EndDate                         string,
        Active                          string,
        Category                        string,
        DisplayOnReport                 string,
        ICD9                            string,
        ICD9Text                        string,
        ICD10                           string,
        ICD10Text                       string,
        PatientCMS2728IDNumber          string,
        CMS2728Code                     string,
        AddedDate                       string,
        EditDate                        string,
        InactivateDate                  string
        )
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
    STORED AS TEXTFILE
    LOCATION {problemlist_input}
    ;

DROP TABLE IF EXISTS LabIDList;
CREATE EXTERNAL TABLE LabIDList (
        AnalyticRowIDNumber    string,
        UniversalServiceID     string,
        ObservationIdentifier  string,
        TestName               string,
        CPTCode                string,
        ICD9DiagnosisCode      string,
        CPTInfo                string,
        ICD9Info               string,
        Revised_Info           string,
        Differencees           string,
        InactivateDate         string
        )
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
    STORED AS TEXTFILE
    LOCATION {labidlist_input}
    ;
