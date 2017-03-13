-- DROP TABLE IF EXISTS Address;
-- CREATE EXTERNAL TABLE Address (
--         AnalyticRowIDNumber             string,
--         ClinicOrganizationIDNumber      string,
--         PatientDataAnalyticRowIDNumber  string,
--         State                           string,
--         ZipCode                         string,
--         PatientAddressIdNumber          string,
--         PatientIDNumber                 string,
--         InactivateDate                  string,
--         AssociationIDNumber             string
--         )
--     ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
--     STORED AS TEXTFILE
--     LOCATION {input_path}.Address.csv
-- ;

-- DROP TABLE IF EXISTS ClinicPreference;
-- CREATE EXTERNAL TABLE ClinicPreference (
--         AnalyticRowIDNumber         string,
--         ClinicOrganizationIDNumber  string,
--         AssociationIDNumber         string,
--         LabMethodAlbumin            string,
--         SerumAlbuminLower           string,
--         HemodialysisMethod          string,
--         PeritonealDialysisMethod    string,
--         PNAMeasure                  string,
--         BSAMethod                   string,
--         CreatineClearanceMethod     string,
--         InactivateDate              string
--         )
--     ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
--     STORED AS TEXTFILE
--     LOCATION {input_path}.ClinicPreference.csv
-- ;

-- DROP TABLE IF EXISTS DialysisTraining;
-- CREATE EXTERNAL TABLE DialysisTraining (
--         AnalyticRowIDNumber             string,
--         ClinicOrganizationIDNumber      string,
--         PatientDataAnalyticRowIDNumber  string,
--         CklResultID                     string,
--         PatientIDNumber                 string,
--         AnalyticDOS                     string,
--         AddedDate                       string,
--         EditDate                        string,
--         TrainingType                    string,
--         OtherTraining                   string,
--         ExpectedSelfCare                string,
--         DialysisPeriod                  string,
--         DialysisTrainingStart           string,
--         DialysisTrainingEnd             string,
--         InactivateDate                  string
--         )
--     ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
--     STORED AS TEXTFILE
--     LOCATION {input_path}.DialysisTraining.csv
-- ;

-- DROP TABLE IF EXISTS DialysisTreatment;
-- CREATE EXTERNAL TABLE DialysisTreatment (
--         AnalyticRowIDNumber                     string,
--         ClinicOrganizationIDNumber              string,
--         PatientDataAnalyticRowIDNumber          string,
--         RunIDNumber                             string,
--         PatientIDNumber                         string,
--         AnalyticDOS                             string,
--         Status_Original                         string,
--         PatientStatus                           string,
--         PrimaryDialysisSetting                  string,
--         ClinicIDNumber                          string,
--         LocationID                              string,
--         OutpatientRun                           string,
--         TreatmentStart                          string,
--         TreatmentEnd                            string,
--         AverageBloodFlowRate                    string,
--         AverageBloodFlowRateUOM                 string,
--         PatientVascularAccessIDNumber           string,
--         StartSittingBP                          string,
--         StartSittingPulse                       string,
--         EndSittingBP                            string,
--         EndSittingPulse                         string,
--         StartStandingBP                         string,
--         StartStandingPulse                      string,
--         EndStandingBP                           string,
--         EndStandingPulse                        string,
--         RunHighBP                               string,
--         RunLowBP                                string,
--         RunActualDialysisRxIDNumber             string,
--         DryWeight                               string,
--         DryWeightUOM                            string,
--         LastWeight                              string,
--         LastWeightUOM                           string,
--         PreWeight                               string,
--         PreWeightUOM                            string,
--         PostWeight                              string,
--         PostWeightUOM                           string,
--         LitersProcessed                         string,
--         LitersProcessedUOM                      string,
--         TimeDialyzed                            string,
--         FluidRemoved                            string,
--         FluidRemovedUOM                         string,
--         DialysateTemp                           string,
--         DialysateTempUOM                        string,
--         PatientTempStart                        string,
--         PatientTempStartUOM                     string,
--         PatientTempEnd                          string,
--         PatientTempEndUOM                       string,
--         PatientTempHigh                         string,
--         PatientTempHighUOM                      string,
--         MachineHeaderID                         string,
--         txSubtype                               string,
--         posteddate                              string,
--         source                                  string,
--         LateStart                               string,
--         CompleteTx                              string,
--         Disposition                             string,
--         AmbulatoryStatus                        string,
--         StatusTimeTx                            string,
--         StartLyingBP                            string,
--         StartLyingPulse                         string,
--         EndLyingBP                              string,
--         EndLyingPulse                           string,
--         Shift                                   string,
--         InfectionPresent                        string,
--         AddedDate                               string,
--         EditDate                                string,
--         InactivateDate                          string,
--         StatusIsActive                          string,
--         PatientVascularAccessSecondaryIDNumber  string,
--         CFResultIDNumber                        string,
--         PatientDialysisRxHeaderIDNumber         string,
--         TreatmentTypeCategory                   string,
--         TreatmentTypeSubType                    string,
--         IsPrimary                               string
--         )
--     ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
--     STORED AS TEXTFILE
--     LOCATION {input_path}.DialysisTreatment.csv
-- ;

-- DROP TABLE IF EXISTS FacilityAdmitDischarge;
-- CREATE EXTERNAL TABLE FacilityAdmitDischarge (
--         AnalyticRowIDNumber             string,
--         ClinicOrganizationIDNumber      string,
--         PatientDataAnalyticRowIDNumber  string,
--         PatientFacilityIDNumber         string,
--         PatientIDNumber                 string,
--         ClinicIDNumber                  string,
--         AdmitDate                       string,
--         AdmitReason                     string,
--         DischargeDate                   string,
--         DischargeReason                 string,
--         AddedDate                       string,
--         EditDate                        string,
--         NetworkEvent                    string,
--         PatientMasterScheduleHeaderID   string,
--         PatientMasterScheduleID         string,
--         InvoluntaryDischargeReason      string,
--         TransferDischargeReason         string,
--         TransientReason                 string,
--         InactivateDate                  string,
--         AnalyticDOS                     string
--         )
--     ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
--     STORED AS TEXTFILE
--     LOCATION {input_path}.FacilityAdmitDischarge.csv
-- ;

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
    LOCATION {input_path}.Hospitalization.csv
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
    LOCATION {input_path}.Immunization.csv
;

-- DROP TABLE IF EXISTS Insurance;
-- CREATE EXTERNAL TABLE Insurance (
--         AnalyticRowIDNumber             string,
--         ClinicOrganizationIDNumber      string,
--         PatientDataAnalyticRowIDNumber  string,
--         InsuranceIDNumber               string,
--         PatientIDNumber                 string,
--         InsuranceCompanyIdNumber        string,
--         InsuranceCompanyName            string,
--         InsuranceCompanyID              string,
--         InsuranceCompanyAddressLine1    string,
--         InsuranceCompanyAddressLine2    string,
--         InsuranceCompanyCity            string,
--         InsuranceCompanyState           string,
--         InsuranceCompanyZipCode         string,
--         InsuranceCompanyPrimaryPhone    string,
--         PayorID                         string,
--         PlanEffectiveDate               string,
--         PlanExpirationDate              string,
--         AuthorizationNumber             string,
--         PlanType                        string,
--         AddedDate                       string,
--         EditDate                        string,
--         InsuredNumberID                 string,
--         MedicarePartA                   string,
--         CompanyPlanType                 string,
--         InactivateDate                  string
--         )
--     ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
--     STORED AS TEXTFILE
--     LOCATION {input_path}.Insurance.csv
-- ;

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
    LOCATION {input_path}.LabPanelsDrawn.csv
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
    LOCATION {input_path}.LabResult.csv
;

-- DROP TABLE IF EXISTS ModalityChangeHistory;
-- CREATE EXTERNAL TABLE ModalityChangeHistory (
--         AnalyticRowIDNumber                             string,
--         PatientDataAnalyticRowIDNumber                  string,
--         DefaultClinicOrganizationIDNumber               string,
--         TreatmentClinicOrganizationIDNumber             string,
--         PatientIDNumber                                 string,
--         EffectiveDate                                   string,
--         PhysicianAnalyticRowIDNumber                    string,
--         PatientPhysicianAnalyticRowIDNumber             string,
--         TreatmentType                                   string,
--         Modality                                        string,
--         MinutesPerSession                               string,
--         PatientDialysisPrescriptionAnalyticRowIDNumber  string,
--         SessionsPerWeek                                 string,
--         PatientMasterScheduleHeaderAnalyticRowIDNumber  string,
--         DialysisTreatmentAnalyticRowIDNumber            string,
--         PrimaryDialysisSetting                          string,
--         CrownDialysisSetting                            string,
--         PatientStatusHistoryAnalyticRowIDNumber         string,
--         CaptureDate                                     string
--         )
--     ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
--     STORED AS TEXTFILE
--     LOCATION {input_path}.ModalityChangeHistory.csv
-- ;

-- DROP TABLE IF EXISTS ModalityChangeHistoryCrownWeb;
-- CREATE EXTERNAL TABLE ModalityChangeHistoryCrownWeb (
--         AnalyticRowIDNumber                             string,
--         ModalityChangeHistoryAnalyticRowIDNumber        string,
--         PatientDialysisPrescriptionAnalyticRowIDNumber  string,
--         PatientIDNumber                                 string,
--         PatientDataAnalyticRowIDNumber                  string,
--         DefaultClinicOrganizationIDNumber               string,
--         TreatmentClinicOrganizationIDNumber             string,
--         EffectiveReportingDate                          string,
--         PhysicianAnalyticRowIDNumber                    string,
--         TreatmentType                                   string,
--         MinutesPerSession                               string,
--         SessionsPerWeek                                 string,
--         CrownDialysisSetting                            string
--         )
--     ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
--     STORED AS TEXTFILE
--     LOCATION {input_path}.ModalityChangeHistoryCrownWeb.csv
-- ;

-- DROP TABLE IF EXISTS NursingHomeHistory;
-- CREATE EXTERNAL TABLE NursingHomeHistory (
--         AnalyticRowIDNumber             string,
--         ClinicOrganizationIDNumber      string,
--         PatientDataAnalyticRowIDNumber  string,
--         PatientNursingHomeIDNumber      string,
--         PatientIDNumber                 string,
--         NursingHomeID                   string,
--         StartDate                       string,
--         EndDate                         string,
--         AddedDate                       string,
--         EditDate                        string,
--         InactivateDate                  string,
--         AnalyticDOS                     string
--         )
--     ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
--     STORED AS TEXTFILE
--     LOCATION {input_path}.NursingHomeHistory.csv
-- ;

-- DROP TABLE IF EXISTS PatientAccess;
-- CREATE EXTERNAL TABLE PatientAccess (
--         AnalyticRowIDNumber             string,
--         PatientDataAnalyticRowIDNumber  string,
--         ClinicOrganizationIDNumber      string,
--         PatientVascularAccessIDNumber   string,
--         PatientIdNumber                 string,
--         VascularAccessType              string,
--         Location                        string,
--         CurrentStatus                   string,
--         ChronicCatheter                 string,
--         AnalyticDOS                     string,
--         StartDate                       string,
--         EndDate                         string,
--         AddedDate                       string,
--         EditDate                        string,
--         PlacedRecorded                  string,
--         IsPrimaryAccess                 string,
--         IsStateActive                   string,
--         InactivateDate                  string
--         )
--     ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
--     STORED AS TEXTFILE
--     LOCATION {input_path}.PatientAccess.csv
-- ;

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
    LOCATION {input_path}.PatientAccess_ExamProc.csv
;

-- DROP TABLE IF EXISTS PatientAccess;
-- CREATE EXTERNAL TABLE PatientAccess_OtherAccessEvent (
--         AnalyticRowIDNumber               string,
--         ClinicOrganizationIDNumber        string,
--         PatientDataAnalyticRowIDNumber    string,
--         PatientAccessAnalyticRowIDNumber  string,
--         ResultIDNumber                    string,
--         PatientIDNumber                   string,
--         AnalyticDOS                       string,
--         AddedDate                         string,
--         EditDate                          string,
--         OtherAccessEventDate              string,
--         OtherAccessEventIsStateActive     string,
--         OtherAccessEventStatusAfter       string,
--         OtherAccessEventIsPrimaryAccess   string,
--         ReasonForChange                   string,
--         InactivateDate                    string
--         )
--     ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
--     STORED AS TEXTFILE
--     LOCATION {input_path}.PatientAccess.csv
-- ;

-- DROP TABLE IF EXISTS PatientAccess;
-- CREATE EXTERNAL TABLE PatientAccess_PlacedRecorded (
--         AnalyticRowIDNumber               string,
--         ClinicOrganizationIDNumber        string,
--         PatientDataAnalyticRowIDNumber    string,
--         PatientAccessAnalyticRowIDNumber  string,
--         ResultIDNumber                    string,
--         PatientIDNumber                   string,
--         AnalyticDOS                       string,
--         AddedDate                         string,
--         EditDate                          string,
--         PlacedRecorded                    string,
--         PlacedRecordedDate                string,
--         AccessType                        string,
--         AccessLocation                    string,
--         CathName                          string,
--         IsChronicCath                     string,
--         PlacedByType                      string,
--         PlacedByOutsidePhysicianIDNumber  string,
--         PlacingFacilityType               string,
--         PlacingFacilityClinicIDNumber     string,
--         PlacingFacilityContactID          string,
--         PlacedRecordedIsStateActive       string,
--         PlacedRecordedStatusAfter         string,
--         PlacedRecordedIsPrimaryAccess     string,
--         InactivateDate                    string
--         )
--     ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
--     STORED AS TEXTFILE
--     LOCATION {input_path}.PatientAccess.csv
-- ;

-- DROP TABLE IF EXISTS PatientAccess;
-- CREATE EXTERNAL TABLE PatientAccess_Removed (
--         AnalyticRowIDNumber                string,
--         ClinicOrganizationIDNumber         string,
--         PatientDataAnalyticRowIDNumber     string,
--         PatientAccessAnalyticRowIDNumber   string,
--         ResultIDNumber                     string,
--         PatientIDNumber                    string,
--         AnalyticDOS                        string,
--         AddedDate                          string,
--         EditDate                           string,
--         RemovedDate                        string,
--         RemovedByType                      string,
--         RemovedByOutsidePhysicianIDNumber  string,
--         RemovingFacilityType               string,
--         RemovingFacilityClinicIDNumber     string,
--         RemovingFacilityContactID          string,
--         RemovedReasonType                  string,
--         InactivateDate                     string
--         )
--     ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
--     STORED AS TEXTFILE
--     LOCATION {input_path}.PatientAccess.csv
-- ;

-- DROP TABLE IF EXISTS PatientAllergy;
-- CREATE EXTERNAL TABLE PatientAllergy (
--         AnalyticRowIDNumber             string,
--         ClinicOrganizationIDNumber      string,
--         PatientDataAnalyticRowIDNumber  string,
--         PatientAlergyIDNumber           string,
--         PatientIDNumber                 string,
--         MedIDNumber                     string,
--         MedicationName                  string,
--         TypeofReaction                  string,
--         StartDate                       string,
--         EndDate                         string,
--         DRUG_ID                         string,
--         CATEGORY_CONCEPT_ID             string,
--         CategoryName                    string,
--         AddedDate                       string,
--         EditDate                        string,
--         InactivateDate                  string
--         )
--     ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
--     STORED AS TEXTFILE
--     LOCATION {input_path}.PatientAllergy.csv
-- ;

-- DROP TABLE IF EXISTS PatientCMS2728;
-- CREATE EXTERNAL TABLE PatientCMS2728 (
--         AnalyticRowIDNumber                string,
--         ClinicOrganizationIDNumber         string,
--         PatientDataAnalyticRowIDNumber     string,
--         AnalyticDOS                        string,
--         PatientIDNumber                    string,
--         Form2728Type                       string,
--         PreDryWeight                       string,
--         PreHeightFeet                      string,
--         PreHeightInches                    string,
--         ApplyingForESRDMediCare            string,
--         MedicalCoverageMedicaid            string,
--         MedicalCoverageDVA                 string,
--         MedicalCoverageMedicare2728        string,
--         MedicalCoverageMedicareAdvantage   string,
--         MedicalCoverageEmployer            string,
--         MedicalCoverageOther               string,
--         MedicalCoverageNone                string,
--         PredialysisEPOAdministered         string,
--         ESRD_ErythropoteinTx               string,
--         ESRD_ErythropoteinPeriod           string,
--         ESRD_NephrologistCare              string,
--         ESRD_NephrologistPeriod            string,
--         ESRD_KidneyDietitianCare           string,
--         ESRD_KidneyDietitianPeriod         string,
--         ESRD_FirstOPDialysisAccessType     string,
--         ESRD_FirstOPDialysisOtherAccess    string,
--         ESRD_FirstOPDialysisMaturingAVF    string,
--         ESRD_FirstOPDialysisMaturingGraft  string,
--         PreLab_Albumin                     string,
--         PreLab_AlbuminDate                 string,
--         PreLab_AlbuminLL                   string,
--         PreLab_AlbuminLLDate               string,
--         PreLab_BUN                         string,
--         PreLab_BUNDate                     string,
--         PreLab_Creatinine                  string,
--         PreLab_CreatinineDate              string,
--         PreLab_CreatinineClearance         string,
--         PreLab_CreatinineClearanceDate     string,
--         PreLab_HbA1c                       string,
--         PreLab_HbA1cDate                   string,
--         PreLab_Hematocrit                  string,
--         PreLab_HematocritDate              string,
--         PreLab_Hemoglobin                  string,
--         PreLab_HemoglobinDate              string,
--         PreLab_LabMethodUsed               string,
--         PreLab_LabMethodUsedDate           string,
--         PreLab_LipidProfileHDL             string,
--         PreLab_LipidProfileHDLDate         string,
--         PreLab_LipidProfileLDL             string,
--         PreLab_LipidProfileLDLDate         string,
--         PreLab_LipidProfileTC              string,
--         PreLab_LipidProfileTCDate          string,
--         PreLab_LipidProfileTG              string,
--         PreLab_LipidProfileTGDate          string,
--         PreLab_UreaClearance               string,
--         PreLab_UreaClearanceDate           string,
--         PatientGFRMethod                   string,
--         ComorbidReportDate                 string,
--         SupervisingPhysicianIDNumber       string,
--         SupervisingPhysicianSignatureDate  string,
--         WillSelfDialyze                    string,
--         TrainingPhysicianIDNumber          string,
--         TrainingPhysicianSignatureDate     string,
--         PatientSignatureDate               string,
--         AddedDate                          string,
--         EditDate                           string,
--         InactivateDate                     string
--         )
--     ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
--     STORED AS TEXTFILE
--     LOCATION {input_path}.PatientCMS2728.csv
-- ;

-- DROP TABLE IF EXISTS PatientComorbidityAndTransplantState;
-- CREATE EXTERNAL TABLE PatientComorbidityAndTransplantState (
--         AnalyticRowIDNumber                          string,
--         ClinicOrganizationIDNumber                   string,
--         PatientDataAnalyticRowIDNumber               string,
--         PatientIDNumber                              string,
--         CauseofRenalFailure                          string,
--         transplantfunctioningatdeath                 string,
--         TransplantedKidney                           string,
--         TransplantDate                               string,
--         TransplantHospital                           string,
--         MedicareProviderNumberofTransplantHospital   string,
--         MedicareProviderNumberofTransplantHospital2  string,
--         MedicareProviderNumberofTransplantHospital3  string,
--         PrepHospital                                 string,
--         PrepHospitalMedicareProviderNumber           string,
--         PrepHospitalEnterDate                        string,
--         CurrentStateofTransplant                     string,
--         TransplantCandidate                          string,
--         TransOpMedicallyUnfit                        string,
--         TransOpNotAssessed                           string,
--         TransOpOther                                 string,
--         TransOpPatientDeclines                       string,
--         TransOpPatientInformed                       string,
--         TransOpPsych                                 string,
--         TransOpUnsuitableAge                         string,
--         TransplantHospital1Note                      string,
--         TransplantHospital2                          string,
--         TransplantHospital2Note                      string,
--         TransplantHospital3Note                      string,
--         TransplantWaitList                           string,
--         editdatetransplant                           string,
--         InactivateDate                               string
--         )
--     ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
--     STORED AS TEXTFILE
--     LOCATION {input_path}.PatientComorbidityAndTransplantState.csv
-- ;

-- DROP TABLE IF EXISTS PatientData;
-- CREATE EXTERNAL TABLE PatientData (
--         AnalyticRowIDNumber                   string,
--         ClinicOrganizationIDNumber            string,
--         DefaultClinicIDNumber                 string,
--         PatientIDNumber                       string,
--         PatientID                             string,
--         MedicalRecordNumber                   string,
--         LabIDNumber                           string,
--         State                                 string,
--         ZipCode                               string,
--         Sex                                   string,
--         Status_Original                       string,
--         PatientStatus                         string,
--         StatusIsActive                        string,
--         PrimaryModality                       string,
--         PrimaryModality_Original              string,
--         PrimaryDialysisSetting                string,
--         DateFirstDialysis                     string,
--         LastStatusChangeDate                  string,
--         ResuscitationCode                     string,
--         AdvDirResuscitationCode               string,
--         AdvDirPowerOfAtty                     string,
--         AdvDirLivingWill                      string,
--         AdvDirPatientDeclines                 string,
--         AdvRevDate                            string,
--         TribeCode                             string,
--         InactivateDate                        string,
--         Age                                   string,
--         MonthsInDialysis                      string,
--         TransplantWaitList                    string,
--         MedicalCoverageMedicare               string,
--         MedicalCoverageMedicareEffectiveDate  string,
--         MasterPatientIDNumber                 string,
--         DateFirstDialysisCurrentUnit          string
--         )
--     ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
--     STORED AS TEXTFILE
--     LOCATION {input_path}.PatientData.csv
-- ;

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
    LOCATION {input_path}.PatientDiagCodes.csv
;

-- DROP TABLE IF EXISTS PatientDialysisPrescription;
-- CREATE EXTERNAL TABLE PatientDialysisPrescription (
--         AnalyticRowIDNumber               string,
--         ClinicOrganizationIDNumber        string,
--         PatientDataAnalyticRowIDNumber    string,
--         PatientIDNumber                   string,
--         StartDate                         string,
--         EndDate                           string,
--         ModalityType                      string,
--         Modality                          string,
--         PrimaryDialysisSetting            string,
--         PrimaryDialysisPeriod             string,
--         DryGoalWeight                     string,
--         DryGoalWeightUOM                  string,
--         TBD                               string,
--         AddedDate                         string,
--         EditDate                          string,
--         physicianidnumber                 string,
--         PatientDialysisHeaderIDNumber     string,
--         PatientDialysisRxNumber           string,
--         PatientDialysisRxInpatientNumber  string,
--         PatientRxRxNxStageIDNumber        string,
--         PatientRxRxOtherIDNumber          string,
--         PatientRxRxCAPDIDNumber           string,
--         PatientRxRxRegularCCPDIDNumber    string,
--         PatientRxRxHighDoseCCPDIDNumber   string,
--         PatientRxRxTidalIDNumber          string,
--         PatientRxRxHighDoseTidalIDNumber  string,
--         PrescriptionType                  string,
--         InactivateDate                    string,
--         AnalyticDOS                       string
--         )
--     ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
--     STORED AS TEXTFILE
--     LOCATION {input_path}.PatientDialysisPrescription.csv
-- ;

-- DROP TABLE IF EXISTS PatientDialysisRxHemo;
-- CREATE EXTERNAL TABLE PatientDialysisRxHemo (
--         AnalyticRowIDNumber                             string,
--         ClinicOrganizationIDNumber                      string,
--         PatientDataAnalyticRowIDNumber                  string,
--         PatientDialysisPrescriptionAnalyticRowIDNumber  string,
--         PatientDialysisHeaderIDNumber                   string,
--         TreatmentTime                                   string,
--         TreatmentTimeUOM                                string,
--         MembraneType                                    string,
--         BloodFlowRate                                   string,
--         ArterialNeedleSize                              string,
--         VenousNeedleSize                                string,
--         DialysateFlowRate                               string,
--         DialysisBathCalciumLevel                        string,
--         DialysisBathPotasiumLevel                       string,
--         DialysisBathSodiumLevelType                     string,
--         SodiumBegin                                     string,
--         SodiumEnd                                       string,
--         SodiumStepInstructions                          string,
--         SodiumProfileNumber                             string,
--         TotalHeparinGiven                               string,
--         HeparinInstructions                             string,
--         BicarbonateLevel                                string,
--         UltraFiltLevelType                              string,
--         UltraFiltBeginLevel                             string,
--         UltraFiltEndLevel                               string,
--         UltraFiltlStepInstructions                      string,
--         UltraFiltProfileNumber                          string,
--         LitersProcessed                                 string,
--         TotalBodyWater                                  string,
--         DialysateTemp                                   string,
--         MaxfluidRemoved                                 string,
--         TreatmentSetupTime                              string,
--         TreatmentCleanupTime                            string,
--         WeightMin                                       string,
--         WeightMax                                       string,
--         TotalVolumeDialysate                            string,
--         Filtration                                      string,
--         OtherFiltration                                 string,
--         BloodflowRateMin                                string,
--         BloodflowRateMax                                string,
--         Lactate                                         string,
--         OtherLactate                                    string,
--         UFRate                                          string,
--         DialysisFrequencyDesc                           string,
--         DialysisFrequencyValue                          string,
--         InactivateDate                                  string
--         )
--     ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
--     STORED AS TEXTFILE
--     LOCATION {input_path}.PatientDialysisRxHemo.csv
-- ;

-- DROP TABLE IF EXISTS PatientDialysisRxPD;
-- CREATE EXTERNAL TABLE PatientDialysisRxPD (
--         AnalyticRowIDNumber                             string,
--         ClinicOrganizationIDNumber                      string,
--         PatientDataAnalyticRowIDNumber                  string,
--         PatientDialysisPrescriptionAnalyticRowIDNumber  string,
--         PatientDialysisHeaderIDNumber                   string,
--         TherapyTime                                     string,
--         NumberOfExchanges                               string,
--         TotalVolumeExchangeSolution                     string,
--         FullDrainEvery                                  string,
--         NumberOfCycles                                  string,
--         CyclerType                                      string,
--         TidalPercent                                    string,
--         UFperCycle                                      string,
--         TotalUF                                         string,
--         ChangeSolutionProtocol                          string,
--         TotalVolumeCyclerSolution                       string,
--         FinalFillVolume                                 string,
--         FinalFillExchangeIDNumber                       string,
--         InactivateDate                                  string
--         )
--     ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
--     STORED AS TEXTFILE
--     LOCATION {input_path}.PatientDialysisRxPD.csv
-- ;

-- DROP TABLE IF EXISTS PatientDialysisRxPDExchanges;
-- CREATE EXTERNAL TABLE PatientDialysisRxPDExchanges (
--         AnalyticRowIDNumber                             string,
--         ClinicOrganizationIDNumber                      string,
--         PatientDataAnalyticRowIDNumber                  string,
--         PatientDialysisPrescriptionAnalyticRowIDNumber  string,
--         PatientDialysisPDExchangesIDNumber              string,
--         PatientDialysisHeaderIDNumber                   string,
--         ExchangeNumber                                  string,
--         ExchangeDialysisPeriod                          string,
--         DialysisExchangeMechanism                       string,
--         ExchangeTime                                    string,
--         ExchangeSolution                                string,
--         LowCalcium                                      string,
--         ExchangeVolume                                  string,
--         FillVolume                                      string,
--         DwellTime                                       string,
--         DrainTime                                       string,
--         AddedDate                                       string,
--         EditDate                                        string,
--         SolutionType                                    string,
--         InactivateDate                                  string
--         )
--     ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
--     STORED AS TEXTFILE
--     LOCATION {input_path}.PatientDialysisRxPDExchanges.csv
-- ;

-- DROP TABLE IF EXISTS PatientEvent;
-- CREATE EXTERNAL TABLE PatientEvent (
--         AnalyticRowIDNumber             string,
--         ClinicOrganizationIDNumber      string,
--         PatientDataAnalyticRowIDNumber  string,
--         PatientEventIDNumber            string,
--         PatientIDNumber                 string,
--         EventDate                       string,
--         EventType                       string,
--         AddedDate                       string,
--         EditDate                        string,
--         DeletedDate                     string,
--         ResultIDNumber                  string,
--         PatientVascularAccessIDNumber   string,
--         PatientInfectionIDNumber        string,
--         InactivateDate                  string,
--         AnalyticDOS                     string
--         )
--     ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
--     STORED AS TEXTFILE
--     LOCATION {input_path}.PatientEvent.csv
-- ;

-- DROP TABLE IF EXISTS PatientFluidWeightManagement;
-- CREATE EXTERNAL TABLE PatientFluidWeightManagement (
--         AnalyticRowIDNumber                string,
--         ClinicOrganizationIDNumber         string,
--         PatientDataAnalyticRowIDNumber     string,
--         CklResultID                        string,
--         PatientIDNumber                    string,
--         AnalyticDOS                        string,
--         SodiumEducationDate                string,
--         AddedDate                          string,
--         EditDate                           string,
--         SodiumEducationReceived            string,
--         SodiumProfilingRxed                string,
--         ConstantDialysateSodiumRxed        string,
--         DialysateSodiumOver138             string,
--         PostDialysisWgtAssessmentDate      string,
--         PostDialysisTargetWgtRxed          string,
--         HomeBPProvided                     string,
--         HomeBPStatus                       string,
--         DryWeightRxed                      string,
--         HasEdema                           string,
--         EchocardiogramDate                 string,
--         HasAbnormalBreathSounds            string,
--         HasLeftVentricularHypertrophy      string,
--         LeftVentricularHypertrophyChanged  string,
--         InactivateDate                     string
--         )
--     ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
--     STORED AS TEXTFILE
--     LOCATION {input_path}.PatientFluidWeightManagement.csv
-- ;

-- DROP TABLE IF EXISTS PatientHeightHistory;
-- CREATE EXTERNAL TABLE PatientHeightHistory (
--         AnalyticRowIDNumber             string,
--         ClinicOrganizationIDNumber      string,
--         PatientDataAnalyticRowIDNumber  string,
--         AnalyticDOS                     string,
--         PatientHeightHistoryIDNumber    string,
--         HeightFeet                      string,
--         HeightInches                    string,
--         TotalHeight                     string,
--         TotalHeightUOM                  string,
--         PatientIDNumber                 string,
--         DoubleAmputee                   string,
--         AddedDate                       string,
--         EditDate                        string,
--         MeasureDate                     string,
--         Unstable                        string,
--         InactivateDate                  string
--         )
--     ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
--     STORED AS TEXTFILE
--     LOCATION {input_path}.PatientHeightHistory.csv
-- ;

-- DROP TABLE IF EXISTS PatientInfection;
-- CREATE EXTERNAL TABLE PatientInfection (
--         AnalyticRowIDNumber                string,
--         ClinicOrganizationIDNumber         string,
--         PatientDataAnalyticRowIDNumber     string,
--         PatientInfectionIDNumber           string,
--         PatientIDNumber                    string,
--         SuspectedInfectionStartDate        string,
--         ConfirmationDate                   string,
--         InfectionSourceType                string,
--         PatientVascularAccessIDNumber      string,
--         PrimaryLocationOther               string,
--         IdentificationType                 string,
--         Symptom_NONE                       string,
--         Symptom_AccessSite                 string,
--         Symptom_Fever_37_8c                string,
--         Symptom_Chills                     string,
--         Symptom_BP                         string,
--         Symptom_Mental                     string,
--         Symptom_Wound                      string,
--         Symptom_Cellulitis                 string,
--         Symptom_Respiratory                string,
--         EndDate                            string,
--         InfectionRequiredHospitalization   string,
--         HospitalizationDate                string,
--         DeathRelatedToInfection            string,
--         PDEffluentCellCountsDifferential   string,
--         AddedDate                          string,
--         EditDate                           string,
--         InactivateDate                     string,
--         AnalyticDOS                        string,
--         LossOfVascularAccess               string,
--         AccessSiteNHSN_Fistula             string,
--         AccessSiteNHSN_Graft               string,
--         AccessSiteNHSN_TunnCentralLine     string,
--         AccessSiteNHSN_NonTunnCentralLine  string,
--         AccessSiteNHSN_OtherAccessDevice   string
--         )
--     ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
--     STORED AS TEXTFILE
--     LOCATION {input_path}.PatientInfection.csv
-- ;

-- DROP TABLE IF EXISTS PatientInfection;
-- CREATE EXTERNAL TABLE PatientInfection_LabOrganism (
--         AnalyticRowIDNumber                        string,
--         ClinicOrganizationIDNumber                 string,
--         PatientDataAnalyticRowIDNumber             string,
--         AnalyticDOS                                string,
--         PatientIDNumber                            string,
--         PatientInfection_LabOrganismIDNumber       string,
--         PatientInfection_LabResultCultureIDNumber  string,
--         OrganismCode                               string,
--         OrganismRank                               string,
--         AddedDate                                  string,
--         EditDate                                   string,
--         InactivateDate                             string
--         )
--     ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
--     STORED AS TEXTFILE
--     LOCATION {input_path}.PatientInfection.csv
-- ;

-- DROP TABLE IF EXISTS PatientInfection;
-- CREATE EXTERNAL TABLE PatientInfection_LabOrganismDrug (
--         AnalyticRowIDNumber                       string,
--         ClinicOrganizationIDNumber                string,
--         PatientDataAnalyticRowIDNumber            string,
--         AnalyticDOS                               string,
--         PatientIDNumber                           string,
--         PatientInfection_LabOrganismDrugIDNumber  string,
--         PatientInfection_LabOrganismIDNumber      string,
--         DrugCode                                  string,
--         SusceptibilityCode                        string,
--         AddedDate                                 string,
--         EditDate                                  string,
--         InactivateDate                            string
--         )
--     ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
--     STORED AS TEXTFILE
--     LOCATION {input_path}.PatientInfection.csv
-- ;

-- DROP TABLE IF EXISTS PatientInfection;
-- CREATE EXTERNAL TABLE PatientInfection_LabResultCulture (
--         AnalyticRowIDNumber                        string,
--         ClinicOrganizationIDNumber                 string,
--         PatientDataAnalyticRowIDNumber             string,
--         PatientInfection_LabResultCultureIDNumber  string,
--         PatientInfectionIDNumber                   string,
--         PatientIDNumber                            string,
--         ResultedDate                               string,
--         ResultType                                 string,
--         CollectedDate                              string,
--         CultureSource                              string,
--         CultureResult                              string,
--         Organisms                                  string,
--         AddedDate                                  string,
--         EditDate                                   string,
--         InactivateDate                             string,
--         AnalyticDOS                                string
--         )
--     ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
--     STORED AS TEXTFILE
--     LOCATION {input_path}.PatientInfection.csv
-- ;

-- DROP TABLE IF EXISTS PatientInfection;
-- CREATE EXTERNAL TABLE PatientInfection_Medication (
--         AnalyticRowIDNumber                  string,
--         ClinicOrganizationIDNumber           string,
--         PatientDataAnalyticRowIDNumber       string,
--         PatientInfection_MedicationIDNumber  string,
--         PatientInfectionIDNumber             string,
--         PatientPrescriptionMedsIDNumber      string,
--         PatientIDNumber                      string,
--         AddedDate                            string,
--         EditDate                             string,
--         InactivateDate                       string
--         )
--     ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
--     STORED AS TEXTFILE
--     LOCATION {input_path}.PatientInfection.csv
-- ;

-- DROP TABLE IF EXISTS PatientInstabilityHistory;
-- CREATE EXTERNAL TABLE PatientInstabilityHistory (
--         AnalyticRowIDNumber                string,
--         ClinicOrganizationIDNumber         string,
--         PatientDataAnalyticRowIDNumber     string,
--         PatientInstabilityHistoryIDNumber  string,
--         PatientIDNumber                    string,
--         EventDate                          string,
--         Event                              string,
--         EventNotes                         string,
--         DateOfCIA                          string,
--         CklResultID                        string,
--         RelationID                         string,
--         RelationType                       string,
--         AddedDate                          string,
--         EditDate                           string,
--         Deleted                            string,
--         DeletedDate                        string,
--         InactivateDate                     string
--         )
--     ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
--     STORED AS TEXTFILE
--     LOCATION {input_path}.PatientInstabilityHistory.csv
-- ;

-- DROP TABLE IF EXISTS PatientMasterScheduleHeader;
-- CREATE EXTERNAL TABLE PatientMasterScheduleHeader (
--         AnalyticRowIDNumber             string,
--         PatientDataAnalyticRowIDNumber  string,
--         ClinicOrganizationIDNumber      string,
--         PatientMasterScheduleHeaderID   string,
--         PatientIDNumber                 string,
--         PatientIDNumber                 string,
--         ClinicID                        string,
--         ScheduleType                    string,
--         StartDate                       string,
--         EndDate                         string,
--         StartTime                       string,
--         EndTime                         string,
--         ScheduleShift                   string,
--         PatientStatus                   string,
--         PatientStatus                   string,
--         PatientStatus_Original          string,
--         StatusIsActive                  string,
--         TxTypeIDNumber                  string,
--         ReoccurrenceType                string,
--         RecurEvery                      string,
--         Mon                             string,
--         Tue                             string,
--         Wed                             string,
--         Thu                             string,
--         Fri                             string,
--         Sat                             string,
--         Sun                             string,
--         Disabled                        string,
--         AddedDate                       string,
--         EditDate                        string,
--         ClinicScheduleID                string,
--         OriginalDate                    string,
--         ReasonTransferred               string,
--         ReferringPhysician              string,
--         NetworkEvent                    string,
--         InactivateDate                  string,
--         SessionsPerWeek                 string
--         )
--     ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
--     STORED AS TEXTFILE
--     LOCATION {input_path}.PatientMasterScheduleHeader.csv
-- ;

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
    LOCATION {input_path}.PatientMedAdministered.csv
;

-- DROP TABLE IF EXISTS PatientMedNotGiven;
-- CREATE EXTERNAL TABLE PatientMedNotGiven (
--         AnalyticRowIDNumber                      string,
--         PatientDataAnalyticRowIDNumber           string,
--         ClinicOrganizationIDNumber               string,
--         RunMedsActualAdministeredDeleteIDNumber  string,
--         PatientAdministeredMedsIDNumber          string,
--         PatientIDNumber                          string,
--         AnalyticDOS                              string,
--         RunIDNumber                              string,
--         MedIDNumber                              string,
--         Medication                               string,
--         Route                                    string,
--         Dose                                     string,
--         Frequency                                string,
--         Duration                                 string,
--         Prescription                             string,
--         StartDate                                string,
--         EndDate                                  string,
--         PRN                                      string,
--         AdminDuringRun                           string,
--         AddedDate                                string,
--         EditDate                                 string,
--         AdministrationTime                       string,
--         RunJustification                         string,
--         MedPrescIDNumber                         string,
--         TotalDoses                               string,
--         DosesGiven                               string,
--         UseDoses                                 string,
--         DoseQty                                  string,
--         DoseUnit                                 string,
--         DoseRoute                                string,
--         DoseFreq                                 string,
--         DoseFreqMonday                           string,
--         DoseFreqTuesday                          string,
--         DoseFreqWednesday                        string,
--         DoseFreqThursday                         string,
--         DoseFreqFriday                           string,
--         DoseFreqSaturday                         string,
--         DoseFreqSunday                           string,
--         Hold                                     string,
--         HoldUntil                                string,
--         LastDoseOn                               string,
--         PatientNotTaking                         string,
--         PhysicianIDNumber                        string,
--         FixedWeekInterval                        string,
--         DateNextDose                             string,
--         DateDoseLastGiven                        string,
--         AdministrationCode                       string,
--         ProcedureCode                            string,
--         MedicationCode                           string,
--         PatientProvided                          string,
--         ESRDRelated                              string,
--         PatientPrescriptionMedsParentID          string,
--         InactivateDate                           string,
--         SelfAdmin                                string,
--         AdminDuringFacility                      string,
--         BulkSupply                               string,
--         TreatmentIDNumber                        string,
--         ICD10                                    string,
--         GENPRODUCT_ID                            string,
--         DRUG_ID                                  string
--         )
--     ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
--     STORED AS TEXTFILE
--     LOCATION {input_path}.PatientMedNotGiven.csv
-- ;

-- DROP TABLE IF EXISTS PatientMedPrescription;
-- CREATE EXTERNAL TABLE PatientMedPrescription (
--         AnalyticRowIDNumber              string,
--         PatientDataAnalyticRowIDNumber   string,
--         ClinicOrganizationIDNumber       string,
--         PatientAdministeredMedsIDNumber  string,
--         PatientIDNumber                  string,
--         MedIDNumber                      string,
--         Medication                       string,
--         Prescription                     string,
--         Hold                             string,
--         HoldUntil                        string,
--         StartDate                        string,
--         EndDate                          string,
--         PRN                              string,
--         AdminDuringRun                   string,
--         AddedDate                        string,
--         EditDate                         string,
--         LastDoseOn                       string,
--         PatientNotTaking                 string,
--         RunJustification                 string,
--         PhysicianIDNumber                string,
--         TotalDoses                       string,
--         DosesGiven                       string,
--         UseDoses                         string,
--         DoseQty                          string,
--         DoseUnit                         string,
--         DoseRoute                        string,
--         DoseFreq                         string,
--         DoseFreqMonday                   string,
--         DoseFreqTuesday                  string,
--         DoseFreqWednesday                string,
--         DoseFreqThursday                 string,
--         DoseFreqFriday                   string,
--         DoseFreqSaturday                 string,
--         DoseFreqSunday                   string,
--         FixedWeekInterval                string,
--         DateNextDose                     string,
--         DateDoseLastGiven                string,
--         AdministrationCode               string,
--         PatientProvided                  string,
--         PatientPrescriptionMedsParentID  string,
--         ProtocolMed                      string,
--         PatientMasterScheduleHeaderID    string,
--         UpdateReason                     string,
--         ESRDRelated                      string,
--         AnalyticDOS                      string,
--         InactivateDate                   string,
--         MonthlyDose                      string,
--         SelfAdmin                        string,
--         AdminDuringFacility              string,
--         BulkSupply                       string,
--         ICD10                            string,
--         GENPRODUCT_ID                    string,
--         DRUG_ID                          string,
--         DoNOTSubstitute                  string,
--         StartingDosesGiven               string,
--         DoseStrength                     string,
--         DoseForm                         string,
--         EPrescribed                      string,
--         EPrescribedQuantity              string,
--         EPrescribedRefill                string,
--         EPrescribedDate                  string
--         )
--     ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
--     STORED AS TEXTFILE
--     LOCATION {input_path}.PatientMedPrescription.csv
-- ;

-- DROP TABLE IF EXISTS PatientStatusHistory;
-- CREATE EXTERNAL TABLE PatientStatusHistory (
--         AnalyticRowIDNumber             string,
--         ClinicOrganizationIDNumber      string,
--         PatientDataAnalyticRowIDNumber  string,
--         PatientStatusHistoryIDNumber    string,
--         PatientIDNumber                 string,
--         Status_Original                 string,
--         PatientStatus                   string,
--         StatusIsActive                  string,
--         DateLastStatusChange            string,
--         AddedDate                       string,
--         EditDate                        string,
--         EditDateHistory                 string,
--         InactivateDate                  string,
--         AnalyticDOS                     string
--         )
--     ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
--     STORED AS TEXTFILE
--     LOCATION {input_path}.PatientStatusHistory.csv
-- ;

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
    LOCATION {input_path}.ProblemList.csv
;

-- DROP TABLE IF EXISTS SodiumUFProfile;
-- CREATE EXTERNAL TABLE SodiumUFProfile (
--         AnalyticRowIDNumber  string,
--         ProfileIDNumber      string,
--         ProfileNumber        string,
--         ProfileType          string,
--         BeginingLevel        string,
--         EndingLevel          string,
--         AddedDate            string,
--         EditDate             string,
--         InactivateDate       string
--         )
--     ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
--     STORED AS TEXTFILE
--     LOCATION {input_path}.SodiumUFProfile.csv
-- ;

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
        Revised Info           string,
        Differencees           string,
        InactivateDate         string
        )
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
    STORED AS TEXTFILE
    LOCATION {input_path}.LabIDList.csv
;

-- DROP TABLE IF EXISTS Medication;
-- CREATE EXTERNAL TABLE Medication (
--         MedIDNumber     string,
--         AddedDate       string,
--         LastEditDate    string,
--         InactivateDate  string
--         )
--     ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
--     STORED AS TEXTFILE
--     LOCATION {input_path}.Medication.csv
-- ;

-- DROP TABLE IF EXISTS MedicationGroup;
-- CREATE EXTERNAL TABLE MedicationGroup (
--         MedicationGroupIDNumber        string,
--         MedicationGroupName            string,
--         MedicationName                 string,
--         CrownMedicationName            string,
--         Route                          string,
--         Original_MedGroupIDDetailNumb  string,
--         Original_MedGroupIDNumber      string,
--         AddedDate                      string,
--         LastEditDate                   string,
--         InactivateDate                 string
--         )
--     ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
--     STORED AS TEXTFILE
--     LOCATION {input_path}.MedicationGroup.csv
-- ;

-- DROP TABLE IF EXISTS StateGeo;
-- CREATE EXTERNAL TABLE StateGeo (
--         StateAbbreviation           string,
--         StateName                   string,
--         StateFIPScode               string,
--         DivisionName                string,
--         CensusBureauDivisionNumber  string,
--         RegionName                  string,
--         CensusBureauRegionNumber    string
--         )
--     ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
--     STORED AS TEXTFILE
--     LOCATION {input_path}.StateGeo.csv
-- ;

-- DROP TABLE IF EXISTS ZipGeo;
-- CREATE EXTERNAL TABLE ZipGeo (
--         GEO_ID              string,
--         ZipCode             string,
--         SUMLEVEL            string,
--         GEO_NAME            string,
--         TotalPopulation     string,
--         UrbanPopulation     string,
--         InsideUrbanArea     string,
--         InsideUrbanCluster  string,
--         RuralPopulation     string,
--         NA                  string,
--         RuralVsUrban        string
--         )
--     ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
--     STORED AS TEXTFILE
--     LOCATION {input_path}.ZipGeo.csv
-- ;
