DROP TABLE IF EXISTS cardinal_dcoa_transactions;
CREATE EXTERNAL TABLE cardinal_dcoa_transactions (
    SKUtilizationKey                string,
    SKClientTypeKey                 string,
    AcqCost                         string,
    QTY                             string,
    ExtendedFee                     string,
    Revenue                         string,
    ExtendedCost                    string,
    UOMQTY                          string,
    DispenseDttm                    string,
    HDC                             string,
    EffectiveStartDate              string,
    DischargeDttm                   string,
    PatientType                     string,
    ClientPatientId                 string,
    Outlier                         string,
    PhysicianCode                   string,
    PrescribingPhysicianCode        string,
    NDC                             string,
    MonthlyPatientDays              string,
    Discharge                       string,
    DischargePatientDays            string,
    TotalPatientDays                string,
    ClientName                      string,
    Address1                        string,
    DrgCode                         string,
    ServiceAreaDescription          string,
    MasterServiceAreaDescription    string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
    "separatorChar" = "|"
)
STORED AS TEXTFILE
LOCATION {input_path}
;
