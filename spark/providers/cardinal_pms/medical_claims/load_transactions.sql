DROP TABLE IF EXISTS transactional_claim;
CREATE EXTERNAL TABLE transactional_claim (
    billproviderid                  string,
    billprovideridqualifier         string,
    billprovidername                string,
    billprovidernpid                string,
    billprovidertaxonomycode        string,
    diagnosiseight                  string,
    diagnosisfive                   string,
    diagnosisfour                   string,
    diagnosisseven                  string,
    diagnosissix                    string,
    diagnosisthree                  string,
    diagnosistwo                    string,
    edifile_id                      string,
    facilitycode                    string,
    groupname                       string,
    groupnumber                     string,
    id                              string,
    insurancetype                   string,
    patientaccountnumber            string,
    patientdob                      string,
    patientfirstname                string,
    patientgender                   string,
    patientlastname                 string,
    patientpolicynumber             string,
    patientrelationshiptoinsured    string,
    payerid                         string,
    payername                       string,
    payerresponsibility             string,
    principaldiagnosis              string,
    priorauthorizationnumber        string,
    priorpayercount                 string,
    priorpayerpaidamount            string,
    submitreasoncode                string,
    submittedchargetotal            string,
    subscriberfirstname             string,
    subscriberid                    string,
    subscriberidqualifier           string,
    subscriberlastname              string,
    subscriberpolicynumber          string,
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
    "separatorChar" = "|"
)
STORED AS TEXTFILE
LOCATION {input_path_claim}
;

DROP TABLE IF EXISTS transactional_claim_file;
CREATE EXTERNAL TABLE transactional_claim_file (
    bhtcontrolnumber                string,
    bhtdatetime                     string,
    gscontrolnumber                 string,
    gsdatetime                      string,
    id                              string,
    importsourceid                  string,
    isacontrolnumber                string,
    isadatetime                     string,
    purposecode                     string,
    submitterentitytypequalifier    string,
    submitteridentificationcode     string,
    submitteridcodequalifier        string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
    "separatorChar" = "|"
)
STORED AS TEXTFILE
LOCATION {input_path_claim_file}
;

DROP TABLE IF EXISTS transactional_claim_lines;
CREATE EXTERNAL TABLE transactional_claim_lines (
    dateservicestart                string,
    ediclaim_id                     string,
    id                              string,
    linesequencenumber              string,
    linkeddiagnosisfour             string,
    linkeddiagnosisone              string,
    linkeddiagnosisthree            string,
    linkeddiagnosistwo              string,
    orderingproviderid              string,
    orderingprovideridqualifier     string,
    orderingprovidername            string,
    orderingprovidernpid            string,
    procedurecode                   string,
    procedurecodequalifier          string,
    proceduremodifierfour           string,
    proceduremodifierone            string,
    proceduremodifierthree          string,
    proceduremodifiertwo            string,
    providerlinecontrolnumber       string,
    referringproviderid             string,
    referringprovideridqualifier    string,
    referringprovidername           string,
    renderingproviderid             string,
    renderingprovideridqualifier    string,
    renderingprovidername           string,
    renderingprovidernpid           string,
    renderingprovidertaxonomycode   string,
    servicefacilityaddress          string,
    servicefacilitycity             string,
    servicefacilityid               string,
    servicefacilityidqualifier      string,
    servicefacilityname             string,
    servicefacilitystate            string,
    servicefacilityzip              string,
    submittedcharge                 string,
    submittedunits                  string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
    "separatorChar" = "|"
)
STORED AS TEXTFILE
LOCATION {input_file_claim_lines}
;
