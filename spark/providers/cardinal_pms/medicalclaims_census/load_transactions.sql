DROP TABLE IF EXISTS transactional_cardinal_pms_temp;
DROP TABLE IF EXISTS transactional_cardinal_pms;

CREATE EXTERNAL TABLE transactional_cardinal_pms_temp (
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
    claim_id                        string,
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
    bhtcontrolnumber                string,
    bhtdatetime                     string,
    gscontrolnumber                 string,
    gsdatetime                      string,
    claim_file_id                   string,
    importsourceid                  string,
    isacontrolnumber                string,
    isadatetime                     string,
    purposecode                     string,
    submitterentitytypequalifier    string,
    submitteridentificationcode     string,
    submitteridcodequalifier        string,
    dateservicestart                string,
    ediclaim_id                     string,
    claim_lines_id                  string,
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
    submittedunits                  string,
    hvid                            string,
    tenant_id                       string,
    hvJoinKey                       string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
    "separatorChar" = "|"
)
STORED AS TEXTFILE
LOCATION {input_path}
;
