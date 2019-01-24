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
    WITH SERDEPROPERTIES (
        "separatorChar" = "|"
    )
    STORED AS TEXTFILE
    LOCATION '{input_path}patientdata/'
    ;

DROP TABLE IF EXISTS patientmasterscheduleheader;
CREATE EXTERNAL TABLE patientmasterscheduleheader (
        analyticrowidnumber             string,
        patientdataanalyticrowidnumber  string,
        clinicorganizationidnumber      string,
        patientmasterscheduleheaderid   string,
        patientidnumber                 string,
        clinicid                        string,
        scheduletype                    string,
        startdate                       string,
        enddate                         string,
        starttime                       string,
        endtime                         string,
        scheduleshift                   string,
        patientstatus                   string,
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
    WITH SERDEPROPERTIES (
        "separatorChar" = "|"
    )
    STORED AS TEXTFILE
    LOCATION '{input_path}patientmasterscheduleheader/'
    ;

DROP TABLE IF EXISTS advancedirective;
CREATE TABLE advancedirective (
        analyticrowidnumber                             string,
        clinicorganizationidnumber                      string,
        patientdataanalyticrowidnumber                  string,
        analyticdos                                     string,
        inactivatedate                                  string,
        advancedirectiveidnumber                        string,
        patientidnumber                                 string,
        resuscitationcode                               string,
        livingwilldocument                              string,
        healthcarepowerofattorneydocument               string,
        surrogatedecisionmakerdocument                  string,
        surrogatedecisionmakerpersonandrelationship     string,
        dnrform                                         string,
        molstdocument                                   string,
        mostdocument                                    string,
        polstdocument                                   string,
        postdocument                                    string,
        otherdocument                                   string,
        advdircomments                                  string,
        addeddate                                       string,
        editdate                                        string
    )
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
    WITH SERDEPROPERTIES (
        "separatorChar" = "|"
    )
    STORED AS TEXTFILE
    LOCATION '{input_path}advancedirective/'
    ;
