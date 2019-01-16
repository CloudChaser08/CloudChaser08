DROP TABLE IF EXISTS clean_patientdata;
CREATE TABLE clean_patientdata (
        analyticrowidnumber                     string,
        clinicorganizationidnumber              string,
        defaultclinicidnumber                   string,
        patientidnumber                         string,
        patientid                               string,
        hvid                                    string,
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
        datefirstdialysis                       date,
        laststatuschangedate                    date,
        resuscitationcode                       string,
        advdirresuscitationcode                 string,
        advdirpowerofatty                       string,
        advdirlivingwill                        string,
        advdirpatientdeclines                   string,
        advrevdate                              date,
        tribecode                               string,
        inactivatedate                          date,
        age                                     string,
        monthsindialysis                        string,
        transplantwaitlist                      string,
        medicalcoveragemedicare                 string,
        medicalcoveragemedicareeffectivedate    date,
        masterpatientidnumber                   string,
        datefirstdialysiscurrentunit            date
        )
    ;

DROP TABLE IF EXISTS clean_patientmasterscheduleheader;
CREATE TABLE clean_patientmasterscheduleheader (
        analyticrowidnumber             string,
        patientdataanalyticrowidnumber  string,
        clinicorganizationidnumber      string,
        patientmasterscheduleheaderid   string,
        patientidnumber                 string,
        patientidnumber2                string,
        clinicid                        string,
        scheduletype                    string,
        startdate                       date,
        enddate                         date,
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
        addeddate                       date,
        editdate                        date,
        clinicscheduleid                string,
        originaldate                    date,
        reasontransferred               string,
        referringphysician              string,
        networkevent                    string,
        inactivatedate                  date,
        sessionsperweek                 string
        )
    ;

