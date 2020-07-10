DROP TABLE IF EXISTS clean_advancedirective;
CREATE TABLE clean_advancedirective AS (
    SELECT analyticrowidnumber,
           clinicorganizationidnumber,
           patientdataanalyticrowidnumber,
           extract_date(substring(analyticdos, 1, 10), '%Y-%m-%d')    as analyticdos,
           extract_date(substring(inactivatedate, 1, 10), '%Y-%m-%d') as inactivatedate,
           advancedirectiveidnumber,
           patientidnumber,
           resuscitationcode,
           livingwilldocument,
           healthcarepowerofattorneydocument,
           surrogatedecisionmakerdocument,
           surrogatedecisionmakerpersonandrelationship,
           dnrform,
           molstdocument,
           mostdocument,
           polstdocument,
           postdocument,
           otherdocument,
           advdircomments,
           extract_date(substring(addeddate, 1, 10), '%Y-%m-%d')      as addeddate,
           extract_date(substring(editdate, 1, 10), '%Y-%m-%d')       as editdate
    FROM advancedirective
);


