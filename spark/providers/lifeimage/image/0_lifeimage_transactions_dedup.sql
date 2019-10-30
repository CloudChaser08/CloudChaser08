SELECT 
    txn.bodypartexamined,
    txn.manufacturer,
    txn.manufacturermodelname,
    txn.modality,
    txn.patientage,
    txn.patientsex,
    txn.patientweight,
    txn.referringphysicianname,
    txn.requestingphysician,
    txn.seriesinstanceuid,
    COUNT(DISTINCT txn.sopinstanceuid) AS sopinstanceuid_cnt ,
    txn.studydate,
    txn.studyinstanceuid,
    MAX(input_file_name) AS input_file_name,    
    MAX(hvjoinkey) AS hvjoinkey

FROM txn
--WHERE txn.seriesinstanceuid = '1.2.840.113619.2.411.3.3322564632.439.1520318770.223'
WHERE 
    (
          SUBSTR(UPPER(countryofresidence),1,2) = 'US'
       OR SUBSTR(UPPER(countryofresidence),1,6) = 'UNITED'
       OR LENGTH(COALESCE(countryofresidence,'')) = 0 
   )
   
    GROUP BY
        txn.bodypartexamined,
        txn.manufacturer,
        txn.manufacturermodelname,
        txn.modality,
        txn.patientage,
        txn.patientsex,
        txn.patientweight,
        txn.referringphysicianname,
        txn.requestingphysician,
        txn.seriesinstanceuid,
        txn.studydate,
        txn.studyinstanceuid
    ORDER BY
        txn.bodypartexamined,
        txn.manufacturer,
        txn.manufacturermodelname,
        txn.modality,
        txn.patientage,
        txn.patientsex,
        txn.patientweight,
        txn.referringphysicianname,
        txn.requestingphysician,
        txn.seriesinstanceuid,
        txn.studydate,
        txn.studyinstanceuid




