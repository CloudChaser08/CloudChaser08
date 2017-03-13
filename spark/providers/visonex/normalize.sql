-- diagnosis
select patientidnumber, icd10, admissiondate
from visonex.hospitalization
UNION
select patientidnumber, icd9, admissiondate
from visonex.hospitalization
UNION
select patientidnumber, icd10, /*what date*/
from visonex.immunization
UNION
select patientidnumber, icd10, examproceduredate
from visonex.PatientAccess_ExamProc
UNION
select patientidnumber, diagnosticcode, examproceduredate
from visonex.PatientAccess_ExamProc
UNION
select patientidnumber, justification, thedate
from visonex.labpanelsdrawn
UNION
select patientidnumber, diagnosiscode, receiveddate
from visonex.labresult
UNION
select patientidnumber, icd10, receiveddate
from visonex.labresult
UNION
select patientidnumber, diagnosiscode, diagnosisdate
from visonex.patientdiagcodes
UNION
select patientidnumber, icd10, diagnosisdate
from visonex.patientdiagcodes
UNION
select patientidnumber, icd10, startdate
from visonex.patientmedadministered
UNION
select patientidnumber, runjustification, startdate
from visonex.patientmedprescription
UNION
select patientidnumber, icd9, startdate
from visonex.problemlist
UNION
select patientidnumber, icd10, startdate
from visonex.problemlist
UNION
(select b.patientidnumber, a.icd9diagnosiscode, b.receiveddate
from visonex.labidlist a
    inner join visonex.labresult b on a.universalserviceid=b.universalserviceid and a.observationidentifier=b.observationidentifier and a.testname=b.testname
)
;

-- procedure
select patientidnumber, procedurecode, examproceduredate
from visonex.PatientAccess_ExamProc
UNION
select patientidnumber, procedurecode, receiveddate
from visonex.labresult
UNION
select patientidnumber, procedurecode, thedate
from visonex.labpanelsdrawn
UNION
select patientidnumber, procedurecode, startdate
from visonex.patientmedadministered
UNION
select patientidnumber, medicationcode, startdate
from visonex.patientmedadministered
UNION
(select b.patientidnumber, a.cptcode, b.receiveddate
from visonex.labidlist a
    inner join visonex.labresult b on a.universalserviceid=b.universalserviceid and a.observationidentifier=b.observationidentifier and a.testname=b.testname
)
;

-- ndc
select patientidnumber, procedurecode, startdate
from visonex.patientmedadministered

/* Notes:

tables needed:
  hospitalization
  immunization
  PatientAccess_ExamProc
  labpanelsdrawn
  labresult
  patientdiagcodes
  patientmedadministered
  problemlist
  labidlist
  labresult

*/
