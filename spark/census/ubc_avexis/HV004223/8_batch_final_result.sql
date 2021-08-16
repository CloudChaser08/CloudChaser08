SELECT
    obfuscate_hvid(hvid, 'hvidNVRARD')  AS HVID,
    uniquepatientnumber AS GUID,
    uniquerecordnumber AS UniqueRecordNumber,
    ubcapp as UBCApp,
    ubcdb as UBCDB,
    ubcprogram as UBCProgram
from batch_raw_result
