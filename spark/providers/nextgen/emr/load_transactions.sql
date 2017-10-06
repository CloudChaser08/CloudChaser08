DROP TABLE IF EXISTS new_raw_data;
CREATE EXTERNAL TABLE new_raw_data (
        column1                    string,
        column2                    string,
        column3                    string,
        column4                    string,
        column5                    string,
        column6                    string,
        column7                    string,
        column8                    string,
        column9                    string,
        column10                   string,
        column11                   string,
        column12                   string,
        column13                   string,
        column14                   string,
        column15                   string,
        column16                   string,
        column17                   string,
        column18                   string,
        column19                   string,
        column20                   string,
        column21                   string,
        column22                   string,
        column23                   string,
        column24                   string,
        column25                   string,
        column26                   string,
        column27                   string,
        column28                   string,
        column29                   string,
        column30                   string,
        column31                   string,
        column32                   string,
        column33                   string,
        column34                   string,
        column35                   string,
        column36                   string,
        column37                   string
    )
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
    WITH SERDEPROPERTIES (
        'separatorChar' = '|'
        )
    STORED AS TEXTFILE
    LOCATION {input_path}
    ;

set hive.exec.dynamic.partition.mode=nonstrict;
DROP TABLE IF EXISTS new_raw_data_local;
CREATE TABLE new_raw_data_local (
        column1                    string,
        column2                    string,
        column3                    string,
        column4                    string,
        column5                    string,
        column6                    string,
        column7                    string,
        column8                    string,
        column9                    string,
        column10                   string,
        column11                   string,
        column12                   string,
        column13                   string,
        column14                   string,
        column15                   string,
        column16                   string,
        column17                   string,
        column18                   string,
        column19                   string,
        column20                   string,
        column21                   string,
        column22                   string,
        column23                   string,
        column24                   string,
        column25                   string,
        column26                   string,
        column27                   string,
        column28                   string,
        column29                   string,
        column30                   string,
        column31                   string,
        column32                   string,
        column33                   string,
        column34                   string,
        column35                   string,
        column36                   string,
        column37                   string,
        input_file_name            string
    )
    PARTITIONED BY (tbl_type string)
    STORED AS PARQUET
;

INSERT INTO new_raw_data_local
SELECT *, input_file_name(), column4
FROM new_raw_data
DISTRIBUTE BY column2;

DROP TABLE IF EXISTS all_raw_data;
CREATE EXTERNAL TABLE all_raw_data (
        column1                    string,
        column2                    string,
        column3                    string,
        column4                    string,
        column5                    string,
        column6                    string,
        column7                    string,
        column8                    string,
        column9                    string,
        column10                   string,
        column11                   string,
        column12                   string,
        column13                   string,
        column14                   string,
        column15                   string,
        column16                   string,
        column17                   string,
        column18                   string,
        column19                   string,
        column20                   string,
        column21                   string,
        column22                   string,
        column23                   string,
        column24                   string,
        column25                   string,
        column26                   string,
        column27                   string,
        column28                   string,
        column29                   string,
        column30                   string,
        column31                   string,
        column32                   string,
        column33                   string,
        column34                   string,
        column35                   string,
        column36                   string,
        column37                   string
    )
    PARTITIONED BY (part_processdate string)
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
    WITH SERDEPROPERTIES (
        'separatorChar' = '|'
        )
    STORED AS TEXTFILE
    LOCATION {input_root_path}
    ;

DROP VIEW IF EXISTS demographics;
CREATE VIEW demographics AS
SELECT
    column1  as preambleformatcode,
    column2  as nextgengroupid,
    column3  as referencedatetime,
    column4  as postamblecategoryformat,
    column5  as datacapturedate,
    column6  as birthyear,
    column7  as birthmonth,
    column8  as gender,
    column9  as race,
    column10 as zip3,
    column11 as coveredbymedicarepartbflag,
    column12 as patientpseudonym,
    regexp_extract(input_file_name(), 'NG_LSSA_([^_]*)_[^\.]*.txt') as reportingenterpriseid,
    regexp_extract(input_file_name(), 'NG_LSSA_[^_]*_([^\.]*).txt') as recorddate,
    regexp_extract(input_file_name(), '(NG_LSSA_[^_]*_[^\.]*.txt)') as dataset
FROM all_raw_data
WHERE tbl_type = '0005.001';

DROP VIEW IF EXISTS encounter;
CREATE VIEW encounter AS
SELECT
    column1  as preambleformatcode,
    column2  as nextgengroupid,
    column3  as referencedatetime,
    column4  as postamblecategoryformat,
    column5  as encounterid,
    column6  as encounterdatetime,
    column7  as encountertype,
    column8  as encounterdescription,
    column9  as hcpzipcode,
    column10 as hcpprimarytaxonomy,
    regexp_extract(input_file_name(), 'NG_LSSA_([^_]*)_[^\.]*.txt') as reportingenterpriseid,
    regexp_extract(input_file_name(), 'NG_LSSA_[^_]*_([^\.]*).txt') as recorddate,
    regexp_extract(input_file_name(), '(NG_LSSA_[^_]*_[^\.]*.txt)') as dataset
FROM all_raw_data
WHERE tbl_type = '0007.001';

DROP VIEW IF EXISTS vitalsigns;
CREATE VIEW vitalsigns AS
SELECT
    column1  as preambleformatcode,
    column2  as nextgengroupid,
    column3  as referencedatetime,
    column4  as postamblecategoryformat,
    column5  as encounter_id,
    column6  as encounterdate,
    column7  as systolic,
    column8  as diastolic,
    column9  as pulserate,
    column10 as bmi,
    column11 as datadate,
    column12 as spo2dtl,
    column13 as spo2timingid,
    column14 as peakflow,
    column15 as peakflowtiming,
    column16 as tempdegf,
    column17 as respirationrate,
    column18 as haqscore,
    column19 as pain,
    column20 as bmipercent,
    column21 as heightdate,
    column22 as heightin,
    column23 as heightcm,
    column24 as heightft,
    column25 as weightkg,
    column26 as weightlb,
    regexp_extract(input_file_name, 'NG_LSSA_([^_]*)_[^\.]*.txt') as reportingenterpriseid,
    regexp_extract(input_file_name, 'NG_LSSA_[^_]*_([^\.]*).txt') as recorddate,
    regexp_extract(input_file_name, '(NG_LSSA_[^_]*_[^\.]*.txt)') as dataset
FROM new_raw_data_local
WHERE tbl_type = '0010.001';

DROP VIEW IF EXISTS lipidpanel;
CREATE VIEW lipidpanel AS
SELECT
    column1  as preambleformatcode,
    column2  as nextgengroupid,
    column3  as referencedatetime,
    column4  as postamblecategoryformat,
    column5  as encounter_id,
    column6  as encounterdate,
    column7  as datadatetime,
    column8  as ldl,
    column9  as hdl,
    column10 as triglycerides,
    column11 as totalcholesterol,
    regexp_extract(input_file_name, 'NG_LSSA_([^_]*)_[^\.]*.txt') as reportingenterpriseid,
    regexp_extract(input_file_name, 'NG_LSSA_[^_]*_([^\.]*).txt') as recorddate,
    regexp_extract(input_file_name, '(NG_LSSA_[^_]*_[^\.]*.txt)') as dataset
FROM new_raw_data_local
WHERE tbl_type = '0020.001';

DROP VIEW IF EXISTS allergy;
CREATE VIEW allergy AS
SELECT
    column1  as preambleformatcode,
    column2  as nextgengroupid,
    column3  as referencedatetime,
    column4  as postamblecategoryformat,
    column5  as encounter_id,
    column6  as encounterdate,
    column7  as onsetdate,
    column8  as allergencode,
    column9  as allergendescription,
    column10 as allergentype,
    column11 as allergentypedescription,
    column12 as resolveddate,
    column13 as reportedsymptoms,
    column14 as intolerenceind,
    regexp_extract(input_file_name, 'NG_LSSA_([^_]*)_[^\.]*.txt') as reportingenterpriseid,
    regexp_extract(input_file_name, 'NG_LSSA_[^_]*_([^\.]*).txt') as recorddate,
    regexp_extract(input_file_name, '(NG_LSSA_[^_]*_[^\.]*.txt)') as dataset
FROM new_raw_data_local
WHERE tbl_type = '0030.001';

DROP VIEW IF EXISTS substanceusage;
CREATE VIEW substanceusage AS
SELECT
    column1  as preambleformatcode,
    column2  as nextgengroupid,
    column3  as referencedatetime,
    column4  as postamblecategoryformat,
    column5  as encounter_id,
    column6  as encounterdate,
    column7  as substancecode,
    column8  as clinicalrecordtypecode,
    column9  as clinicalrecorddescription,
    column10 as datadate,
    column11 as emrcode,
    regexp_extract(input_file_name, 'NG_LSSA_([^_]*)_[^\.]*.txt') as reportingenterpriseid,
    regexp_extract(input_file_name, 'NG_LSSA_[^_]*_([^\.]*).txt') as recorddate,
    regexp_extract(input_file_name, '(NG_LSSA_[^_]*_[^\.]*.txt)') as dataset
FROM new_raw_data_local
WHERE tbl_type = '0040.001';

DROP VIEW IF EXISTS diagnosis;
CREATE VIEW diagnosis AS
SELECT
    column1  as preambleformatcode,
    column2  as nextgengroupid,
    column3  as referencedatetime,
    column4  as postamblecategoryformat,
    column5  as encounter_id,
    column6  as encounterdate,
    column7  as diagnosisdate,
    column8  as onsetdate,
    column9  as emrcode,
    column10 as dateresolved,
    column11 as statusid,
    column12 as statusidtext,
    column13 as dxpriority,
    column14 as snomedconceptid,
    regexp_extract(input_file_name, 'NG_LSSA_([^_]*)_[^\.]*.txt') as reportingenterpriseid,
    regexp_extract(input_file_name, 'NG_LSSA_[^_]*_([^\.]*).txt') as recorddate,
    regexp_extract(input_file_name, '(NG_LSSA_[^_]*_[^\.]*.txt)') as dataset
FROM new_raw_data_local
WHERE tbl_type = '0050.001';

DROP VIEW IF EXISTS `order`;
CREATE VIEW `order` AS
SELECT
    column1  as preambleformatcode,
    column2  as nextgengroupid,
    column3  as referencedatetime,
    column4  as postamblecategoryformat,
    column5  as encounter_id,
    column6  as encounterdate,
    column7  as actclass,
    column8  as actcode,
    column9  as actdiagnosiscode,
    column10 as actdiagnosis,
    column11 as actreasoncode,
    column12 as actstatus,
    column13 as acttext,
    column14 as completed,
    column15 as completedate,
    column16 as orderdate,
    column17 as completedreason,
    column18 as actdescription,
    column19 as acteffectivedate,
    column20 as cancelledReason,
    column21 as obsinterpretation,
    column22 as refertospecialty,
    column23 as obsvalue,
    column24 as therapytype,
    column25 as orderedReason,
    column26 as orderencounterdate,
    column27 as cancelleddate,
    column28 as actmood,
    column29 as receiveddate,
    column30 as acttextdisplay,
    column31 as specinsttext,
    column32 as education,
    column33 as educationdate,
    column34 as vcxcode,
    column35 as orderedosteoporosisprogramflag,
    column36 as orderinghcpzipcode,
    column37 as orderinghcpprimarytaxonomy,
    regexp_extract(input_file_name, 'NG_LSSA_([^_]*)_[^\.]*.txt') as reportingenterpriseid,
    regexp_extract(input_file_name, 'NG_LSSA_[^_]*_([^\.]*).txt') as recorddate,
    regexp_extract(input_file_name, '(NG_LSSA_[^_]*_[^\.]*.txt)') as dataset
FROM new_raw_data_local
WHERE tbl_type = '0060.001';

DROP VIEW IF EXISTS laborder;
CREATE VIEW laborder AS
SELECT
    column1  as preambleformatcode,
    column2  as nextgengroupid,
    column3  as referencedatetime,
    column4  as postamblecategoryformat,
    column5  as encounter_id,
    column6  as encounterdate,
    column7  as datadate,
    column8  as emrcode,
    column9  as testcodeid,
    column10 as ngnstatus,
    column11 as scheduledtime,
    column12 as collectiontime,
    column13 as loinccode,
    column14 as snomedcode,
    column15 as ordernum,
    column16 as diagnosiscount,
    column17 as diagnoses,
    regexp_extract(input_file_name, 'NG_LSSA_([^_]*)_[^\.]*.txt') as reportingenterpriseid,
    regexp_extract(input_file_name, 'NG_LSSA_[^_]*_([^\.]*).txt') as recorddate,
    regexp_extract(input_file_name, '(NG_LSSA_[^_]*_[^\.]*.txt)') as dataset
FROM new_raw_data_local
WHERE tbl_type = '0070.001';

DROP VIEW IF EXISTS labresult;
CREATE VIEW labresult AS
SELECT
    column1  as preambleformatcode,
    column2  as nextgengroupid,
    column3  as referencedatetime,
    column4  as postamblecategoryformat,
    column5  as encounter_id,
    column6  as encounterdate,
    column7  as datadate,
    column8  as result,
    column9  as emrcode,
    column10 as testcodeid,
    column11 as ngnstatus,
    column12 as orderedelsewhereind,
    column13 as collectiontime,
    column14 as loinccode,
    column15 as snomedcode,
    column16 as ordernum,
    regexp_extract(input_file_name, 'NG_LSSA_([^_]*)_[^\.]*.txt') as reportingenterpriseid,
    regexp_extract(input_file_name, 'NG_LSSA_[^_]*_([^\.]*).txt') as recorddate,
    regexp_extract(input_file_name, '(NG_LSSA_[^_]*_[^\.]*.txt)') as dataset
FROM new_raw_data_local
WHERE tbl_type = '0080.001';

DROP VIEW IF EXISTS medicationorder;
CREATE VIEW medicationorder AS
SELECT
    column1  as preambleformatcode,
    column2  as nextgengroupid,
    column3  as referencedatetime,
    column4  as postamblecategoryformat,
    column5  as encounter_id,
    column6  as encounterdate,
    column7  as startdate,
    column8  as orderdate,
    column9  as datestopped,
    column10 as diagnosis_code_id,
    column11 as hiclsqno,
    column12 as hic3,
    column13 as gcnseqno,
    column14 as emrcode,
    column15 as sigdesc,
    column16 as rxnorm,
    column17 as rxquantity,
    column18 as sigcodes,
    column19 as med_class_id,
    column20 as rxrefills,
    column21 as dose,
    column22 as orgrefills,
    column23 as datelastrefilled,
    regexp_extract(input_file_name, 'NG_LSSA_([^_]*)_[^\.]*.txt') as reportingenterpriseid,
    regexp_extract(input_file_name, 'NG_LSSA_[^_]*_([^\.]*).txt') as recorddate,
    regexp_extract(input_file_name, '(NG_LSSA_[^_]*_[^\.]*.txt)') as dataset
FROM new_raw_data_local
WHERE tbl_type = '0090.001';

DROP VIEW IF EXISTS `procedure`;
CREATE VIEW `procedure` AS
SELECT
    column1  as preambleformatcode,
    column2  as nextgengroupid,
    column3  as referencedatetime,
    column4  as postamblecategoryformat,
    column5  as encounter_id,
    column6  as encounterdate,
    column7  as emrcode,
    column8  as datadatetime,
    regexp_extract(input_file_name, 'NG_LSSA_([^_]*)_[^\.]*.txt') as reportingenterpriseid,
    regexp_extract(input_file_name, 'NG_LSSA_[^_]*_([^\.]*).txt') as recorddate,
    regexp_extract(input_file_name, '(NG_LSSA_[^_]*_[^\.]*.txt)') as dataset
FROM new_raw_data_local
WHERE tbl_type = '0100.001';

DROP VIEW IF EXISTS extendeddata;
CREATE VIEW extendeddata AS
SELECT
    column1  as preambleformatcode,
    column2  as nextgengroupid,
    column3  as referencedatetime,
    column4  as postamblecategoryformat,
    column5  as encounter_id,
    column6  as encounterdate,
    column7  as datasourcecode,
    column8  as datacategory,
    column9  as clinicalrecordtypecode,
    column10 as clinicalrecorddescription,
    column11 as datadate,
    column12 as emrcode,
    column13 as result,
    regexp_extract(input_file_name, 'NG_LSSA_([^_]*)_[^\.]*.txt') as reportingenterpriseid,
    regexp_extract(input_file_name, 'NG_LSSA_[^_]*_([^\.]*).txt') as recorddate,
    regexp_extract(input_file_name, '(NG_LSSA_[^_]*_[^\.]*.txt)') as dataset
FROM new_raw_data_local
WHERE tbl_type = '0110.001';
