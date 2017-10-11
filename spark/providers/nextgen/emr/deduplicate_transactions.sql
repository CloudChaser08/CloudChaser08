DROP TABLE IF EXISTS encounter_dedup
CREATE TABLE encounter_dedup
AS SELECT
    first(preambleformatcode) as preambleformatcode,
    first(nextgengroupid) as nextgengroupid,
    first(referencedatetime) as referencedatetime,
    first(postamblecategoryformat) as postamblecategoryformat,
    x.encounterid,
    first(encounterdatetime) as encounterdatetime,
    first(encountertype) as encountertype,
    first(encounterdescription) as encounterdescription,
    first(hcpzipcode) as hcpzipcode,
    first(hcpprimarytaxonomy) as hcpprimarytaxonomy,
    x.reportingenterpriseid,
    x.recorddate,
    first(dataset) as dataset
FROM encounter
LEFT JOIN
    (
        SELECT reportingenterpriseid, encounterid, MAX(recorddate) as recorddate
        FROM encounter
        GROUP BY reportingenterpriseid, encounterid
    ) x
    ON x.reportingenterpriseid = encounter.reportingenterpriseid
        AND x.encounterid = encounter.encounterid
        AND x.recorddate = encounter.recorddate
WHERE x.recorddate IS NOT NULL
GROUP BY x.encounterid, x.reportingenterpriseid, x.recorddate

DROP TABLE IF EXISTS demographics_local;
CREATE TABLE demographics_local
    STORED AS PARQUET
AS SELECT
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
    regexp_extract(input_file_name(), '(NG_LSSA_[^_]*_[^\.]*.txt)') as dataset,
    lead(recorddate, 1) OVER (PARTITION BY nextgengroupid, reportingenterpriseid SORT BY recorddate) as nextrecorddate
FROM all_raw_data
WHERE column4 = '0005.001';
