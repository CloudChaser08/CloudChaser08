DROP TABLE IF EXISTS demographics_dedup;
CREATE TABLE demographics_dedup
AS SELECT 
    first(preambleformatcode) as preambleformatcode,
    x.nextgengroupid,
    first(referencedatetime) as referencedatetime,
    first(postamblecategoryformat) as postamblecategoryformat,
    first(datacapturedate) as datacapturedate,
    first(birthyear) as birthyear,
    first(birthmonth) as birthmonth,
    first(gender) as gender,
    first(race) as race,
    first(zip3) as zip3,
    first(coveredbymedicarepartbflag) as coveredbymedicarepartbflag,
    first(patientpseudonym) as patientpseudonym,
    x.reportingenterpriseid,
    x.recorddate,
    first(dataset) as dataset
FROM demographics
LEFT JOIN
    (
        SELECT reportingenterpriseid, nextgengroupid, MAX(recorddate) as recorddate
        FROM demographics
        GROUP BY reportingenterpriseid, nextgengroupid
    ) x
    ON x.reportingenterpriseid = demographics.reportingenterpriseid
        AND x.nextgengroupid = demographics.nextgengroupid
        AND x.recorddate = demographics.recorddate
WHERE x.recorddate IS NOT NULL
GROUP BY x.nextgengroupid, x.reportingenterpriseid, x.recorddate;

DROP TABLE IF EXISTS encounter_dedup;
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
GROUP BY x.encounterid, x.reportingenterpriseid, x.recorddate;

