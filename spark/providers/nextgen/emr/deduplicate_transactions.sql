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

