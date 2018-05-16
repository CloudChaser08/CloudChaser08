DROP TABLE IF EXISTS encounter_dedup;
CREATE TABLE encounter_dedup
    STORED AS ORC
AS SELECT
    preambleformatcode,
    nextgengroupid,
    referencedatetime,
    postamblecategoryformat,
    encounterid,
    encounterdatetime,
    encountertype,
    encounterdescription,
    hcpzipcode,
    hcpprimarytaxonomy,
    reportingenterpriseid,
    recorddate,
    dataset
FROM (
    SELECT *,
        lead(recorddate, 1) OVER (PARTITION BY encounterid, reportingenterpriseid SORT BY recorddate) as nextrecorddate
    FROM (
        SELECT * FROM new_encounter
        UNION ALL
        SELECT preambleformatcode, nextgengroupid, referencedatetime,
            postamblecategoryformat, encounterid, encounterdatetime,
            encountertype, encounterdescription, hcpzipcode, hcpprimarytaxonomy,
            reportingenterpriseid, recorddate, dataset
        FROM old_encounter
    ) encounter_union
) encounter_distinct
WHERE nextrecorddate IS NULL;

DROP TABLE IF EXISTS demographics_local;
CREATE TABLE demographics_local
    STORED AS ORC
AS SELECT *,
    lead(recorddate, 1) OVER (PARTITION BY nextgengroupid, reportingenterpriseid SORT BY recorddate) as nextrecorddate
FROM (
    SELECT * FROM new_demographics
    UNION ALL
    SELECT preambleformatcode, nextgengroupid, referencedatetime,
        postamblecategoryformat, datacapturedate, birthyear, birthmonth,
        gender, race, zip3, coveredbymedicarepartbflag,
        patientpseudonym, reportingenterpriseid, recorddate, dataset, hvid
    FROM old_demographics
) demogpraphics_union;
