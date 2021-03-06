DROP TABLE IF EXISTS matching_payload_w_row_id;
CREATE TABLE matching_payload_w_row_id
AS SELECT
    monotonically_increasing_id() as row_id,
    hvid,
    claimid,
    matchstatus,
    topcandidates
FROM matching_payload;

DROP TABLE IF EXISTS matching_payload_exploded;
-- explode() ignores NULL values
-- Explicitly union rows where topCandidates is NULL with the exploded rows
-- where topCandidates was not NULL
CREATE TABLE matching_payload_exploded
AS SELECT
    row_id,
    hvid,
    claimid,
    matchstatus,
    explode(topcandidates) as candidate
FROM matching_payload_w_row_id
WHERE topcandidates IS NOT NULL

UNION ALL

SELECT
    row_id,
    hvid,
    claimid,
    matchstatus,
    NULL as candidate
FROM matching_payload_w_row_id
WHERE topcandidates IS NULL;

DROP TABLE IF EXISTS matching_payload_clean;
CREATE TABLE matching_payload_clean
AS SELECT
    row_id,
    hvid,
    claimid,
    matchstatus,
    to_json(collect_list(candidate)) as candidates
    FROM (
        SELECT
            row_id,
            hvid,
            claimid,
            matchstatus,
            map("hvid",
                cast(slightly_obfuscate_hvid(cast(round(candidate[0]) as integer), 'Cardinal_MPI-0') as string),
                "confidence",
                round(candidate[1], 2)
            ) as candidate
        FROM matching_payload_exploded
    ) x
    GROUP BY row_id, hvid, claimid, matchstatus
;

SET spark.sql.shuffle.partitions=1;
SET hive.exec.compress.output=true;
SET mapreduce.output.fileoutputformat.compress=true;
SET mapreduce.output.fileoutputformat.compress.codec=org.apache.hadoop.io.compress.GzipCodec;
DROP TABLE IF EXISTS cardinal_mpi_model;
CREATE TABLE cardinal_mpi_model
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
    WITH SERDEPROPERTIES (
        "separatorChar" = "|",
        "escapeChar"    = "\\"
    )
    STORED AS TEXTFILE
    LOCATION {location}
AS SELECT
    slightly_obfuscate_hvid(cast(hvid as integer), 'Cardinal_MPI-0') as hvid,
    claimid,
    CASE WHEN matchstatus = 'multi_match' THEN candidates ELSE NULL END as candidates
FROM matching_payload_clean
DISTRIBUTE BY 1
;

