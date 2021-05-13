CREATE TABLE cardinal_mpi_model
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
    WITH SERDEPROPERTIES (
        "separatorChar" = "|",
        "escapeChar"    = "\\"
    )
    STORED AS TEXTFILE
    LOCATION '/tmp/cardinal_mpi/'
AS SELECT
    slightly_obfuscate_hvid(cast(hvid as integer), 'Cardinal_MPI-0') as hvid,
    claimid,
    CASE WHEN matchstatus = 'multi_match' THEN candidates ELSE NULL END as candidates
FROM matching_payload_clean
DISTRIBUTE BY 1
