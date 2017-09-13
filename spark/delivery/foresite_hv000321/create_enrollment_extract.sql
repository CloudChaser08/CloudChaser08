DROP TABLE IF EXISTS {analyticsdb_schema}.temp;
CREATE TABLE {analyticsdb_schema}.temp AS
SELECT DISTINCT b.hvid,
    a.data_feed,
    d.marketplace_feed_name AS data_vendor,
    a.date_start,
    a.date_end
FROM enrollmentrecords a
    INNER JOIN (
    SELECT DISTINCT hvid
    FROM {analyticsdb_schema}.pharmacy_claims_t2d
        ) b ON MD5(CONCAT(a.hvid, 'hvid321')) = b.hvid
    INNER JOIN (
    SELECT *
    FROM {analyticsdb_schema}.mkt_def_calendar 
        ) c ON a.date_start BETWEEN c.start_date AND c.end_date
    INNER JOIN (
    SELECT *
    FROM ref_marketplace_to_warehouse
    WHERE data_type='pharmacy'
        ) d ON a.part_provider = d.warehouse_feed_name
    ;

INSERT INTO {analyticsdb_schema}.temp
SELECT DISTINCT b.hvid,
    a.data_feed,
    d.marketplace_feed_name AS data_vendor,
    a.date_start,
    a.date_end
FROM enrollmentrecords a
    INNER JOIN (
    SELECT DISTINCT hvid
    FROM {analyticsdb_schema}.pharmacy_claims_t2d
        ) b ON MD5(CONCAT(a.hvid, 'hvid321')) = b.hvid
    INNER JOIN (
    SELECT *
    FROM {analyticsdb_schema}.mkt_def_calendar 
        ) c ON a.date_end BETWEEN c.start_date AND c.end_date
    INNER JOIN (
    SELECT *
    FROM ref_marketplace_to_warehouse
    WHERE data_type='pharmacy'
        ) d ON a.part_provider = d.warehouse_feed_name
    ;

INSERT INTO {analyticsdb_schema}.temp
SELECT DISTINCT hvid,
    data_feed,
    data_vendor, 
    date_service AS date_start,
    date_service AS date_end
FROM {analyticsdb_schema}.pharmacy_claims_t2d
    ;

DROP TABLE IF EXISTS {analyticsdb_schema}.temp1;
CREATE TABLE {analyticsdb_schema}.temp1 AS
SELECT DISTINCT b.calendar_date
FROM {analyticsdb_schema}.mkt_def_calendar a
    INNER JOIN ref_calendar b 
    ON b.calendar_date BETWEEN a.start_date AND a.end_date
WHERE delivery_date={delivery_date}
    ;

DROP TABLE IF EXISTS {analyticsdb_schema}.enrollment_t2d;
CREATE TABLE {analyticsdb_schema}.enrollment_t2d AS
SELECT DISTINCT d.hvid,
    d.data_feed,
    d.data_vendor, 
    c.calendar_date,
    'Y' as enrolled_flag
FROM {analyticsdb_schema}.temp1 c 
    INNER JOIN {analyticsdb_schema}.temp d 
    ON c.calendar_date BETWEEN d.date_start AND d.date_end
    ;