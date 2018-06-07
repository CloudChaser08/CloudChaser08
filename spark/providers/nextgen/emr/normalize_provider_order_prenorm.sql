SELECT
    *,
    UPPER(ord.actcode) as clean_actcode,
    clean_up_freetext(ord.actdiagnosiscode, false) as clean_actdiagnosiscode,
    extract_date(
        substring(ord.encounterdate, 1, 8), '%Y%m%d', CAST({min_date} AS DATE), CAST({max_date} AS DATE)
        ) as enc_dt,
    extract_date(
        substring(ord.orderdate, 1, 8), '%Y%m%d', CAST({min_date} AS DATE), CAST({max_date} AS DATE)
        ) as prov_ord_dt
FROM `order` ord
