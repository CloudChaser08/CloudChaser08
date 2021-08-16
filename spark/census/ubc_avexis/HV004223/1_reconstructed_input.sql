SELECT
    r.uniquepatientnumber,
    r.uniquerecordnumber,
    r.ubcapp,
    r.ubcdb,
    r.ubcprogram,
    mp.privateidone,
    r.dateofservice,
    r.firstservicedate,
    r.gender
FROM records r
    LEFT JOIN matching_payload mp ON r.hv_join_key = mp.hvJoinKey
