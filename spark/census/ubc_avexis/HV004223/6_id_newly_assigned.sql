SELECT a.privateidone, a.gender, a.firstservicedate, a.ubcapp, a.ubcdb, a.ubcprogram, a.uniquepatientnumber, a.uniquerecordnumber, b.hvid
FROM id_eligible a
INNER JOIN (
SELECT *,
    {max_assigned_id} + RANK() OVER (ORDER BY privateidone) as hvid
FROM (
    SELECT DISTINCT privateidone, gender, firstservicedate
    FROM id_eligible
) x
) b USING(privateidone, gender, firstservicedate)
