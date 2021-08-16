SELECT uniquepatientnumber, uniquerecordnumber, ubcapp, ubcdb, ubcprogram, hvid FROM id_previously_assigned
UNION ALL
SELECT uniquepatientnumber, uniquerecordnumber, ubcapp, ubcdb, ubcprogram, NULL as hvid FROM id_ineligible
UNION ALL
SELECT uniquepatientnumber, uniquerecordnumber, ubcapp, ubcdb, ubcprogram, hvid FROM id_newly_assigned
