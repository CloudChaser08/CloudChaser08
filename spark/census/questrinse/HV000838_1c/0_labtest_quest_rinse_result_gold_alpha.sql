SELECT DISTINCT gen_ref_cd, gen_ref_desc
FROM
(
    SELECT '^TNP124' AS gen_ref_cd, 'TEST NOT PERFORMED' AS gen_ref_desc UNION ALL
    SELECT 'ADD^TNP167' AS gen_ref_cd, 'TEST NOT PERFORMED' AS gen_ref_desc UNION ALL
    SELECT 'CFUmL' AS gen_ref_cd, 'CFU/mL' AS gen_ref_desc UNION ALL
    SELECT 'CONSISTENT' AS gen_ref_cd, 'CONSISTENT' AS gen_ref_desc UNION ALL
    SELECT 'CULTURE INDICATED' AS gen_ref_cd, 'CULTURE INDICATED' AS gen_ref_desc UNION ALL
    SELECT 'DETECTED' AS gen_ref_cd, 'DETECTED' AS gen_ref_desc UNION ALL
    SELECT 'DETECTED (A)' AS gen_ref_cd, 'DETECTED' AS gen_ref_desc UNION ALL
    SELECT 'DETECTED (A1)' AS gen_ref_cd, 'DETECTED' AS gen_ref_desc UNION ALL
    SELECT 'DETECTED ABN' AS gen_ref_cd, 'DETECTED' AS gen_ref_desc UNION ALL
    SELECT 'DNR' AS gen_ref_cd, 'DO NOT REPORT' AS gen_ref_desc UNION ALL
    SELECT 'DNRTNP' AS gen_ref_cd, 'DO NOT REPORT' AS gen_ref_desc UNION ALL
    SELECT 'DO NOT REPORT' AS gen_ref_cd, 'DO NOT REPORT' AS gen_ref_desc UNION ALL
    SELECT 'DTEL^TNP1003' AS gen_ref_cd, 'TEST NOT PERFORMED' AS gen_ref_desc UNION ALL
    SELECT 'EQUIOC' AS gen_ref_cd, 'EQUIVOCAL' AS gen_ref_desc UNION ALL
    SELECT 'EQUIVOCAL' AS gen_ref_cd, 'EQUIVOCAL' AS gen_ref_desc UNION ALL
    SELECT 'FINAL RSLT: NEGATIVE' AS gen_ref_cd, 'NEGATIVE' AS gen_ref_desc UNION ALL
    SELECT 'INCONCLUSIVE' AS gen_ref_cd, 'INCONCLUSIVE' AS gen_ref_desc UNION ALL
    SELECT 'INCONSISTENT' AS gen_ref_cd, 'INCONSISTENT' AS gen_ref_desc UNION ALL
    SELECT 'INDETERMINANT' AS gen_ref_cd, 'INDETERMINATE' AS gen_ref_desc UNION ALL
    SELECT 'INDETERMINAT' AS gen_ref_cd, 'INDETERMINATE' AS gen_ref_desc UNION ALL
    SELECT 'INDETERMINATE' AS gen_ref_cd, 'INDETERMINATE' AS gen_ref_desc UNION ALL
    SELECT 'INDICATED' AS gen_ref_cd, 'INDICATED' AS gen_ref_desc UNION ALL
    SELECT 'ISOLATED' AS gen_ref_cd, 'ISOLATED' AS gen_ref_desc UNION ALL
    SELECT 'NEG' AS gen_ref_cd, 'NEGATIVE' AS gen_ref_desc UNION ALL
    SELECT 'NEG/' AS gen_ref_cd, 'NEGATIVE' AS gen_ref_desc UNION ALL
    SELECT 'NEGA CONF' AS gen_ref_cd, 'NEGATIVE CONFIRMED' AS gen_ref_desc UNION ALL
    SELECT 'NEGATI' AS gen_ref_cd, 'NEGATIVE' AS gen_ref_desc UNION ALL
    SELECT 'NEGATIVE' AS gen_ref_cd, 'NEGATIVE' AS gen_ref_desc UNION ALL
    SELECT 'NEGATIVE CONFIRMED' AS gen_ref_cd, 'NEGATIVE CONFIRMED' AS gen_ref_desc UNION ALL
    SELECT 'NO CULTURE INDICATED' AS gen_ref_cd, 'NO CULTURE INDICATED' AS gen_ref_desc UNION ALL
    SELECT 'NO VARIANT DETECTED' AS gen_ref_cd, 'NO VARIANT DETECTED' AS gen_ref_desc UNION ALL
    SELECT 'NON-DETECTED' AS gen_ref_cd, 'NOT DETECTED' AS gen_ref_desc UNION ALL
    SELECT 'NON-REACTIVE' AS gen_ref_cd, 'NON-REACTIVE' AS gen_ref_desc UNION ALL
    SELECT 'None Detected' AS gen_ref_cd, 'NOT DETECTED' AS gen_ref_desc UNION ALL
    SELECT 'Nonreactive' AS gen_ref_cd, 'NON-REACTIVE' AS gen_ref_desc UNION ALL
    SELECT 'Not Detected' AS gen_ref_cd, 'NOT DETECTED' AS gen_ref_desc UNION ALL
    SELECT 'Not Indicated' AS gen_ref_cd, 'NOT INDICATED' AS gen_ref_desc UNION ALL
    SELECT 'NOT INTERPRETED~DNR' AS gen_ref_cd, 'DO NOT REPORT' AS gen_ref_desc UNION ALL
    SELECT 'NOT ISOLATED' AS gen_ref_cd, 'NOT ISOLATED' AS gen_ref_desc UNION ALL
    SELECT 'POS' AS gen_ref_cd, 'POSITIVE' AS gen_ref_desc UNION ALL
    SELECT 'POS/' AS gen_ref_cd, 'POSITIVE' AS gen_ref_desc UNION ALL
    SELECT 'POSITIVE' AS gen_ref_cd, 'POSITIVE' AS gen_ref_desc UNION ALL
    SELECT 'REACTIVE' AS gen_ref_cd, 'REACTIVE' AS gen_ref_desc UNION ALL
    SELECT 'TA/DNR' AS gen_ref_cd, 'DO NOT REPORT' AS gen_ref_desc UNION ALL
    SELECT 'TEST NOT PERFORMED' AS gen_ref_cd, 'TEST NOT PERFORMED' AS gen_ref_desc UNION ALL
    SELECT 'TNP' AS gen_ref_cd, 'TEST NOT PERFORMED' AS gen_ref_desc UNION ALL
    SELECT 'TNP QUANTITY NOT SUFFICIENT' AS gen_ref_cd, 'TEST NOT PERFORMED' AS gen_ref_desc UNION ALL
    SELECT 'TNP/000' AS gen_ref_cd, 'TEST NOT PERFORMED' AS gen_ref_desc UNION ALL
    SELECT 'TNP/002' AS gen_ref_cd, 'TEST NOT PERFORMED' AS gen_ref_desc UNION ALL
    SELECT 'TNP/074' AS gen_ref_cd, 'TEST NOT PERFORMED' AS gen_ref_desc UNION ALL
    SELECT 'TNP/074^TNP/Z56' AS gen_ref_cd, 'TEST NOT PERFORMED' AS gen_ref_desc UNION ALL
    SELECT 'TNP/086' AS gen_ref_cd, 'TEST NOT PERFORMED' AS gen_ref_desc UNION ALL
    SELECT 'TNP/087' AS gen_ref_cd, 'TEST NOT PERFORMED' AS gen_ref_desc UNION ALL
    SELECT 'TNP/088' AS gen_ref_cd, 'TEST NOT PERFORMED' AS gen_ref_desc UNION ALL
    SELECT 'TNP/095' AS gen_ref_cd, 'TEST NOT PERFORMED' AS gen_ref_desc UNION ALL
    SELECT 'TNP/098' AS gen_ref_cd, 'TEST NOT PERFORMED' AS gen_ref_desc UNION ALL
    SELECT 'TNP/098A' AS gen_ref_cd, 'TEST NOT PERFORMED' AS gen_ref_desc UNION ALL
    SELECT 'TNP/1003' AS gen_ref_cd, 'TEST NOT PERFORMED' AS gen_ref_desc UNION ALL
    SELECT 'TNP/104' AS gen_ref_cd, 'TEST NOT PERFORMED' AS gen_ref_desc UNION ALL
    SELECT 'TNP/109' AS gen_ref_cd, 'TEST NOT PERFORMED' AS gen_ref_desc UNION ALL
    SELECT 'TNP/112' AS gen_ref_cd, 'TEST NOT PERFORMED' AS gen_ref_desc UNION ALL
    SELECT 'TNP/113' AS gen_ref_cd, 'TEST NOT PERFORMED' AS gen_ref_desc UNION ALL
    SELECT 'TNP/114' AS gen_ref_cd, 'TEST NOT PERFORMED' AS gen_ref_desc UNION ALL
    SELECT 'TNP/116' AS gen_ref_cd, 'TEST NOT PERFORMED' AS gen_ref_desc UNION ALL
    SELECT 'TNP/117' AS gen_ref_cd, 'TEST NOT PERFORMED' AS gen_ref_desc UNION ALL
    SELECT 'TNP/118' AS gen_ref_cd, 'TEST NOT PERFORMED' AS gen_ref_desc UNION ALL
    SELECT 'TNP/1189' AS gen_ref_cd, 'TEST NOT PERFORMED' AS gen_ref_desc UNION ALL
    SELECT 'TNP/120' AS gen_ref_cd, 'TEST NOT PERFORMED' AS gen_ref_desc UNION ALL
    SELECT 'TNP/121' AS gen_ref_cd, 'TEST NOT PERFORMED' AS gen_ref_desc UNION ALL
    SELECT 'TNP/1220' AS gen_ref_cd, 'TEST NOT PERFORMED' AS gen_ref_desc UNION ALL
    SELECT 'TNP/1229' AS gen_ref_cd, 'TEST NOT PERFORMED' AS gen_ref_desc UNION ALL
    SELECT 'TNP/124' AS gen_ref_cd, 'TEST NOT PERFORMED' AS gen_ref_desc UNION ALL
    SELECT 'TNP/130' AS gen_ref_cd, 'TEST NOT PERFORMED' AS gen_ref_desc UNION ALL
    SELECT 'TNP/131' AS gen_ref_cd, 'TEST NOT PERFORMED' AS gen_ref_desc UNION ALL
    SELECT 'TNP/137' AS gen_ref_cd, 'TEST NOT PERFORMED' AS gen_ref_desc UNION ALL
    SELECT 'TNP/138' AS gen_ref_cd, 'TEST NOT PERFORMED' AS gen_ref_desc UNION ALL
    SELECT 'TNP/139' AS gen_ref_cd, 'TEST NOT PERFORMED' AS gen_ref_desc UNION ALL
    SELECT 'TNP/1405' AS gen_ref_cd, 'TEST NOT PERFORMED' AS gen_ref_desc UNION ALL
    SELECT 'TNP/1442' AS gen_ref_cd, 'TEST NOT PERFORMED' AS gen_ref_desc UNION ALL
    SELECT 'TNP/1451' AS gen_ref_cd, 'TEST NOT PERFORMED' AS gen_ref_desc UNION ALL
    SELECT 'TNP/1452' AS gen_ref_cd, 'TEST NOT PERFORMED' AS gen_ref_desc UNION ALL
    SELECT 'TNP/1453' AS gen_ref_cd, 'TEST NOT PERFORMED' AS gen_ref_desc UNION ALL
    SELECT 'TNP/1455' AS gen_ref_cd, 'TEST NOT PERFORMED' AS gen_ref_desc UNION ALL
    SELECT 'TNP/149' AS gen_ref_cd, 'TEST NOT PERFORMED' AS gen_ref_desc UNION ALL
    SELECT 'TNP/1495' AS gen_ref_cd, 'TEST NOT PERFORMED' AS gen_ref_desc UNION ALL
    SELECT 'TNP/150' AS gen_ref_cd, 'TEST NOT PERFORMED' AS gen_ref_desc UNION ALL
    SELECT 'TNP/152' AS gen_ref_cd, 'TEST NOT PERFORMED' AS gen_ref_desc UNION ALL
    SELECT 'TNP/1529' AS gen_ref_cd, 'TEST NOT PERFORMED' AS gen_ref_desc UNION ALL
    SELECT 'TNP/154' AS gen_ref_cd, 'TEST NOT PERFORMED' AS gen_ref_desc UNION ALL
    SELECT 'TNP/156' AS gen_ref_cd, 'TEST NOT PERFORMED' AS gen_ref_desc UNION ALL
    SELECT 'TNP/1568' AS gen_ref_cd, 'TEST NOT PERFORMED' AS gen_ref_desc UNION ALL
    SELECT 'TNP/1569' AS gen_ref_cd, 'TEST NOT PERFORMED' AS gen_ref_desc UNION ALL
    SELECT 'TNP/1576' AS gen_ref_cd, 'TEST NOT PERFORMED' AS gen_ref_desc UNION ALL
    SELECT 'TNP/1601' AS gen_ref_cd, 'TEST NOT PERFORMED' AS gen_ref_desc UNION ALL
    SELECT 'TNP/1602' AS gen_ref_cd, 'TEST NOT PERFORMED' AS gen_ref_desc UNION ALL
    SELECT 'TNP/1613' AS gen_ref_cd, 'TEST NOT PERFORMED' AS gen_ref_desc UNION ALL
    SELECT 'TNP/1635' AS gen_ref_cd, 'TEST NOT PERFORMED' AS gen_ref_desc UNION ALL
    SELECT 'TNP/167' AS gen_ref_cd, 'TEST NOT PERFORMED' AS gen_ref_desc UNION ALL
    SELECT 'TNP/1674' AS gen_ref_cd, 'TEST NOT PERFORMED' AS gen_ref_desc UNION ALL
    SELECT 'TNP/168' AS gen_ref_cd, 'TEST NOT PERFORMED' AS gen_ref_desc UNION ALL
    SELECT 'TNP/1710' AS gen_ref_cd, 'TEST NOT PERFORMED' AS gen_ref_desc UNION ALL
    SELECT 'TNP/1711' AS gen_ref_cd, 'TEST NOT PERFORMED' AS gen_ref_desc UNION ALL
    SELECT 'TNP/1712' AS gen_ref_cd, 'TEST NOT PERFORMED' AS gen_ref_desc UNION ALL
    SELECT 'TNP/1728' AS gen_ref_cd, 'TEST NOT PERFORMED' AS gen_ref_desc UNION ALL
    SELECT 'TNP/1766' AS gen_ref_cd, 'TEST NOT PERFORMED' AS gen_ref_desc UNION ALL
    SELECT 'TNP/187' AS gen_ref_cd, 'TEST NOT PERFORMED' AS gen_ref_desc UNION ALL
    SELECT 'TNP/200' AS gen_ref_cd, 'TEST NOT PERFORMED' AS gen_ref_desc UNION ALL
    SELECT 'TNP/202' AS gen_ref_cd, 'TEST NOT PERFORMED' AS gen_ref_desc UNION ALL
    SELECT 'TNP/220' AS gen_ref_cd, 'TEST NOT PERFORMED' AS gen_ref_desc UNION ALL
    SELECT 'TNP/234' AS gen_ref_cd, 'TEST NOT PERFORMED' AS gen_ref_desc UNION ALL
    SELECT 'TNP/244' AS gen_ref_cd, 'TEST NOT PERFORMED' AS gen_ref_desc UNION ALL
    SELECT 'TNP/270' AS gen_ref_cd, 'TEST NOT PERFORMED' AS gen_ref_desc UNION ALL
    SELECT 'TNP/282' AS gen_ref_cd, 'TEST NOT PERFORMED' AS gen_ref_desc UNION ALL
    SELECT 'TNP/310' AS gen_ref_cd, 'TEST NOT PERFORMED' AS gen_ref_desc UNION ALL
    SELECT 'TNP/314' AS gen_ref_cd, 'TEST NOT PERFORMED' AS gen_ref_desc UNION ALL
    SELECT 'TNP/317' AS gen_ref_cd, 'TEST NOT PERFORMED' AS gen_ref_desc UNION ALL
    SELECT 'TNP/328' AS gen_ref_cd, 'TEST NOT PERFORMED' AS gen_ref_desc UNION ALL
    SELECT 'TNP/329' AS gen_ref_cd, 'TEST NOT PERFORMED' AS gen_ref_desc UNION ALL
    SELECT 'TNP/373' AS gen_ref_cd, 'TEST NOT PERFORMED' AS gen_ref_desc UNION ALL
    SELECT 'TNP/379' AS gen_ref_cd, 'TEST NOT PERFORMED' AS gen_ref_desc UNION ALL
    SELECT 'TNP/500' AS gen_ref_cd, 'TEST NOT PERFORMED' AS gen_ref_desc UNION ALL
    SELECT 'TNP/521' AS gen_ref_cd, 'TEST NOT PERFORMED' AS gen_ref_desc UNION ALL
    SELECT 'TNP/563' AS gen_ref_cd, 'TEST NOT PERFORMED' AS gen_ref_desc UNION ALL
    SELECT 'TNP/564' AS gen_ref_cd, 'TEST NOT PERFORMED' AS gen_ref_desc UNION ALL
    SELECT 'TNP/568' AS gen_ref_cd, 'TEST NOT PERFORMED' AS gen_ref_desc UNION ALL
    SELECT 'TNP/603' AS gen_ref_cd, 'TEST NOT PERFORMED' AS gen_ref_desc UNION ALL
    SELECT 'TNP/607' AS gen_ref_cd, 'TEST NOT PERFORMED' AS gen_ref_desc UNION ALL
    SELECT 'TNP/632' AS gen_ref_cd, 'TEST NOT PERFORMED' AS gen_ref_desc UNION ALL
    SELECT 'TNP/637' AS gen_ref_cd, 'TEST NOT PERFORMED' AS gen_ref_desc UNION ALL
    SELECT 'TNP/665' AS gen_ref_cd, 'TEST NOT PERFORMED' AS gen_ref_desc UNION ALL
    SELECT 'TNP/685' AS gen_ref_cd, 'TEST NOT PERFORMED' AS gen_ref_desc UNION ALL
    SELECT 'TNP/726' AS gen_ref_cd, 'TEST NOT PERFORMED' AS gen_ref_desc UNION ALL
    SELECT 'TNP/767' AS gen_ref_cd, 'TEST NOT PERFORMED' AS gen_ref_desc UNION ALL
    SELECT 'TNP/776' AS gen_ref_cd, 'TEST NOT PERFORMED' AS gen_ref_desc UNION ALL
    SELECT 'TNP/797' AS gen_ref_cd, 'TEST NOT PERFORMED' AS gen_ref_desc UNION ALL
    SELECT 'TNP/875' AS gen_ref_cd, 'TEST NOT PERFORMED' AS gen_ref_desc UNION ALL
    SELECT 'TNP/877' AS gen_ref_cd, 'TEST NOT PERFORMED' AS gen_ref_desc UNION ALL
    SELECT 'TNP/916' AS gen_ref_cd, 'TEST NOT PERFORMED' AS gen_ref_desc UNION ALL
    SELECT 'TNP/931' AS gen_ref_cd, 'TEST NOT PERFORMED' AS gen_ref_desc UNION ALL
    SELECT 'TNP/942' AS gen_ref_cd, 'TEST NOT PERFORMED' AS gen_ref_desc UNION ALL
    SELECT 'TNP/945' AS gen_ref_cd, 'TEST NOT PERFORMED' AS gen_ref_desc UNION ALL
    SELECT 'TNP/970' AS gen_ref_cd, 'TEST NOT PERFORMED' AS gen_ref_desc UNION ALL
    SELECT 'TNP/A06' AS gen_ref_cd, 'TEST NOT PERFORMED' AS gen_ref_desc UNION ALL
    SELECT 'TNP/B24' AS gen_ref_cd, 'TEST NOT PERFORMED' AS gen_ref_desc UNION ALL
    SELECT 'TNP/B28' AS gen_ref_cd, 'TEST NOT PERFORMED' AS gen_ref_desc UNION ALL
    SELECT 'TNP/B38' AS gen_ref_cd, 'TEST NOT PERFORMED' AS gen_ref_desc UNION ALL
    SELECT 'TNP/BPIS' AS gen_ref_cd, 'TEST NOT PERFORMED' AS gen_ref_desc UNION ALL
    SELECT 'TNP/C06' AS gen_ref_cd, 'TEST NOT PERFORMED' AS gen_ref_desc UNION ALL
    SELECT 'TNP/C52' AS gen_ref_cd, 'TEST NOT PERFORMED' AS gen_ref_desc UNION ALL
    SELECT 'TNP/D20' AS gen_ref_cd, 'TEST NOT PERFORMED' AS gen_ref_desc UNION ALL
    SELECT 'TNP/DUPLICATE' AS gen_ref_cd, 'TEST NOT PERFORMED' AS gen_ref_desc UNION ALL
    SELECT 'TNP/HEESR' AS gen_ref_cd, 'TEST NOT PERFORMED' AS gen_ref_desc UNION ALL
    SELECT 'TNP/HSVTNP' AS gen_ref_cd, 'TEST NOT PERFORMED' AS gen_ref_desc UNION ALL
    SELECT 'TNP/K05' AS gen_ref_cd, 'TEST NOT PERFORMED' AS gen_ref_desc UNION ALL
    SELECT 'TNP/M299' AS gen_ref_cd, 'TEST NOT PERFORMED' AS gen_ref_desc UNION ALL
    SELECT 'TNP/M406' AS gen_ref_cd, 'TEST NOT PERFORMED' AS gen_ref_desc UNION ALL
    SELECT 'TNP/O54' AS gen_ref_cd, 'TEST NOT PERFORMED' AS gen_ref_desc UNION ALL
    SELECT 'TNP/PPT098' AS gen_ref_cd, 'TEST NOT PERFORMED' AS gen_ref_desc UNION ALL
    SELECT 'TNP/T84' AS gen_ref_cd, 'TEST NOT PERFORMED' AS gen_ref_desc UNION ALL
    SELECT 'TNP/TNP1' AS gen_ref_cd, 'TEST NOT PERFORMED' AS gen_ref_desc UNION ALL
    SELECT 'TNP/TNP1710' AS gen_ref_cd, 'TEST NOT PERFORMED' AS gen_ref_desc UNION ALL
    SELECT 'TNP/TNP1711' AS gen_ref_cd, 'TEST NOT PERFORMED' AS gen_ref_desc UNION ALL
    SELECT 'TNP/TNP282' AS gen_ref_cd, 'TEST NOT PERFORMED' AS gen_ref_desc UNION ALL
    SELECT 'TNP/TNP6011' AS gen_ref_cd, 'TEST NOT PERFORMED' AS gen_ref_desc UNION ALL
    SELECT 'TNP/TNP797' AS gen_ref_cd, 'TEST NOT PERFORMED' AS gen_ref_desc UNION ALL
    SELECT 'TNP/TNPFECALBD' AS gen_ref_cd, 'TEST NOT PERFORMED' AS gen_ref_desc UNION ALL
    SELECT 'TNP/TNPR138' AS gen_ref_cd, 'TEST NOT PERFORMED' AS gen_ref_desc UNION ALL
    SELECT 'TNP/TNPR1601' AS gen_ref_cd, 'TEST NOT PERFORMED' AS gen_ref_desc UNION ALL
    SELECT 'TNP/TNPTIO' AS gen_ref_cd, 'TEST NOT PERFORMED' AS gen_ref_desc UNION ALL
    SELECT 'TNP/TWW12322' AS gen_ref_cd, 'TEST NOT PERFORMED' AS gen_ref_desc UNION ALL
    SELECT 'TNP/TWWRAM05' AS gen_ref_cd, 'TEST NOT PERFORMED' AS gen_ref_desc UNION ALL
    SELECT 'TNP/TWWRAM05A' AS gen_ref_cd, 'TEST NOT PERFORMED' AS gen_ref_desc UNION ALL
    SELECT 'TNP/UA25' AS gen_ref_cd, 'TEST NOT PERFORMED' AS gen_ref_desc UNION ALL
    SELECT 'TNP/WW11158' AS gen_ref_cd, 'TEST NOT PERFORMED' AS gen_ref_desc UNION ALL
    SELECT 'TNP/Y32' AS gen_ref_cd, 'TEST NOT PERFORMED' AS gen_ref_desc UNION ALL
    SELECT 'TNP124' AS gen_ref_cd, 'TEST NOT PERFORMED' AS gen_ref_desc UNION ALL
    SELECT 'TNP124^CANC' AS gen_ref_cd, 'TEST NOT PERFORMED' AS gen_ref_desc 

)
