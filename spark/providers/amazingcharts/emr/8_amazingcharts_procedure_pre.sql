SELECT
    procedure.*
FROM
(
    SELECT
        procedure_1.*
    FROM
        amazingcharts_procedure_1 procedure_1
    WHERE NOT EXISTS (
        SELECT 1 FROM amazingcharts_procedure_3 procedure_3
        WHERE procedure_1.proc_dt = procedure_3.proc_dt
            AND procedure_1.hvid = procedure_3.hvid
            AND procedure_1.proc_cd = procedure_3.proc_cd
        )
    UNION ALL
        SELECT procedure_2.* FROM amazingcharts_procedure_2 procedure_2
    UNION ALL
        SELECT procedure_3.* FROM amazingcharts_procedure_3 procedure_3
) procedure