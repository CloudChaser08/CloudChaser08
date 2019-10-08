SELECT DISTINCT
    parent,
    child
 FROM
(
    SELECT
        key                                                                             AS parent,
        EXPLODE(value)                                                                  AS child
     FROM
    (
        SELECT
            EXPLODE(col1)
         FROM
        (
            SELECT
                FIND_DESCENDANTS_RECURSIVELY(COLLECT_LIST(parent), COLLECT_LIST(child)) AS col1
             FROM sentry_temp13_linkage_input
        )
    )
    UNION ALL
    SELECT
        lnk1.parent,
        lnk1.child
     FROM sentry_temp12_claim_linkage lnk1
    WHERE lnk1.parent = lnk1.child
      AND NOT EXISTS
        (
            SELECT 1
             FROM sentry_temp12_claim_linkage lnk2
            WHERE lnk1.child = lnk2.child
              AND lnk1.parent <> lnk2.parent
        )
    UNION ALL
    SELECT 
        clm.row_num                                                                     AS parent,
        clm.row_num                                                                     AS child
     FROM sentry_temp11_claim clm
    WHERE clm.admit_dt > clm.disch_dt
)
