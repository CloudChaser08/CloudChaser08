SELECT
    lnk1.parent,
    lnk1.child
 FROM sentry_temp12_claim_linkage lnk1
WHERE
    (
        lnk1.parent <= lnk1.child
    AND NOT EXISTS
        (
            SELECT 1
             FROM sentry_temp12_claim_linkage lnk2
            WHERE lnk1.child = lnk2.child
              AND lnk2.parent < lnk1.parent
        )
    )
  AND NOT
    (
        lnk1.parent = lnk1.child
    AND NOT EXISTS
        (
            SELECT 1
             FROM sentry_temp12_claim_linkage lnk2
            WHERE lnk1.child = lnk2.child
              AND lnk1.parent <> lnk2.parent
        )
    )
