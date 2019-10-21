SELECT
    clm1.row_num                                                            AS parent,
    clm2.row_num                                                            AS child
 FROM sentry_temp11_claim clm1
INNER JOIN sentry_temp11_claim clm2
        ON clm1.hvid = clm2.hvid
       AND clm1.clm_attributes = clm2.clm_attributes
       AND clm2.admit_dt >= clm1.admit_dt
       AND clm2.admit_dt <= clm1.disch_dt
       -- Remove anomalies where the dates don't make sense.
       AND clm1.admit_dt <= clm1.disch_dt
       AND clm2.admit_dt <= clm2.disch_dt
