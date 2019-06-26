SELECT
    slightly_obfuscate_hvid(cast(pay.hvid as integer), 'Cardinal_MPI-0')   as hvid,
    pay.claimId                                                            as claim_id,
    txn.client_id                                                          as client_id,
    txn.job_id                                                             as job_id,
    txn.callback_data                                                      as callback_data,
    pay.errors                                                             as errors
FROM cardinal_mpi_api_transactions txn
LEFT OUTER JOIN matching_payload pay
    ON txn.job_id = pay.hvJoinKey
