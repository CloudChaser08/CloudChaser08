SELECT
    named_struct(
        "census-api",
        named_struct(
            "client_id",
            txn.client_id,
            "job_id",
            txn.job_id,
            "stage",
            {stage},
            "callback",
            txn.callback_data
        )
    )                                                   as message_body,
    named_struct(
        "payload",
        named_struct(
            "hvid",
            pay.hvid,
            "claimId",
            pay.claimId,
            "errors",
            pay.errors
        )
    )                                                   as content,
    named_struct(
        "messageId",
        txn.job_id
    )                                                   as metadata
FROM cardinal_mpi_api_transactions txn
LEFT OUTER JOIN matching_payload pay
    ON txn.job_id = pay.hvJoinKey
