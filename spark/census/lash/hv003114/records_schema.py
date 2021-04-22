"""
HV003114 Lash record table definitions
"""
from spark.helpers.source_table import SourceTable

TABLE_CONF = {
    "consenter_preferences": SourceTable(
        "csv",
        separator="|",
        columns=[
            "Claim_ID",
            "_1",
            "_2",
            "_3",
            "_4",
            "Gender",
            "_5",
            "_6",
            "State",
            "_7",
            "_8",
            "_9",
            "_10",
            "_11",
            "_12",
            "_13",
            "consentee_name",
            "consentee_type",
            "caregiver_id",
            "consenter_region",
            "consenter_type",
            "franchise",
            "brand",
            "program",
            "service",
            "channel",
            "consent_type",
            "transaction_date",
            "expiry_date",
            "status",
            "document_id",
            "docid",
            "source_system",
            "record_id",
            "hvjoinkey"
        ]
    )
}
