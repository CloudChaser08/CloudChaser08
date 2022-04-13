"""
HV004190 CMM record table definitions
"""
from spark.helpers.source_table import SourceTable

TABLE_CONF = {
    "consenter_preferences": SourceTable(
        "csv",
        separator="|",
        columns=[
            "hub_id",
            "_1",
            "_2",
            "_3",
            "_4",
            "_5",
            "gender",
            "_7",
            "_8",
            "state",
            "_10",
            "_11",
            "_12",
            "_13",
            "_14",
            "_15",
            "consentee_name",
            "consentee_type",
            "consenter_id",
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
            "document_location"
            "document_signature"
            "source_system",
            "record_id",
            "hvjoinkey"
        ]
    )
}
