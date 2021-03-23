"""
Janssen HV003628 record table definition (updated over the eConsent & StudyHub initial pipeline)
"""
from spark.helpers.source_table import SourceTable

TABLE_CONF = {
    "records": SourceTable(
        "csv",
        separator="|",
        columns=[
            "study_id",
            "subject_id_sh",
            "subject_id_ec",
            "subject_number",
            "c0",
            "c1",
            "c2",
            "c3",
            "gender",
            "c4",
            "c5",
            "country",
            "sub_study_consent_indicator",
            "sub_study_withdraw_indicator",
            "consent_indicator",
            "consent_withdraw_indicator",
            "hvJoinKey"
        ]
    )
}
