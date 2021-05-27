# medicalclaims schemas
from spark.common.dhc.medicalclaims import medicalclaims_common_model_claims_v1
from spark.common.dhc.medicalclaims import medicalclaims_common_model_service_lines_v1

schemas = {
    'claims_schema_v1': medicalclaims_common_model_claims_v1.schema,
    'service_lines_schema_v1': medicalclaims_common_model_service_lines_v1.schema
}
