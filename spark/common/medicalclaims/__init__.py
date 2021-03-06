"""all medical claims schemas inport"""
# medicalclaims schemas
from spark.common.medicalclaims import medicalclaims_common_model_v1
from spark.common.medicalclaims import medicalclaims_common_model_v2
from spark.common.medicalclaims import medicalclaims_common_model_v3
from spark.common.medicalclaims import medicalclaims_common_model_v4
from spark.common.medicalclaims import medicalclaims_common_model_v5
from spark.common.medicalclaims import medicalclaims_common_model_v6
from spark.common.medicalclaims import medicalclaims_common_model_v6_daily
from spark.common.medicalclaims import medicalclaims_common_model_v7
from spark.common.medicalclaims import medicalclaims_common_model_v8
from spark.common.medicalclaims import medicalclaims_common_model_v8_daily
from spark.common.medicalclaims import medicalclaims_common_model_v9
from spark.common.medicalclaims import medicalclaims_common_model_v10
from spark.common.medicalclaims import medicalclaims_common_model_v10_daily


schemas = {
    'schema_v1': medicalclaims_common_model_v1.schema,
    'schema_v2': medicalclaims_common_model_v2.schema,
    'schema_v3': medicalclaims_common_model_v3.schema,
    'schema_v4': medicalclaims_common_model_v4.schema,
    'schema_v5': medicalclaims_common_model_v5.schema,
    'schema_v6': medicalclaims_common_model_v6.schema,
    'schema_v6_daily': medicalclaims_common_model_v6_daily.schema,
    'schema_v7': medicalclaims_common_model_v7.schema,
    'schema_v8': medicalclaims_common_model_v8.schema,
    'schema_v8_daily': medicalclaims_common_model_v8_daily.schema,
    'schema_v9': medicalclaims_common_model_v9.schema,
    'schema_v10': medicalclaims_common_model_v10.schema,
    'schema_v10_daily': medicalclaims_common_model_v10_daily.schema
}
