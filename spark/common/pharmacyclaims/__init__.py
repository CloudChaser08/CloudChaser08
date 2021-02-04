from spark.common.pharmacyclaims import pharmacyclaims_common_model_v3
from spark.common.pharmacyclaims import pharmacyclaims_common_model_v4
from spark.common.pharmacyclaims import pharmacyclaims_common_model_v6
from spark.common.pharmacyclaims import pharmacyclaims_common_model_v7
from spark.common.pharmacyclaims import pharmacyclaims_common_model_v8
from spark.common.pharmacyclaims import pharmacyclaims_common_model_v9
from spark.common.pharmacyclaims import pharmacyclaims_common_model_v10
from spark.common.pharmacyclaims import pharmacyclaims_common_model_v11
from spark.common.pharmacyclaims import pharmacyclaims_common_model_v11_daily


schemas = {
    'schema_v3': pharmacyclaims_common_model_v3.schema,
    'schema_v4': pharmacyclaims_common_model_v4.schema,
    'schema_v6': pharmacyclaims_common_model_v6.schema,
    'schema_v7': pharmacyclaims_common_model_v7.schema,
    'schema_v8': pharmacyclaims_common_model_v8.schema,
    'schema_v9': pharmacyclaims_common_model_v9.schema,
    'schema_v10': pharmacyclaims_common_model_v10.schema,
    'schema_v11': pharmacyclaims_common_model_v11.schema,
    'schema_v11_daily': pharmacyclaims_common_model_v11_daily.schema
}
