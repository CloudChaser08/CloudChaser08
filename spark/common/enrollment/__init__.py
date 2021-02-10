"""
Enrollment Records common models
"""
from spark.common.enrollment import enrollment_common_model_v4
from spark.common.enrollment import enrollment_common_model_v5
from spark.common.enrollment import enrollment_common_model_v6


schemas = {
    'schema_v4': enrollment_common_model_v4.schema,
    'schema_v5': enrollment_common_model_v5.schema,
    'schema_v6': enrollment_common_model_v6.schema
}
