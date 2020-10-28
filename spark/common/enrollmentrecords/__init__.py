"""
Enrollment Records common models
"""
from spark.common.enrollmentrecords import enrollment_common_model_v4
from spark.common.enrollmentrecords import enrollment_common_model_v5


schemas = {
    'schema_v4': enrollment_common_model_v4.schema,
    'schema_v5': enrollment_common_model_v5.schema
}
