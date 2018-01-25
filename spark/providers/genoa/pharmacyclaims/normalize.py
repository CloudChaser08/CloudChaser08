import spark.common.pharmacyclaims_common_model_v4 as pharmacyclaims_common_model
import spark.helpers.schema_enforcer as schema_enforcer

def run(runner,):
    norm_pharmacy = runner.run_spark_script('mapping.sql', return_output=True)

    schema_enforcer.apply_schema(norm_pharmacy, pharmacyclaims_common_model.schema) \
        .createOrReplaceTempView('pharmacyclaims_common_model')

