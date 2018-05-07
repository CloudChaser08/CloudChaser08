import sys

from pyspark.sql.functions import col, lit

from spark.spark_setup import init
import spark.helpers.payload_loader as payload_loader
import spark.helpers.schema_enforcer as schema_enforcer
import spark.helpers.privacy.events as events_priv
import spark.helpers.postprocessor as postprocessor
import spark.helpers.normalized_records_unloader as normalized_records_unloader
from spark.common.event_common_model_v4 import schema
from spark.runner import Runner

date_input = sys.argv[1]

payload_dir = 's3a://salusv/matching/payload/consumer/alliance/{}/'.format(date_input.replace('-', '/'))
dest_dir = 's3a://salusv/warehouse/parquet/consumer/2017-08-02/part_provider=alliance/part_best_date={}/'.format(date_input[:7])

spark, sqlContext = init('Payload to Event')
runner = Runner(sqlContext)

payload = payload_loader.load(runner, payload_dir, extra_cols=['personId'], return_output=True)

postprocessed = postprocessor.compose(
    lambda df: schema_enforcer.apply_schema(df, schema),
    postprocessor.add_universal_columns(
        '56', '243', 'alliance', '4'
    ),
    events_priv.filter
)(
    payload.select(
        col('hvid'),
        col('threeDigitZip').alias('patient_zip3'),
        col('age').alias('patient_age'),
        col('yearOfBirth').alias('patient_year_of_birth'),
        col('state').alias('patient_state'),
        col('gender').alias('patient_gender'),
        col('personId').alias('source_record_id'),
        lit('ALLIANCEID').alias('source_record_qual'),
        lit(date_input).alias('source_record_date')
    )
)

postprocessed.repartition(100).write.parquet('hdfs:///staging/')

normalized_records_unloader.distcp(dest_dir)

spark.stop()
