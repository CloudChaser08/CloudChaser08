"""lhv.hvXXXXXX. driver"""
from datetime import datetime
import dateutil.tz as tz
import json
import subprocess

import pyspark.sql.functions as FN
from pyspark.sql.types import StringType, ArrayType

from spark.common.census_driver import CensusDriver, SAVE_PATH
from spark.common.utility.logger import log
import spark.helpers.payload_loader as payload_loader
import spark.helpers.records_loader as records_loader
import spark.helpers.schema_enforcer as schema_enforcer

DRIVER_MODULE_NAME = 'driver'

def _get_date():
    return datetime.now(tz.gettz('America/New York')).date()

class LiquidhubCensusDriver(CensusDriver):
    VALID_MANUFACTURERS = [m.lower() for m in ['Amgen', 'Novartis', 'Lilly']]
    CLIENT_NAME = 'lhv2'

    def __init__(self, opportunity_id, end_to_end_test=False):
        super(LiquidhubCensusDriver, self).__init__(self.CLIENT_NAME, opportunity_id,
                                                    end_to_end_test=end_to_end_test)
        self.source_patient_id_col = None
        self.run_version = 1

    def load(self, batch_date, batch_id, chunk_records_files=None):
        # Possible group id patterns
        # LHV1_<source>_PatDemo_YYYYMMDD_v#
        # LHV1_<manufacturer>_<source>_YYYYMMDD_v#
        # LHV2_<source>_PatDemo_YYYYMMDD_v#
        # LHV2_<manufacturer>_<source>_YYYYMMDD_v#

        (lh_version, manufacturer, source_name, lh_batch_date, batch_version) = batch_id.split('_')[:5]

        if 'LHV1' in batch_id:
            self.records_module_name = 'records_schemas_v1'
            self.source_patient_id_col = 'source_patient_id'
        else:
            self.records_module_name = 'records_schemas_v2'
            self.source_patient_id_col = 'claimId'

        super().load(batch_date, batch_id, chunk_records_files)

        # The beginning of the output file should be the same the batch_id
        # Then today's date and version number of the data processing run
        # (1 for the first run of this group, 2 for the second, etc)
        # and then any file ID that HealthVerity wants, we'll use a combination
        # of the original group date and version number
        self._output_file_name_template = '_'.join([lh_version, manufacturer, source_name])
        self._output_file_name_template += '_' + _get_date().isoformat().replace('-', '')
        self._output_file_name_template += '_' + batch_version
        self._output_file_name_template += '_' + lh_batch_date + 'v' + str(self.run_version) + '.txt.gz'

        # Special handling for Accredo
        no_transactional = self._spark.table('liquidhub_raw').count() == 0
        if 'accredo' == source_name.lower() and no_transactional:
            # This version of the feed doesn't have an hvJoinKey, so create one to reduce
            # downstream burden
            df = self._spark.table('matching_payload')\
                .withColumn('hvJoinKey', FN.monotonically_increasing_id()).cache()
            df.createOrReplaceTempView('matching_payload')
            df.count()

            df = self._spark.table('matching_payload')\
                .select(FN.col('hvJoinKey').alias('hvjoinkey')) \
                .withColumn('manufacturer', FN.lit(manufacturer)) \
                .withColumn('source_name', FN.lit(source_name)) \

            self.source_patient_id_col = 'personId'

            schema_enforcer.apply_schema(df, self._spark.table('liquidhub_raw').schema) \
                .createOrReplaceTempView('liquidhub_raw')

        # If the manufacturer name is not in the data, it will be in the group id
        if 'PatDemo' not in batch_id:
            self._spark.table('liquidhub_raw') \
                .withColumn('manufacturer', FN.lit(manufacturer)) \
                .createOrReplaceTempView('liquidhub_raw')

    def transform(self, batch_date, batch_id):
        log('Transforming')
        content = self._runner.run_all_spark_scripts(variables=[
            ['source_patient_id_col', self.source_patient_id_col, False]
        ])

        # Custom header
        header = (
            ['HVID', 'Source Patient Id', 'Source Name', 'Brand', 'Manufacturer'] +
            ['Filler ' + str(i) for i in range(1, 8)] +
            ['Weak Match', 'Custom HV ID', 'Provider Meta', 'Matching Meta']
        )
        deliverable = content.select(*[content[content.columns[i]].alias(header[i]) for i in range(len(header))])

        content.select('source_name', 'manufacturer')\
            .distinct().createOrReplaceTempView('liquidhub_summary')

        # Identify data from unexpected manufacturers
        bad_content = content.select('source_patient_id', 'source_name'
                                     , FN.coalesce(FN.col('manufacturer'),
                                                   FN.lit('UNKNOWN')).alias('manufacturer'))\
            .where((FN.lower(FN.col('manufacturer')).isin(self.VALID_MANUFACTURERS) == False)
                   | FN.isnull(FN.col('manufacturer')))

        small_bad_manus = bad_content\
            .groupBy(FN.concat(FN.lower(FN.col('source_name')), FN.lit('|'),
                               FN.lower(FN.col('manufacturer')))
                     .alias('manu'))\
            .count().where('count <= 5').collect()

        small_bad_manus = [r.manu for r in small_bad_manus]

        few_bad_rows = bad_content\
            .where(FN.concat(FN.lower(FN.col('source_name')), FN.lit('|'),
                             FN.lower(FN.col('manufacturer')))
                   .alias('manu').isin(small_bad_manus))\
            .groupBy('source_name', 'manufacturer')\
            .agg(FN.collect_set('source_patient_id')
                 .alias('bad_patient_ids'), FN.count('manufacturer').alias('bad_patient_count'))

        lots_bad_rows = bad_content\
            .where(FN.concat(FN.lower(FN.col('source_name')), FN.lit('|'),
                             FN.lower(FN.col('manufacturer')))
                   .alias('manu').isin(small_bad_manus) == False)\
            .groupBy('source_name', 'manufacturer')\
            .agg(FN.count('manufacturer').alias('bad_patient_count'))\
            .select('source_name', 'manufacturer', FN.lit(None).cast(ArrayType(StringType()))
                    .alias('bad_patient_ids'), 'bad_patient_count')

        few_bad_rows.union(lots_bad_rows).createOrReplaceTempView('liquidhub_error')

        return deliverable

    def save(self, dataframe, batch_date, batch_id, chunk_idx=None, header=True):
        super().save(dataframe, batch_date, batch_id, chunk_idx, header)
        with open('/tmp/summary_report_' + batch_id + '.txt', 'w') as fout:
            summ = self._spark.table('liquidhub_summary').collect()
            fout.write('\n'.join(['{}|{}'.format(r.source_name, r.manufacturer) for r in summ]))

        subprocess.check_call(
            ['hadoop', 'fs', '-put', '/tmp/summary_report_'
             + batch_id + '.txt', 'hdfs:///staging/' + batch_id + '/summary_report_' + batch_id + '.txt'])
        err = self._spark.table('liquidhub_error').collect()
        if len(err) != 0:
            with open('/tmp/error_report_' + batch_id + '.txt', 'w') as fout:
                fout.write('\n'.join([
                    '{}|{}|{}|{}'.format(r.source_name, r.manufacturer,
                                         r.bad_patient_count, json.dumps(r.bad_patient_ids))
                    for r in err
                ]))
            subprocess.check_call([
                'hadoop', 'fs', '-put', '/tmp/error_report_'+ batch_id + '.txt',
                'hdfs:///staging/' + batch_id + '/error_report_' + batch_id + '.txt'
            ])
