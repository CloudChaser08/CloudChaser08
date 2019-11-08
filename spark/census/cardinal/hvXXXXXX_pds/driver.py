from spark.common.census_driver import CensusDriver
from spark.common.pharmacyclaims_common_model_census_v6 import schema as pharma_schema
import spark.helpers.schema_enforcer as schema_enforcer
import spark.helpers.postprocessor as postprocessor
import spark.helpers.privacy.pharmacyclaims as pharm_priv

class CardinalPDSCensusDriver(CensusDriver):
    def __init__(self, client_name, opportunity_id, end_to_end_test=False, test=False):
        super(CardinalPDSCensusDriver, self).__init__(
            client_name=client_name,
            opportunity_id=opportunity_id,
            end_to_end_test=end_to_end_test,
            test=test
        )

        self._output_file_name_template = 'cardinal_pds_normalized_{batch_id_value}.psv.gz'

    def load(self, batch_date, batch_id, chunk_records_files=None):
        super(CardinalPDSCensusDriver, self).load(batch_date, batch_id, chunk_records_files)

        df = self._spark.table('cardinal_pds_transactions')
        df = postprocessor.nullify(df, ['NULL', 'Unknown', '-1', '-2'])
        df.createOrReplaceTempView('cardinal_pds_transactions')

    def transform(self, batch_date, batch_id):
        EXTRA_COLUMNS = ['tenant_id']
        REMOVE_COLUMNS = ['discharge_date', 'prov_prescribing_tax_id', 'prov_prescribing_dea_id',
                          'prov_prescribing_ssn', 'prov_prescribing_state_license',
                          'prov_prescribing_upin', 'prov_prescribing_commercial_id',
                          'prov_prescribing_name_1', 'prov_prescribing_name_2',
                          'prov_prescribing_address_1', 'prov_prescribing_address_2',
                          'prov_prescribing_city', 'prov_prescribing_state',
                          'prov_prescribing_zip', 'prov_prescribing_std_taxonomy',
                          'prov_prescribing_vendor_specialty']

        setid = 'PDS.' + batch_id

        min_date = '2011-01-01'
        max_date = batch_date.isoformat()

        normalized_output = self._runner.run_all_spark_scripts([
            ["min_date", min_date],
            ["max_date", max_date]
        ])

        df = postprocessor.compose(
            schema_enforcer.apply_schema_func(pharma_schema, cols_to_keep=EXTRA_COLUMNS),
            postprocessor.nullify,
            postprocessor.add_universal_columns(feed_id='39', vendor_id='42', filename=setid),
            pharm_priv.filter
        )(
            normalized_output
        ).persist()

        return df.drop(*REMOVE_COLUMNS)