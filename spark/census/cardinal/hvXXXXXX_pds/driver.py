from datetime import datetime
from spark.common.census_driver import CensusDriver
from spark.common.pharmacyclaims_common_model_census_v6 import schema as pharma_schema
from spark.runner import PACKAGE_PATH
import spark.helpers.file_utils as file_utils
import spark.helpers.schema_enforcer as schema_enforcer
import spark.helpers.postprocessor as postprocessor
import spark.helpers.privacy.pharmacyclaims as pharm_priv

V1_CUTOFF = '2020-07-17'


class CardinalPDSCensusDriver(CensusDriver):
    def __init__(self, client_name, opportunity_id, end_to_end_test=False, test=False):
        super(CardinalPDSCensusDriver, self).__init__(
            client_name=client_name,
            opportunity_id=opportunity_id,
            end_to_end_test=end_to_end_test,
            test=test
        )

        self._output_file_name_template = 'cardinal_pds_normalized_{batch_id_value}.psv.gz'
        self._v1_cutoff_date = datetime.strptime(V1_CUTOFF, '%Y-%m-%d').date()

    def load(self, batch_date, batch_id, chunk_records_files=None):
        if batch_date >= self._v1_cutoff_date:
            self.records_module_name = 'records_schemas_v2'

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

        if batch_date >= self._v1_cutoff_date:
            normalized_output = self._runner.run_spark_script(
                file_utils.get_abs_path(__file__.replace(PACKAGE_PATH, ""), '0_normalize_v2.sql'),
                return_output=True
            )
        else:
            normalized_output = self._runner.run_spark_script(
                file_utils.get_abs_path(__file__.replace(PACKAGE_PATH, ""), '0_normalize.sql'),
                return_output=True
            )

        df = postprocessor.compose(
            schema_enforcer.apply_schema_func(pharma_schema, cols_to_keep=EXTRA_COLUMNS),
            postprocessor.nullify,
            postprocessor.add_universal_columns(feed_id='39', vendor_id='42', filename=setid),
            pharm_priv.filter
        )(
            normalized_output
        ).persist()

        return df.drop(*REMOVE_COLUMNS)

    def save(self, dataframe, batch_date, batch_id, chunk_idx=None, header=True):
        super(CardinalPDSCensusDriver, self).save(
            dataframe, batch_date, batch_id, chunk_idx=chunk_idx, header=False
        )
