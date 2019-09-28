import spark.census.example.hvXXXXX2.records_schemas_weekdays
import spark.census.example.hvXXXXX2.records_schemas_weekends
import spark.census.example.hvXXXXX2.matching_payloads_schemas

from spark.common.census_driver import CensusDriver, PACKAGE_PATH

class ComplexExampleCensusDriver(CensusDriver):
    """
    Simple example census driver that overrides default behavior
    """
    CLIENT_NAME    = 'example'
    OPPORTUNITY_ID = 'hvXXXXX2'
    OUTPUT_FILE_NAME_TEMPLATE = 'hv_census_{year}{month}{day}.psv.gz'

    def __init__(self, end_to_end_test=False):
        super(ComplexExampleCensusDriver, self).__init__(self.CLIENT_NAME, self.OPPORTUNITY_ID,
                end_to_end_test=end_to_end_test)

        # Example of overwriting the default output file name template
        self.output_file_name_template = OUTPUT_FILE_NAME_TEMPLATE

    # Example of overwriting the default load function
    def load(self, batch_date, batch_id):
        if batch_date.weekday() <= 4:
            records_schemas = spark.census.example.hvXXXXX2.records_schemas_weekdays
        else:
            records_schemas = spark.census.example.hvXXXXX2.records_schemas_weekends

        matching_payloads_schemas = spark.census.example.hvXXXXX2.matching_payloads_schemas

        _batch_id_path, _batch_id_value = self._get_batch_info(self, batch_date, batch_id):
        records_path  = self._records_path_template.format(
            batch_id_path=_batch_id_path
        )
        matching_path = self._matching_path_template.format(
            batch_id_path=_batch_id_path
        )

        records_loader.load_and_clean_all_v2(self._runner, records_path,
                records_schemas, load_file_name=True)
        payload_loader.load_all(self._runner, matching_path,
                matching_payloads_schemas)


    # Example of overwriting the default transform function
    def transform(self):
        content = self._runner.run_all_spark_scripts().repartition(1).sortWithinPartitions('hvid')
        return content

    # Example of overwriting the default save function
    def save(self, dataframe, batch_date, batch_id):
        if batch_date.weekday() <= 4:
            day_type = 'weekday'
        else:
            day_type = 'weekend'
        
        dataframe.createOrReplaceTempView('deliverable')
        normalized_records_unloader.unload_delimited_file(
            self._spark, self._runner, 'hdfs:///staging/{}/{}/{}/{}/'.format(
                batch_date.year, batch_date.month, batch_date.day, day_type
            ),
            'deliverable',
            output_file_name=self._output_file_name.format(
                batch_date.year, batch_date.month, batch_date.day
            )
        )
