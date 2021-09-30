"""run details"""
from spark.common.utility.output_type import DataType, RunType
from spark.common.utility.singleton import Singleton


class RunDetails(metaclass=Singleton):
    __slots__ = [
        'provider_name',
        'data_type',
        'data_source_transaction_path',
        'data_source_matching_path',
        'output_path',
        'run_type',
        'input_date'
    ]

    def __init__(self,
                 provider_name,
                 data_type,
                 data_source_transaction_path,
                 data_source_matching_path,
                 output_path,
                 run_type,
                 input_date):
        """Contains the details regarding the data that was ingested.

        Args:
            provider_name (str): The name of the provider the data comes from.
            data_type (str or DataType): The type of data being ingested.
            data_source_transaction_path (str): The path to the transaction data.
            data_source_matching_path (str): The path to the matching data.
            output_path (str): The path where the ingested data is saved to.
            run_type (str or RunType): The service the ingested data will be used for.
            input_date (str): The date associated with the ingest data.

        Attributes:
            provider_name (str): The name of the provider the data comes from.
            data_type (str or DataType): The type of data being ingested.
            data_source_transaction_path (str): The path to the transaction data.
            data_source_matching_path (str): The path to the matching data.
            output_path (str): The path where the ingested data is saved to.
            run_type (str or RunType): The service the ingested data will be used for.
            input_date (str): The date associated with the ingest data.

        Returns:
            `RunDetails`
        """

        self.provider_name = provider_name
        self.data_type = DataType(data_type)
        self.data_source_transaction_path = data_source_transaction_path
        self.data_source_matching_path = data_source_matching_path
        self.output_path = output_path
        self.run_type = RunType(run_type)
        self.input_date = input_date
