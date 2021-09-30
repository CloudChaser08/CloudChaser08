"""logger"""
from spark.common.utility.spark_state import SparkState
from spark.common.utility.run_details import RunDetails


def log(message):
    print(message)


def log_spark_state():
    """Captures the running Spark application's state in a
    `SparkState` instance.
    """
    SparkState.get_current_state()


def log_run_details(provider_name,
                    data_type,
                    data_source_transaction_path,
                    data_source_matching_path,
                    output_path,
                    run_type,
                    input_date):
    """Captures the information about the data being ingested in a
    `RunDetails` instance.

    Args:
        provider_name (str): The name of the provider the data comes from.
        data_type (str or DataType): The type of data being ingested.
        data_source_transaction_path (str): The path to the transaction data.
        data_source_matching_path (str): The path to the matching data.
        output_path (str): The path where the ingested data is saved to.
        run_type (str or RunType): The service the ingested data will be used for.
        input_date (str): The date associated with the ingest data.
    """

    RunDetails(
        provider_name,
        data_type,
        data_source_transaction_path,
        data_source_matching_path,
        output_path,
        run_type,
        input_date)
