"""
    Processing functions for stats
"""

from datetime import datetime
import logging

from dateutil.relativedelta import relativedelta
from pyspark.sql.functions import coalesce, col, lit, upper

import spark.stats.calc.fill_rate as fill_rate
import spark.stats.calc.top_values as top_values
import spark.stats.calc.key_stats as key_stats
import spark.stats.calc.longitudinality as longitudinality
import spark.stats.calc.year_over_year as year_over_year
import spark.stats.calc.epi as epi
import spark.helpers.stats.utils as utils

ALL_DATA = None
SAMPLED_DATA = None
SAMPLED_DATA_MULTIPLIER = None


class DataframeProvider(object):
    """ An object that lazily provides dataframes to use in processing,
        serving cached results if available
    """

    def __init__(self, spark, sql_context, provider_conf, start_date, end_date):

        # Cached data
        self._all_data = None
        self._sampled_data = None
        self._sampled_data_multiplier = None

        # Public attributes
        self.spark = spark
        self.sql_context = sql_context
        self.provider_conf = provider_conf
        if self.provider_conf.earliest_date > start_date:
            self.start_date = self.provider_conf.earliest_date
        else:
            self.start_date = start_date
        self.end_date = end_date

    def reconfigure(self, provider_conf):
        """ Reconfigures as a copy with a new provider config """
        return DataframeProvider(
            spark=self.spark,
            sql_context=self.sql_context,
            provider_conf=provider_conf,
            start_date=self.start_date,
            end_date=self.end_date
        )

    @property
    def all_data(self):
        """ Returns a dataframe with all data for the provider in the date
            range, using cache if available
        """
        if not self._all_data:
            if self.provider_conf.datatype == 'emr':
                self._all_data = utils.get_emr_union(
                    self.sql_context,
                    self.provider_conf.models,
                    self.provider_conf.datafeed_id
                )
            else:
                if self.provider_conf.datatype.startswith('emr'):
                    partition = self.provider_conf.datafeed_id
                else:
                    partition = self.provider_conf.name

                self._all_data = utils.get_provider_data(
                    self.sql_context,
                    self.provider_conf.datatype,
                    partition,
                    custom_schema=self.provider_conf.custom_schema,
                    custom_table=self.provider_conf.custom_table
                )

            if 'logical_delete_reason' in self._all_data.columns:
                self._all_data = self._all_data.where(
                    col('logical_delete_reason').isNull() |
                    (upper(col('logical_delete_reason')) != lit('INACTIVE'))
                )

            self._all_data = self._all_data.withColumn(
                'coalesced_date', coalesce(*self.provider_conf.date_fields)
            )

        return self._all_data

    @property
    def sampled_data(self):
        """ Returns a dataframe that reflects a sampling of `all_data` """
        if not self._sampled_data:
            self._load_sampled_data()
        return self._sampled_data

    @property
    def sampled_data_multiplier(self):
        """ Returns the data multiplier used for sampling """
        if not self._sampled_data_multiplier:
            self._load_sampled_data()
        return self._sampled_data_multiplier

    def _load_sampled_data(self):
        partitions = int(self.spark.conf.get('spark.sql.shuffle.partitions'))

        mult, data = utils.select_data_sample_in_date_range(
            '1900-01-01' if self.provider_conf.index_all_dates else self.start_date, self.end_date,
            'coalesced_date', include_nulls=self.provider_conf.index_null_dates,
            record_field=self.provider_conf.record_field
        )(self.all_data)
        self._sampled_data = data.coalesce(partitions).cache()
        self._sampled_data_multiplier = mult


def run_fill_rates(provider_conf, df_provider):
    """
        Runs fill rates using the provider's fill rate configuration, if
        that configuration is defined (otherwise returns None)
    """
    if not provider_conf.fill_rate_conf:
        return None
    dataframe = df_provider.sampled_data

    # Get only the columns needed to calculate fill rates on
    cols = {
        c for c in dataframe.columns
        if c in provider_conf.fill_rate_conf.columns.keys()
    }
    if provider_conf.record_field:
        cols.add(provider_conf.record_field)
        dataframe = utils.select_distinct_values_from_column(
            provider_conf.record_field
        )(dataframe)
    fill_rate_cols_dataframe = dataframe.select(*cols)
    return fill_rate.calculate_fill_rate(fill_rate_cols_dataframe)


def run_top_values(provider_conf, df_provider):
    """
       Runs top values using the provider's top_values configuration, if
       that configuration is defined (otherwise returns None)
    """
    if not provider_conf.top_values_conf:
        return None
    dataframe = df_provider.sampled_data
    multiplier = df_provider.sampled_data_multiplier

    # Get only the columns needed to calculate fill rates on
    cols = {
        c for c in dataframe.columns
        if c in provider_conf.top_values_conf.columns.keys()
    }
    if provider_conf.record_field:
        cols.add(provider_conf.record_field)
    max_num_values = provider_conf.top_values_conf.max_values
    top_values_cols_dataframe = dataframe.select(*cols)

    sampled_top_values = top_values.calculate_top_values(
        top_values_cols_dataframe, max_num_values,
        distinct_column=provider_conf.record_field
    )

    if sampled_top_values:
        for top_value_stat in sampled_top_values:
            top_value_stat['count'] = int(top_value_stat['count'] * multiplier)

    return sampled_top_values

def run_key_stats(provider_conf, start_date, end_date, df_provider):
    """
        Runs key stats using the provider's top_values configuration, between
        the provided dates, if that configuration is defined
        (otherwise returns None)
    """

    if provider_conf.key_stats:
        return key_stats.calculate_key_stats(
            df_provider.all_data,
            provider_conf.earliest_date,
            start_date,
            end_date,
            provider_conf
        )
    return None

def run_longitudinality(provider_conf, end_date, df_provider):
    """
        Runs longitudinality using the provider's longitudinality
        configuration, between the provided dates, if that configuration
        is defined (otherwise returns None)
    """
    if provider_conf.longitudinality:
        if _calculate_available_years(provider_conf, end_date) < 2:
            logging.error(
                "Cannot calculate longitudinality for %s, this feed has "
                "less than 2 years available.",
                provider_conf.datafeed_id
            )
        else:
            return longitudinality.calculate_longitudinality(
                df_provider.all_data.select("hvid", "coalesced_date"),
                provider_conf
            )
    return None

def run_year_over_year(provider_conf, end_date, df_provider):
    """
        Runs longitudinality using the provider's longitudinality
        configuration, between the provided dates, if that configuration
        is defined (otherwise returns None)
    """
    if provider_conf.year_over_year:
        if _calculate_available_years(provider_conf, end_date) < 2:
            logging.error(
                "Cannot calculate year_over_year for %s, this feed has "
                "less than 2 years available.",
                provider_conf.datafeed_id
            )
        else:
            return year_over_year.calculate_year_over_year(
                df_provider.all_data.select("hvid", "coalesced_date"),\
                provider_conf.earliest_date,
                end_date,
                provider_conf
            )
    return None


def _calculate_available_years(provider_conf, end_date):
    return relativedelta(
        datetime.strptime(end_date, '%Y-%m-%d'),
        datetime.strptime(provider_conf.earliest_date, '%Y-%m-%d')
    ).years


def get_epi_calcs(provider_conf):
    """ Runs EPI calculations """

    if provider_conf.epi_calc_conf:
        return {
            field: epi.calculate_epi(provider_conf, field)
            for field in provider_conf.epi_calc_conf.fields
        }
    return None
