import logging
import spark.helpers.file_utils as file_utils
import inspect
from pyspark.sql import DataFrame


class Runner:
    """
    Run spark queries and scripts
    """

    def __init__(self, sqlContext):
        self.sqlContext = sqlContext
        self._persisted_dfs = {}

        def persist_and_track(inst, df_identifier, *args, **kwargs):
            df = inst.persist(*args, **kwargs)
            self.track_persisted_df(df_identifier, df)
            return df

        DataFrame.persist_and_track = persist_and_track
        DataFrame.cache_and_track   = persist_and_track

    def run_spark_script(self, script, variables=[], source_file_path=None, return_output=False):
        """
        Execute a spark sql script
        """

        # Implicitly get relative path to script
        script = file_utils.get_abs_path(
            # here we use the source_file_path if it was provided, if
            # there was no source_file_path provided, derive the file
            # path to the calling module's file using inspect.stack()
            source_file_path if source_file_path
            else inspect.getframeinfo(inspect.stack()[1][0]).filename,
            script
        )

        content = ''
        with open(script) as inf:
            content = inf.read()
        for i in xrange(len(variables)):
            # special variable
            # expected value is a list of column names and data types
            # [['column1', 'string'], ['column2', 'int'], ...]
            if variables[i][0] == 'additional_columns':
                variables[i][1] = \
                    ''.join(["," + column[0] + " " + column[1] for column in variables[i][1]])
            elif variables[i][0] == 'all_columns':
                variables[i][1] = \
                    ','.join([column[0] + " " + column[1] for column in variables[i][1]])
            elif len(variables[i]) != 3 or variables[i][2]:
                variables[i][1] = "'" + variables[i][1] + "'"
        k_vars = {
            variables[i][0]: variables[i][1] for i in xrange(len(variables))
        }
        content = content.format(**k_vars)
        for statement in content.split(';'):
            if len(statement.strip()) == 0:
                continue
            logging.info("STATEMENT: " + statement)
            if return_output and len(content.split(';')) == 1:
                return self.run_spark_query(statement, return_output=True)
            self.run_spark_query(statement)

    def run_spark_query(self, query, return_output=False):
        """
        Execute an individual spark sql query
        """
        if return_output:
            return self.sqlContext.sql(query)
        self.sqlContext.sql(query)

    def track_persisted_df(self, df_identifier, persisted_df):
        old_df = self._persisted_dfs.get(df_identifier)

        self._persisted_dfs[df_identifier] = persisted_df

        if old_df is not None:
            logging.warning("Persisted DataFrame with identifier {} already \
                    existed. The old DataFrame will be unpersisted".format(df_identifier))
            old_df.unpersist()

    def unpersist(self, df_identifier):
        df = self._persisted_dfs.get(df_identifier)

        if df is None:
            logging.warning("Persisted DataFrame with identifier {} not found".format(df_identifier))
        else:
            df.unpersist()
            del self._persisted_dfs[df_identifier]

