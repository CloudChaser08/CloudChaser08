import logging
import inspect
import os
import copy
import spark.common.utility.logger as logger
import spark.helpers.file_utils as file_utils
from pyspark.sql import DataFrame


PACKAGE_PATH = 'spark/target/dewey.zip/'

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

    def run_spark_script(self, script, variables=None, source_file_path=None, return_output=False,
                         print_counts=False):
        """
        Execute a spark sql script
        """
        if variables is None:
            variables = []

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
        for i in range(len(variables)):
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
            variables[i][0]: variables[i][1] for i in range(len(variables))
        }
        content = content.format(**k_vars)
        for statement in content.split(';'):
            if len(statement.strip()) == 0:
                continue
            logging.info("STATEMENT: " + statement)
            if return_output and len(content.split(';')) == 1:
                return self.run_spark_query(statement, return_output=True)
            self.run_spark_query(statement)

    def run_all_spark_scripts(self, variables=None, directory_path=None,
                              count_transform_sql=False):
        if variables is None:
            variables = []

        if directory_path:
            if directory_path[-1] != '/':
                directory_path += '/'
        else:
            directory_path = os.path.dirname(inspect.getframeinfo(inspect.stack()[1][0]).filename) \
                                    .replace(PACKAGE_PATH, "") + '/'

        # SQL scripts have a naming convention of <step_number>_<table_name>.sql
        # Where <step_number> defines the order in which they need to run and
        # <table_name> the name of the table that results from this script
        scripts = [f for f in os.listdir(directory_path) if f.endswith('.sql')]

        # Compare the number of unique step numbers to the number of scripts
        if len(set([int(f.split('_')[0]) for f in scripts])) != len(scripts):
            raise Exception("At least two SQL scripts have the same step number")

        try:
            scripts = sorted(scripts, key=lambda f: int(f.split('_')[0]))
        except:
            raise Exception("At least one SQL script did not follow naming convention <step_number>_<table_name>.sql")

        for s in scripts:
            table_name = '_'.join(s.replace('.sql', '').split('_')[1:])
            logger.log(' -loading:' + table_name)
            tbl_df = self.run_spark_script(s, variables=copy.deepcopy(variables), source_file_path=directory_path, return_output=True)
            if count_transform_sql:
                logger.log('Count ' + table_name + ': ' + str(tbl_df.count()))
            tbl_df.createOrReplaceTempView(table_name)

        last_table = '_'.join(scripts[-1].replace('.sql', '').split('_')[1:])
        return self.sqlContext.table(last_table)

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
            logging.warning(
                "Persisted DataFrame with identifier {} already "
                "existed. The old DataFrame will be unpersisted".format(df_identifier)
            )
            old_df.unpersist()

    def unpersist(self, df_identifier):
        df = self._persisted_dfs.get(df_identifier)

        if df is None:
            logging.warning("Persisted DataFrame with identifier {} not found".format(df_identifier))
        else:
            df.unpersist()
            del self._persisted_dfs[df_identifier]
