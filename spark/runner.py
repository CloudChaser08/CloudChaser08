import logging
import spark.helpers.file_utils as file_utils
import inspect


class Runner:
    """
    Run spark queries and scripts
    """

    def __init__(self, sqlContext):
        self.sqlContext = sqlContext

    def run_spark_script(self, script, variables=[], source_file_path=None):
        """
        Execute a spark sql script
        """

        # Implicitly get relative path to script
        script = file_utils.get_abs_path(
            # here we use the source_file_path if it was provided, if
            # there was no source_file_path provided, derive the file
            # path to the calling module's file using inspect.stack()
            source_file_path if source_file_path
            else inspect.getmodule(inspect.stack()[1][0]).__file__,
            script
        )

        content = ''
        with open(script) as inf:
            content = inf.read()
        for i in xrange(len(variables)):
            if len(variables[i]) != 3 or variables[i][2]:
                variables[i][1] = "'" + variables[i][1] + "'"
        k_vars = {
            variables[i][0]: variables[i][1] for i in xrange(len(variables))
        }
        content = content.format(**k_vars)
        for statement in content.split(';'):
            if len(statement.strip()) == 0:
                continue
            logging.info("STATEMENT: " + statement)
            self.run_spark_query(statement)

    def run_spark_query(self, query, return_output=False):
        """
        Execute an individual spark sql query
        """
        if return_output:
            return self.sqlContext.sql(query)
        self.sqlContext.sql(query)
