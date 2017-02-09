import logging


class Runner:
    """
    Run spark queries and scripts
    """

    def __init__(self, sqlContext):
        self.sqlContext = sqlContext

    def run_spark_script(self, script, variables=[]):
        """
        Execute a spark sql script
        """
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
