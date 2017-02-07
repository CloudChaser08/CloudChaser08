class Runner:

    def __init__(self, sqlContext):
        self.sqlContext = sqlContext
        self.scripts = []
        self.queries = []
        self.variables = []

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
            print "STATEMENT: " + statement
            self.sqlContext.sql(statement)

    def run_spark_query(self, query, return_output=False):
        """
        Execute an individual spark sql query
        """
        if return_output:
            return self.sqlContext.sql(query)
        self.sqlContext.sql(query)

    def enqueue_psql_script(self, script, variables=[]):
        """
        Add a sql script to the queue
        """
        self.scripts.append(script)
        self.queries.append("")
        self.variables.append(variables)

    def enqueue_psql_query(self, query, variables=[]):
        """
        Add a sql script to the queue
        """
        self.scripts.append("")
        self.queries.append(query)
        self.variables.append(variables)

    def execute_queue(self, debug=False):
        """
        Execute all scripts in the queue
        """
        for i in xrange(len(self.scripts)):
            if self.scripts[0] != "":
                self.run_spark_script(
                    self.scripts.pop(0), self.variables.pop(0)
                )
                del self.queries[0]
            else:
                self.run_spark_query(
                    self.queries.pop(0), self.variables.pop(0)
                )
                del self.scripts[0]
