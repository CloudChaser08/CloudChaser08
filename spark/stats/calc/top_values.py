from pyspark.sql.functions import ***stuff***

def _col_top_values(c):
    '''
    Calculate the top values for a given column
    Input:
        - c: dataframe column of type pyspark.sql.Column
    Output:
        - tv: the top values for that column in descending order
              along with the count
            
              i.e. 
              ----------------------------
              |   col_1   | col_1_counts |
              +-----------+--------------+
              |   val_1   |      450     |
              |   val_2   |      400     |
              |   val_3   |      200     |
              |   val_4   |       27     |
              |    ...    |      ...     |
              |    val_n  |       1      |
              +--------------------------+
    '''

    pass


def calculate_top_values(df, max_top_values):
    '''
    Calculate the top values of a dataframe
    Input:
        - df: a pyspark.sql.DataFrame
        - max_top_values: the max number of values to 
                          store for each column
    Output:
        - top_values: a Dictionary of each columns top values
                      and its associated counts
    '''

    pass


