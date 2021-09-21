import pytest
from spark.census.questrinse.HV000838_1c.udf import parse_value
import pyspark.sql.functions as F
from pyspark.sql import DataFrame, Row


def eval_test(result_value, expect_op, expect_num, expect_alpha, expect_passthru, spark):

    df: DataFrame = spark['spark'].sparkContext.parallelize([
        Row(
            result_value=result_value,
            expect_op=expect_op,
            expect_num=expect_num,
            expect_alpha=expect_alpha,
            expect_passthru=expect_passthru
        )]).toDF()

    df_post: Row = df.withColumn(colName="result_arr",
                                 col=parse_value(F.col("result_value")))\
        .head(1)[0]
    operator: str
    numeric: str
    alpha: str
    passthru: str
    operator, numeric, alpha, passthru = df_post["result_arr"]

    assert expect_op == operator
    assert expect_num == numeric
    assert expect_alpha == alpha
    assert expect_passthru == passthru
