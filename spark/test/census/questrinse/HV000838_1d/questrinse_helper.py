import pytest
from spark.census.questrinse.HV000838_1d.udf import udf_gen
import pyspark.sql.functions as F
from pyspark.sql import DataFrame, Row

REPLACEMENT_LOOKUP = {
    "^TNP124": "TEST NOT PERFORMED",
    "NEG": "NEGATIVE",
    "FINAL RSLT: NEGATIVE": "NEGATIVE",
    "NEGATIVE": "NEGATIVE",
    "NEGATIVE CONFIRMED": "NEGATIVE CONFIRMED",
    "NEGA CONF": "NEGATIVE CONFIRMED",
    "POS": "POSITIVE",
    "POSITIVE": "POSITIVE",
    "NONE DETECTED": "NOT DETECTED",
    "NOT DETECTED": "NOT DETECTED",
    "NON-REACTIVE": "NON-REACTIVE",
    "NONREACTIVE": "NON-REACTIVE",
    "NOT INDICATED": "NOT INDICATED",
    "CONSISTENT": "CONSISTENT",
    "INCONSISTENT": "INCONSISTENT",
    "DNRTNP": "DO NOT REPORT",
    "DNR": "DO NOT REPORT",
    "NOT INTERPRETED~DNR": "DO NOT REPORT",
    "TNP124": "TEST NOT PERFORMED",
    "DETECTED": "DETECTED",
    "REACTIVE": "REACTIVE",
    "EQUIVOCAL": "EQUIVOCAL",
    "INDETERMINATE": "INDETERMINATE",
    "INCONCLUSIVE": "INCONCLUSIVE",
    "NOT ISOLATED": "NOT ISOLATED",
    "ISOLATED": "ISOLATED",
    "NO CULTURE INDICATED": "NO CULTURE INDICATED",
    "CULTURE INDICATED": "CULTURE INDICATED",
    "INDICATED": "INDICATED",
    "EQUIOC": "EQUIVOCAL",
    "INDETERMINANT": "INDETERMINATE",
    "NEG/": "NEGATIVE",
    "POS/": "POSITIVE",
    "DETECTED ABN": "DETECTED",
    "DETECTED (A)": "DETECTED",
    "NON-DETECTED": "NOT DETECTED",
    "NO VARIANT DETECTED": "NO VARIANT DETECTED",
    "ADD^TNP167": "TEST NOT PERFORMED",
    "DTEL^TNP1003": "TEST NOT PERFORMED",
    "TNP QUANTITY NOT SUFFICIENT": "TEST NOT PERFORMED",
    "TNP124^CANC": "TEST NOT PERFORMED",
    "TA/DNR": "DO NOT REPORT",
    "DETECTED (A1)": "DETECTED",
    "NEGATI": "NEGATIVE",
    "TNP/632": "TEST NOT PERFORMED",
    "NT": "TEST NOT PERFORMED",
    "NG": "NOT GIVEN",
    "TEST NOT PERFORMED": "TEST NOT PERFORMED",
    "TNP": "TEST NOT PERFORMED",
}

def eval_test(result_value, expect_op, expect_num, expect_alpha, expect_passthru, spark):

    df: DataFrame = spark['spark'].sparkContext.parallelize([
        Row(
            result_value=result_value,
            expect_op=expect_op,
            expect_num=expect_num,
            expect_alpha=expect_alpha,
            expect_passthru=expect_passthru
        )]).toDF()

    parse_value = udf_gen(table=None, test=REPLACEMENT_LOOKUP, spark=spark)
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
