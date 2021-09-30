"""
Transmed Schema
"""
from pyspark.sql.types import StructField, StructType, StringType

cancerepisode = StructType([
    StructField('pk', StringType(), True),
    StructField('patientfk', StringType(), True),
    StructField('ageatdiagnosis', StringType(), True),
    StructField('primarysite', StringType(), True),
    StructField('histology', StringType(), True),
    StructField('breastcancertype', StringType(), True),
    StructField('stage', StringType(), True),
    StructField('tumorgrade', StringType(), True)
])

treatmentsite = StructType([
        StructField('pk', StringType(), True),
        StructField('patientfk', StringType(), True),
        StructField('cancerepisodefk', StringType(), True),
        StructField('treatmentmodality', StringType(), True)
])
