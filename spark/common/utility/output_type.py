from enum import Enum


class RunType(Enum):
    MARKETPLACE = "marketplace"
    CENSUS = "census"


class DataType(Enum):
    CDM = "cdm"
    CONSUMER = "consumer"
    CUSTOM = "custom"
    EMR = "emr"
    ERA = "era"
    IMAGE = "image"
    LAB_TESTS = "labtests"
    MEDICAL_CLAIMS = "medicalclaims"
    PHARMACY_CLAIMS = "pharmacyclaims"
    TEST_CLAIMS = "TEST_CLAIMS"
    ENROLLMENT_RECORDS = "enrollmentrecords"
    CUSTOM_MART = "custom_mart"
