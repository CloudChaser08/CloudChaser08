"""
mckesson Transactional schema
"""
from pyspark.sql.types import StringType, StructType, StructField

old_schema = StructType([
    StructField(field_name, StringType(), True) for field_name in [
        'FillerRecordID', 'PrescriptionKey', 'PatientID', 'PatientFirstName', 'PatientLastName',
        'PatientGenderCode', 'PatientAge', 'PatientDateofBirth', 'PatientAddress', 'PatientCity',
        'PatientState', 'PatientZipCode', 'DateFilled', 'DateWritten', 'FillerDateofInjury',
        'ClaimTransactionDate', 'FillerTransactionTime', 'RebillIndicator', 'CAMStatusCode',
        'RejectCode1', 'RejectCode2', 'RejectCode3', 'RejectCode4', 'RejectCode5', 'DiagnosisCode',
        'DiagnosisCodeQualifier', 'ProcedureCode', 'ProcedureCodeQualifier', 'DispensedNDCNumber',
        'FillerProductServiceID', 'ProductServiceIDQualifier', 'PrescriptionNumber',
        'PrescriptionNumberQualifier', 'BINNumber', 'ProcessControlNumber', 'RefillCode',
        'NumberofRefillsAuthorized', 'MetricQuantity', 'UnitOfMeasure', 'DaysSupply',
        'CustomerNPINumber', 'PharmacistNPI', 'PayerID', 'PayerIDQualifier', 'PayerName',
        'FillerPayerParentName', 'PayerOrgName', 'PayerPlanID', 'PlanName', 'PayerType',
        'CompoundIndicator', 'UnitDoseIndicator', 'DispensedAsWritten', 'OriginofRx',
        'SubmissionClarificationCode', 'PrescribedNDCNumber', 'PrescribedProductServiceCodeQualifier',
        'PrescribedQuantity', 'PriorAuthTypeCode', 'LevelofService', 'ReasonforService',
        'ProfessionalServiceCode', 'ResultofServiceCode', 'PrescriberNPI', 'PrimaryCareProviderID',
        'COBCount', 'UsualandCustomary', 'AmountAttribtoSalesTax', 'ProductSelectionAttributed',
        'OtherPayerAmountPaid', 'PeriodicDeductibleApplied', 'PeriodicBenefitExceed',
        'AccumulatedDeductible', 'RemainingDeductible', 'RemainingBenefit', 'CopayAmount',
        'BasisofIngredientCostSubmitted', 'IngredientCostSubmitted', 'DispensingFeeSubmitted',
        'IncentiveAmountSubmitted', 'GrossAmountDue', 'ProfessionalServiceFeeSubmitted',
        'FlatSalesTaxAmtSubmitted', 'BasisOfPercentageSalesTaxSubmitted',
        'PercentageSalesTaxRateSubmitted', 'PercentageSalesTaxAmountSubmitted',
        'PatientPayAmountSubmitted', 'OtherAmountSubmittedQualfier', 'FillerOtherAmountSubmitted',
        'BasisofIngredientCostPaid', 'IngredientCostPaid', 'DispensingFeePaid', 'IncentiveAmountPaid',
        'GrossAmountDuePaid', 'ProfessionalServiceFeePaid', 'FlatSalesTaxAmountPaid',
        'BasisOfPercentageSalesTaxPaid', 'PercentageSalesTaxRatePaid', 'PercentageSalesTaxAmountPaid',
        'PatientPayAmountPaid', 'OtherAmountPaidQualifier', 'PaidOtherClaimed', 'TaxExemptIndicator',
        'CouponType', 'CouponIDNumber', 'CouponFaceValue', 'PharmacyOtherID',
        'PharmacyOtherIDQualifier', 'CustomerZipCode', 'ProviderDispensingNPI', 'ProvDispensingQual',
        'PrimaryCareProviderID2_ignore', 'PrimaryCareProviderIDQualifier', 'OtherPayerCoverageType',
        'OtherPayerCoverageID', 'OtherPayerCoverageIDQualifier', 'OtherPayerDate',
        'OtherPayerCoverageCode', 'HvJoinKey'
    ]
])

new_schema = StructType([
    StructField(field_name, StringType(), True) for field_name in [
        'FillerRecordID', 'PrescriptionKey', 'PatientID', 'PatientFirstName', 'PatientLastName',
        'PatientGenderCode', 'PatientAge', 'PatientDateofBirth', 'PatientAddress', 'PatientCity',
        'PatientState', 'PatientZipCode', 'DateFilled', 'DateWritten', 'FillerDateofInjury',
        'ClaimTransactionDate', 'ClaimTransactionTime', 'RebillIndicator', 'CAMStatusCode',
        'RejectCode1', 'RejectCode2', 'RejectCode3', 'RejectCode4', 'RejectCode5', 'DiagnosisCode',
        'DiagnosisCodeQualifier', 'ProcedureCode', 'ProcedureCodeQualifier', 'DispensedNDCNumber',
        'FillerProductServiceID', 'ProductServiceIDQualifier', 'PrescriptionNumber',
        'PrescriptionNumberQualifier', 'BINNumber', 'ProcessControlNumber', 'RefillCode',
        'NumberofRefillsAuthorized', 'MetricQuantity', 'UnitOfMeasure', 'DaysSupply',
        'CustomerNPINumber', 'PharmacistNPI', 'PayerID', 'PayerIDQualifier', 'PayerName',
        'FillerPayerParentName', 'PayerOrgName', 'PayerPlanID', 'PlanName', 'PayerType',
        'CompoundIndicator', 'UnitDoseIndicator', 'DispensedAsWritten', 'OriginofRx',
        'SubmissionClarificationCode', 'PrescribedNDCNumber', 'PrescribedProductServiceCodeQualifier',
        'PrescribedQuantity', 'PriorAuthTypeCode', 'LevelofService', 'ReasonforService',
        'ProfessionalServiceCode', 'ResultofServiceCode', 'PrescriberNPI', 'PrimaryCareProviderID',
        'COBCount', 'UsualandCustomary', 'AmountAttribtoSalesTax', 'ProductSelectionAttributed',
        'OtherPayerAmountPaid', 'PeriodicDeductibleApplied', 'PeriodicBenefitExceed',
        'AccumulatedDeductible', 'RemainingDeductible', 'RemainingBenefit', 'CopayAmount',
        'BasisofIngredientCostSubmitted', 'IngredientCostSubmitted', 'DispensingFeeSubmitted',
        'IncentiveAmountSubmitted', 'GrossAmountDue', 'ProfessionalServiceFeeSubmitted',
        'FlatSalesTaxAmtSubmitted', 'BasisOfPercentageSalesTaxSubmitted',
        'PercentageSalesTaxRateSubmitted', 'PercentageSalesTaxAmountSubmitted',
        'PatientPayAmountSubmitted', 'OtherAmountSubmittedQualfier', 'FillerOtherAmountSubmitted',
        'BasisofIngredientCostPaid', 'IngredientCostPaid', 'IncentiveAmountPaid', 'GrossAmountDuePaid',
        'ProfessionalServiceFeePaid', 'FlatSalesTaxAmountPaid', 'BasisOfPercentageSalesTaxPaid',
        'PercentageSalesTaxRatePaid', 'PercentageSalesTaxAmountPaid', 'PatientPayAmountPaid',
        'OtherAmountPaidQualifier', 'PaidOtherClaimed', 'TaxExemptIndicator', 'CouponType',
        'CouponIDNumber', 'CouponFaceValue', 'PharmacyOtherID', 'PharmacyOtherIDQualifier',
        'CustomerZipCode', 'ProviderDispensingNPI', 'ProvDispensingQual', 'FillerPrimaryCareProviderID',
        'PrimaryCareProviderIDQualifier', 'OtherPayerCoverageType', 'OtherPayerCoverageID',
        'OtherPayerCoverageIDQualifier', 'OtherPayerDate', 'OtherPayerCoverageCode', 'HvJoinKey'
    ]
])
