create table emdeon_rx(
RecordID text,
DateAuthorized date,
TimeAuthorized text,
BINNumber text,
VersionNumber text,
TransactionCode text,
ProcessorControlNumber text,
TransactionCount text,
ServiceProviderID text,
ServiceProviderIDQualifier text,
DateOfService text,
DateOfBirth text,
PatientGenderCode text,
PatientLocation text,
PatientFirstName text,
PatientLastName text,
PatientStreetAddress text,
PatientStateProvince text,
PatientZipPostalZone text,
PatientIDQualifier text,
PatientID text,
ProviderID text,
ProviderIDQualifier text,
PrescriberID text,
PrimaryCareProviderID text,
PrescriberLastName text,
PrescriberIDQualifier text,
PrimaryCareProviderIDQualifier text,
GroupID text,
CardholderID text,
PersonCode text,
PatientRelationshipCode text,
EligibilityClarificationCode text,
HomePlan text,
CoordinationofBenefitsCount text,
OtherPayerCoverageType text,
OtherPayerIDQualifier text,
OtherPayerID text,
OtherPayerAmountPaidQualifier text,
OtherPayerAmountPaidSubmitted text,
OtherPayerDate text,
CarrierID text,
DateOfInjury text,
ClaimReferenceID text,
OtherCoverageCode text,
PrescriptionServiceReferenceNumber text,
FillNumber text,
DaysSupply text,
CompoundCode text,
ProductServiceID text distkey,
DispenseAsWritten text,
DatePrescriptionWritten text,
NumberOfRefillsAuthorized text,
LevelOfService text,
PrescriptionOriginCode text,
SubmissionClarificationCode text,
UnitDoseIndicator text,
ProductServiceIDQualifier text,
QuantityDispensed text,
OrigPrescribedProductServiceCode text,
OrigPrescribedQuantity text,
OrigPrescribedProductServiceCodeQualifier text,
PrescriptionServiceReferenceNumberQualifier text,
PriorAuthorizationTypeCode text,
UnitOfMeasure text,
ReasonForService text,
ProfessionalServiceCode text,
ResultOfServiceCode text,
CouponType text,
CouponNumber text,
CouponValueAmount text,
IngredientCostSubmitted text,
DispensingFeeSubmitted text,
BasisofCostDetermination text,
UsualAndCustomaryCharge text,
PatientPaidAmountSubmitted text,
GrossAmountDueSubmitted text,
IncentiveAmountSubmitted text,
ProfessionalServiceFeeSubmitted text,
OtherAmountClaimedSubmittedQualifier text,
OtherAmountClaimedSubmitted text,
FlatSalesTaxAmountSubmitted text,
PercentSalesTaxAmountSubmitted text,
PercentSalesTaxRateSubmitted text,
PercentSalesTaxBasisSubmitted text,
DiagnosisCode text,
DiagnosisCodeQualifier text,
ResponseCode text,
PatientPayAmount text,
IngredientCostPaid text,
DispensingFeePaid text,
TotalAmountPaid text,
AccumulatedDeductibleAmount text,
RemainingDeductibleAmount text,
RemainingBenefitAmount text,
AmountAppliedtoPeriodicDeductible text,
AmountOfCopayCoinsurance text,
AmountAttributedToProductSelection text,
AmountExceedingPeriodicBenefitMaximum text,
IncentiveAmountPaid text,
BasisofReimbursementDetermination text,
AmountAttributedtoSalesTax text,
TaxExemptIndicator text,
FlatSalesTaxAmountPaid text,
PercentageSalesTaxAmountPaid text,
PercentSalesTaxRatePaid text,
PercentSalesTaxBasisPaid text,
ProfessionalServiceFeePaid text,
OtherAmountPaidQualifier text,
OtherAmountPaid text,
OtherPayerAmountRecognized text,
PlanIdentification text,
NCPDPNumber text,
NationalProviderID text,
PlanType text,
PharmacyLocationPostalCode text,
RejectCode1 text,
RejectCode2 text,
RejectCode3 text,
RejectCode4 text,
RejectCode5 text,
PayerID text,
PayerIDQualifier text,
PlanName text,
TypeofPayment text,
EmdeonTDRClaimIDUOW_AGN text
)
