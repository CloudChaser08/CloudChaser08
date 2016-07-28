COPY gsdd_product_contraindications FROM 's3://salusv/reference/gsdd/Product_Indications.txt'
    WITH CREDENTIALS :'AWS_CREDENTIALS'
    DELIMITER '|'
    TRUNCATECOLUMNS
    COMPUPDATE ON;

CREATE TABLE gsdd_product_indications(
 ProductID text,
 GSTermID text,
 MinAge text,
 MaxAge text,
 AgeUnit text,
 Gender text,
 IsOffLabel text,
 RouteCode text,
 SectionID text
);

CREATE TABLE gsdd_product_contraindications(
 ProductID text,
 GSTermID text,
 MinAge text,
 MaxAge text,
 AgeUnits text,
 Gender text,
 IsContraindication text,
 IsBoxedWarning text,
 SectionID text
);

COPY gsdd_product_contraindications FROM 's3://salusv/reference/gsdd/Product_Contraindications.txt'
    WITH CREDENTIALS :'AWS_CREDENTIALS'
    DELIMITER '|'
    TRUNCATECOLUMNS
    COMPUPDATE ON;

CREATE TABLE gsdd_Product_CMSName (
 ProductID text,
 CMSName text
);

COPY gsdd_Product_CMSName FROM 's3://salusv/reference/gsdd/Product_CMSName.txt'
    WITH CREDENTIALS :'AWS_CREDENTIALS'
    DELIMITER '|'
    TRUNCATECOLUMNS
    COMPUPDATE ON;

CREATE TABLE gsdd_CVXCode_VaccineGroup (
 CVXCode text,
 VaccineGroup text
);

COPY gsdd_CVXCode_VaccineGroup FROM 's3://salusv/reference/gsdd/CVXCode_VaccineGroup.txt'
    WITH CREDENTIALS :'AWS_CREDENTIALS'
    DELIMITER '|'
    TRUNCATECOLUMNS
    COMPUPDATE ON;


CREATE TABLE gsdd_AGS_Beers_Strength_Of_Recommendation (
 StrengthOfRecommendationId text,
 StrengthOfRecommendationText text
);

COPY gsdd_AGS_Beers_Strength_Of_Recommendation FROM 's3://salusv/reference/gsdd/AGS_Beers_Strength_Of_Recommendation.txt'
    WITH CREDENTIALS :'AWS_CREDENTIALS'
    DELIMITER '|'
    TRUNCATECOLUMNS
    COMPUPDATE ON;




CREATE TABLE gsdd_Scoring (
 ScoringID text,
 ScoringName text
);

COPY gsdd_Scoring FROM 's3://salusv/reference/gsdd/Scoring.txt'
    WITH CREDENTIALS :'AWS_CREDENTIALS'
    DELIMITER '|'
    TRUNCATECOLUMNS
    COMPUPDATE ON;




CREATE TABLE gsdd_Nondrug_Item_Version_Color (
 NondrugItemID text,
 Version text,
 ColorID text
);

COPY gsdd_Nondrug_Item_Version_Color FROM 's3://salusv/reference/gsdd/Nondrug_Item_Version_Color.txt'
    WITH CREDENTIALS :'AWS_CREDENTIALS'
    DELIMITER '|'
    TRUNCATECOLUMNS
    COMPUPDATE ON;




CREATE TABLE gsdd_Marketed_Product_RxNorm (
 MarketedProductID text,
 RXCUI text,
 RxNormName text,
 RxNormType text
);

COPY gsdd_Marketed_Product_RxNorm FROM 's3://salusv/reference/gsdd/Marketed_Product_RxNorm.txt'
    WITH CREDENTIALS :'AWS_CREDENTIALS'
    DELIMITER '|'
    TRUNCATECOLUMNS
    COMPUPDATE ON;




CREATE TABLE gsdd_Package_Version_Drug_Item_Version (
 PackageID text,
 PackageVersion text,
 DrugItemID text,
 DrugItemVersion text,
 CompanyID text
);

COPY gsdd_Package_Version_Drug_Item_Version FROM 's3://salusv/reference/gsdd/Package_Version_Drug_Item_Version.txt'
    WITH CREDENTIALS :'AWS_CREDENTIALS'
    DELIMITER '|'
    TRUNCATECOLUMNS
    COMPUPDATE ON;




CREATE TABLE gsdd_Marketed_Product (
 MarketedProductID text,
 Name text,
 CompanyID text,
 SpecificProductID text,
 Brand text,
 Repackaged text
);

COPY gsdd_Marketed_Product FROM 's3://salusv/reference/gsdd/Marketed_Product.txt'
    WITH CREDENTIALS :'AWS_CREDENTIALS'
    DELIMITER '|'
    TRUNCATECOLUMNS
    COMPUPDATE ON;




CREATE TABLE gsdd_AGS_Beers_Disease_Syndrome_Statement (
 DiseaseSyndromeStatementId text,
 DiseaseSyndromeStatementText text,
 DiseaseSyndromeCategoryId text
);

COPY gsdd_AGS_Beers_Disease_Syndrome_Statement FROM 's3://salusv/reference/gsdd/AGS_Beers_Disease_Syndrome_Statement.txt'
    WITH CREDENTIALS :'AWS_CREDENTIALS'
    DELIMITER '|'
    TRUNCATECOLUMNS
    COMPUPDATE ON;




CREATE TABLE gsdd_Product_Strength_Route_Form (
 ProductID text,
 IngredientID text,
 IngredientName text,
 BaseIngredientID text,
 BaseIngredient text,
 TallmanName text,
 Strength text,
 StrengthUnitCode text,
 PerVolume text,
 PerVolumeUnitCode text,
 RouteID text,
 RouteName text,
 FDAFormID text,
 FDAForm text,
 GSFormID text,
 GSForm text,
 AlternativeFormID text,
 AlternativeFormName text,
 ClinicalDoseFormID text,
 ClinicalDoseFormName text,
 NormalizedFormID text,
 NormalizedFormName text,
 TopLevelDoseFormID text,
 TopLevelDoseFormName text
);

COPY gsdd_Product_Strength_Route_Form FROM 's3://salusv/reference/gsdd/Product_Strength_Route_Form.txt'
    WITH CREDENTIALS :'AWS_CREDENTIALS'
    DELIMITER '|'
    TRUNCATECOLUMNS
    COMPUPDATE ON;




CREATE TABLE gsdd_Nondrug_Item_Ingredient (
 NondrugItemID text,
 IngredientID text,
 Strength text,
 StrengthUnitCode text,
 PerVolume text,
 PerVolumeUnitCode text,
 DilutionNumerator text,
 DilutionDenominator text,
 BaseEquivalent text
);

COPY gsdd_Nondrug_Item_Ingredient FROM 's3://salusv/reference/gsdd/Nondrug_Item_Ingredient.txt'
    WITH CREDENTIALS :'AWS_CREDENTIALS'
    DELIMITER '|'
    TRUNCATECOLUMNS
    COMPUPDATE ON;




CREATE TABLE gsdd_AGS_Beers_Avoid_Caution (
 AvoidCautionId text,
 AvoidCautionText text,
 TableId text
);

COPY gsdd_AGS_Beers_Avoid_Caution FROM 's3://salusv/reference/gsdd/AGS_Beers_Avoid_Caution.txt'
    WITH CREDENTIALS :'AWS_CREDENTIALS'
    DELIMITER '|'
    TRUNCATECOLUMNS
    COMPUPDATE ON;




CREATE TABLE gsdd_Drug_Item_Version_Attribute (
 DrugItemVersionAttributeID text,
 AttributeName text
);

COPY gsdd_Drug_Item_Version_Attribute FROM 's3://salusv/reference/gsdd/Drug_Item_Version_Attribute.txt'
    WITH CREDENTIALS :'AWS_CREDENTIALS'
    DELIMITER '|'
    TRUNCATECOLUMNS
    COMPUPDATE ON;




CREATE TABLE gsdd_Package_Drug_Item (
 PackageID text,
 DrugItemID text,
 ItemCount text,
 PackageItemUnitID text
);

COPY gsdd_Package_Drug_Item FROM 's3://salusv/reference/gsdd/Package_Drug_Item.txt'
    WITH CREDENTIALS :'AWS_CREDENTIALS'
    DELIMITER '|'
    TRUNCATECOLUMNS
    COMPUPDATE ON;




CREATE TABLE gsdd_GSTerm_ICD10 (
 ICD10_Code text,
 GSTermId text
);

COPY gsdd_GSTerm_ICD10 FROM 's3://salusv/reference/gsdd/GSTerm_ICD10.txt'
    WITH CREDENTIALS :'AWS_CREDENTIALS'
    DELIMITER '|'
    TRUNCATECOLUMNS
    COMPUPDATE ON;




CREATE TABLE gsdd_Company (
 CompanyID text,
 CompanyName text,
 CompanyNameShort text,
 LabelerCode text,
 ParentCompanyID text,
 MVX text
);

COPY gsdd_Company FROM 's3://salusv/reference/gsdd/Company.txt'
    WITH CREDENTIALS :'AWS_CREDENTIALS'
    DELIMITER '|'
    TRUNCATECOLUMNS
    COMPUPDATE ON;




CREATE TABLE gsdd_Nondrug_Item_Version (
 NondrugItemID text,
 Version text,
 Description text,
 CoatingID text,
 FlavorID text,
 ShapeID text,
 ScoringID text
);

COPY gsdd_Nondrug_Item_Version FROM 's3://salusv/reference/gsdd/Nondrug_Item_Version.txt'
    WITH CREDENTIALS :'AWS_CREDENTIALS'
    DELIMITER '|'
    TRUNCATECOLUMNS
    COMPUPDATE ON;




CREATE TABLE gsdd_Ingredient_RxNorm_UNII (
 IngredientID text,
 RxNormCode text,
 UniiCode text
);

COPY gsdd_Ingredient_RxNorm_UNII FROM 's3://salusv/reference/gsdd/Ingredient_RxNorm_UNII.txt'
    WITH CREDENTIALS :'AWS_CREDENTIALS'
    DELIMITER '|'
    TRUNCATECOLUMNS
    COMPUPDATE ON;




CREATE TABLE gsdd_Script_Form (
 ScriptFormID text,
 NCPDPScriptFormCode text,
 FormName text
);

COPY gsdd_Script_Form FROM 's3://salusv/reference/gsdd/Script_Form.txt'
    WITH CREDENTIALS :'AWS_CREDENTIALS'
    DELIMITER '|'
    TRUNCATECOLUMNS
    COMPUPDATE ON;




CREATE TABLE gsdd_Product_SSMS (
 ProductID text,
 SpecificProductID text,
 BrandGenericStatusID text,
 SSMSTypeID text,
 SourceCount text
);

COPY gsdd_Product_SSMS FROM 's3://salusv/reference/gsdd/Product_SSMS.txt'
    WITH CREDENTIALS :'AWS_CREDENTIALS'
    DELIMITER '|'
    TRUNCATECOLUMNS
    COMPUPDATE ON;




CREATE TABLE gsdd_AGS_Beers_Modifier (
 ModifierId text,
 ModifierType text,
 ModifierText text
);

COPY gsdd_AGS_Beers_Modifier FROM 's3://salusv/reference/gsdd/AGS_Beers_Modifier.txt'
    WITH CREDENTIALS :'AWS_CREDENTIALS'
    DELIMITER '|'
    TRUNCATECOLUMNS
    COMPUPDATE ON;




CREATE TABLE gsdd_DESI_Status (
 DESIStatusID text,
 StatusName text,
 CMSID text
);

COPY gsdd_DESI_Status FROM 's3://salusv/reference/gsdd/DESI_Status.txt'
    WITH CREDENTIALS :'AWS_CREDENTIALS'
    DELIMITER '|'
    TRUNCATECOLUMNS
    COMPUPDATE ON;




CREATE TABLE gsdd_Product_Branded_Name_Stub (
 ProductID text,
 BrandedNameStubID text,
 Defaults text
);

COPY gsdd_Product_Branded_Name_Stub FROM 's3://salusv/reference/gsdd/Product_Branded_Name_Stub.txt'
    WITH CREDENTIALS :'AWS_CREDENTIALS'
    DELIMITER '|'
    TRUNCATECOLUMNS
    COMPUPDATE ON;




CREATE TABLE gsdd_DEA_Classification (
 DEAClassificationID text,
 Classification text
);

COPY gsdd_DEA_Classification FROM 's3://salusv/reference/gsdd/DEA_Classification.txt'
    WITH CREDENTIALS :'AWS_CREDENTIALS'
    DELIMITER '|'
    TRUNCATECOLUMNS
    COMPUPDATE ON;




CREATE TABLE gsdd_Package (
 PackageID text,
 NDC10 text,
 NDC11 text,
 UPC text,
 NHRIC text,
 GTIN12 text,
 GTIN14 text,
 PIN text,
 NCPDPExceptionalCount text,
 NCPDPBillingUnitID text,
 NCPDPScriptFormCode text,
 OuterPackageUnitID text,
 InnerPackageCount text,
 InnerPackageUnitID text,
 ReplacedByPackageID text,
 ReplacedDate text,
 ProductID text,
 PackageDescription text,
 PackageDescriptorText text,
 PreservativeFree text,
 OffMarket text,
 LotExpiry text,
 PackageOnMarketDate text,
 PackageSize text,
 NotSplittable text,
 ShortCycleDispensing text,
 ShortCycleDispensingIsManual text,
 MinimumDispenseQuantity text,
 MinimumDispenseQuantityIsManual text
);

COPY gsdd_Package FROM 's3://salusv/reference/gsdd/Package.txt'
    WITH CREDENTIALS :'AWS_CREDENTIALS'
    DELIMITER '|'
    TRUNCATECOLUMNS
    COMPUPDATE ON;




CREATE TABLE gsdd_AGS_Beers_Disease_Syndrome_Category (
 DiseaseSyndromeCategoryId text,
 DiseaseSyndromeCategoryText text
);

COPY gsdd_AGS_Beers_Disease_Syndrome_Category FROM 's3://salusv/reference/gsdd/AGS_Beers_Disease_Syndrome_Category.txt'
    WITH CREDENTIALS :'AWS_CREDENTIALS'
    DELIMITER '|'
    TRUNCATECOLUMNS
    COMPUPDATE ON;




CREATE TABLE gsdd_Product_Ingredient_Hazardous_Waste_Code (
 ProductID text,
 IngredientID text,
 Substance text,
 P_Code text,
 P_CodeQualifier text,
 P_CodeHazardousWasteCode text,
 F_Code text,
 F_CodeHazardousWasteCode text,
 K_Code text,
 K_CodeHazardousWasteCode text,
 U_Code text,
 U_CodeHazardousWasteCode text,
 U_CodeQualifier text
);

COPY gsdd_Product_Ingredient_Hazardous_Waste_Code FROM 's3://salusv/reference/gsdd/Product_Ingredient_Hazardous_Waste_Code.txt'
    WITH CREDENTIALS :'AWS_CREDENTIALS'
    DELIMITER '|'
    TRUNCATECOLUMNS
    COMPUPDATE ON;




CREATE TABLE gsdd_GSTerm_SNOMED (
 SNOMED_Code text,
 GSTermId text
);

COPY gsdd_GSTerm_SNOMED FROM 's3://salusv/reference/gsdd/GSTerm_SNOMED.txt'
    WITH CREDENTIALS :'AWS_CREDENTIALS'
    DELIMITER '|'
    TRUNCATECOLUMNS
    COMPUPDATE ON;




CREATE TABLE gsdd_Product_CVXCode (
 ProductID text,
 CVXCode text
);

COPY gsdd_Product_CVXCode FROM 's3://salusv/reference/gsdd/Product_CVXCode.txt'
    WITH CREDENTIALS :'AWS_CREDENTIALS'
    DELIMITER '|'
    TRUNCATECOLUMNS
    COMPUPDATE ON;




CREATE TABLE gsdd_Product_Therapeutic_Equivalence (
 ProductID text,
 EquivalenceCode text
);

COPY gsdd_Product_Therapeutic_Equivalence FROM 's3://salusv/reference/gsdd/Product_Therapeutic_Equivalence.txt'
    WITH CREDENTIALS :'AWS_CREDENTIALS'
    DELIMITER '|'
    TRUNCATECOLUMNS
    COMPUPDATE ON;




CREATE TABLE gsdd_ATC_Active_Composition_Generic_Group (
 ATCCode text,
 ActiveCompositionGenericGroupID text
);

COPY gsdd_ATC_Active_Composition_Generic_Group FROM 's3://salusv/reference/gsdd/ATC_Active_Composition_Generic_Group.txt'
    WITH CREDENTIALS :'AWS_CREDENTIALS'
    DELIMITER '|'
    TRUNCATECOLUMNS
    COMPUPDATE ON;




CREATE TABLE gsdd_Therapeutic_Concept (
 TherapeuticConceptID text,
 ConceptName text
);

COPY gsdd_Therapeutic_Concept FROM 's3://salusv/reference/gsdd/Therapeutic_Concept.txt'
    WITH CREDENTIALS :'AWS_CREDENTIALS'
    DELIMITER '|'
    TRUNCATECOLUMNS
    COMPUPDATE ON;




CREATE TABLE gsdd_AGS_Beers_Quality_Of_Evidence (
 QualityOfEvidenceId text,
 QualityOfEvidenceText text
);

COPY gsdd_AGS_Beers_Quality_Of_Evidence FROM 's3://salusv/reference/gsdd/AGS_Beers_Quality_Of_Evidence.txt'
    WITH CREDENTIALS :'AWS_CREDENTIALS'
    DELIMITER '|'
    TRUNCATECOLUMNS
    COMPUPDATE ON;




CREATE TABLE gsdd_Imprint_Location (
 ImprintLocationID text,
 ImprintLocationDescription text
);

COPY gsdd_Imprint_Location FROM 's3://salusv/reference/gsdd/Imprint_Location.txt'
    WITH CREDENTIALS :'AWS_CREDENTIALS'
    DELIMITER '|'
    TRUNCATECOLUMNS
    COMPUPDATE ON;




CREATE TABLE gsdd_Product_Name_Type (
 ProductNameTypeID text,
 TypeName text
);

COPY gsdd_Product_Name_Type FROM 's3://salusv/reference/gsdd/Product_Name_Type.txt'
    WITH CREDENTIALS :'AWS_CREDENTIALS'
    DELIMITER '|'
    TRUNCATECOLUMNS
    COMPUPDATE ON;




CREATE TABLE gsdd_License_Type (
 LicenseTypeID text,
 LicenseTypeName text,
 LicenseTypeDescription text
);

COPY gsdd_License_Type FROM 's3://salusv/reference/gsdd/License_Type.txt'
    WITH CREDENTIALS :'AWS_CREDENTIALS'
    DELIMITER '|'
    TRUNCATECOLUMNS
    COMPUPDATE ON;




CREATE TABLE gsdd_Outer_Package_Unit (
 OuterPackageUnitID text,
 UnitName text
);

COPY gsdd_Outer_Package_Unit FROM 's3://salusv/reference/gsdd/Outer_Package_Unit.txt'
    WITH CREDENTIALS :'AWS_CREDENTIALS'
    DELIMITER '|'
    TRUNCATECOLUMNS
    COMPUPDATE ON;




CREATE TABLE gsdd_Product (
 ProductID text,
 NDC9 text,
 ProductNameLong text,
 ProductNameTypeID text,
 ProductNameShort text,
 ePrescribingName text,
 MarketerID text,
 DESIStatusID text,
 DEAClassificationID text,
 BrandGenericStatusID text,
 LegendStatusID text,
 MarketedProductID text,
 CP_NUM text,
 OffMarketDate text,
 ReplacedByProductID text,
 LicenseTypeID text,
 Repackaged text,
 Innovator text,
 PrivateLabel text,
 ProductOnMarketDate text,
 BulkChemical text
);

COPY gsdd_Product FROM 's3://salusv/reference/gsdd/Product.txt'
    WITH CREDENTIALS :'AWS_CREDENTIALS'
    DELIMITER '|'
    TRUNCATECOLUMNS
    COMPUPDATE ON;




CREATE TABLE gsdd_Drug_Item_Version (
 DrugItemID text,
 Version text,
 Description text,
 CoatingID text,
 FlavorID text,
 ShapeID text,
 ScoringID text
);

COPY gsdd_Drug_Item_Version FROM 's3://salusv/reference/gsdd/Drug_Item_Version.txt'
    WITH CREDENTIALS :'AWS_CREDENTIALS'
    DELIMITER '|'
    TRUNCATECOLUMNS
    COMPUPDATE ON;




CREATE TABLE gsdd_Imprint (
 DrugItemID text,
 Version text,
 ImprintLocationID text,
 ImprintText text
);

COPY gsdd_Imprint FROM 's3://salusv/reference/gsdd/Imprint.txt'
    WITH CREDENTIALS :'AWS_CREDENTIALS'
    DELIMITER '|'
    TRUNCATECOLUMNS
    COMPUPDATE ON;




CREATE TABLE gsdd_Therapeutic_Equivalence (
 EquivalenceCode text,
 EquivalenceDescription text
);

COPY gsdd_Therapeutic_Equivalence FROM 's3://salusv/reference/gsdd/Therapeutic_Equivalence.txt'
    WITH CREDENTIALS :'AWS_CREDENTIALS'
    DELIMITER '|'
    TRUNCATECOLUMNS
    COMPUPDATE ON;




CREATE TABLE gsdd_AGS_Beers_Rationale (
 RationalId text,
 RationalText text,
 AvoidCautionId text
);

COPY gsdd_AGS_Beers_Rationale FROM 's3://salusv/reference/gsdd/AGS_Beers_Rationale.txt'
    WITH CREDENTIALS :'AWS_CREDENTIALS'
    DELIMITER '|'
    TRUNCATECOLUMNS
    COMPUPDATE ON;




CREATE TABLE gsdd_GSTerm_ICD9 (
 ICD9_Code text,
 GSTermId text
);

COPY gsdd_GSTerm_ICD9 FROM 's3://salusv/reference/gsdd/GSTerm_ICD9.txt'
    WITH CREDENTIALS :'AWS_CREDENTIALS'
    DELIMITER '|'
    TRUNCATECOLUMNS
    COMPUPDATE ON;




CREATE TABLE gsdd_Color (
 ColorID text,
 ColorName text
);

COPY gsdd_Color FROM 's3://salusv/reference/gsdd/Color.txt'
    WITH CREDENTIALS :'AWS_CREDENTIALS'
    DELIMITER '|'
    TRUNCATECOLUMNS
    COMPUPDATE ON;




CREATE TABLE gsdd_Drug_Item_Version_Color (
 DrugItemID text,
 Version text,
 ColorID text
);

COPY gsdd_Drug_Item_Version_Color FROM 's3://salusv/reference/gsdd/Drug_Item_Version_Color.txt'
    WITH CREDENTIALS :'AWS_CREDENTIALS'
    DELIMITER '|'
    TRUNCATECOLUMNS
    COMPUPDATE ON;




CREATE TABLE gsdd_Shape (
 ShapeID text,
 ShapeName text
);

COPY gsdd_Shape FROM 's3://salusv/reference/gsdd/Shape.txt'
    WITH CREDENTIALS :'AWS_CREDENTIALS'
    DELIMITER '|'
    TRUNCATECOLUMNS
    COMPUPDATE ON;




CREATE TABLE gsdd_Unit (
 UnitCode text,
 UnitName text
);

COPY gsdd_Unit FROM 's3://salusv/reference/gsdd/Unit.txt'
    WITH CREDENTIALS :'AWS_CREDENTIALS'
    DELIMITER '|'
    TRUNCATECOLUMNS
    COMPUPDATE ON;




CREATE TABLE gsdd__GSDD_TableList (
 leName text,
 Description text,
 ModuleName text
);

COPY gsdd__GSDD_TableList FROM 's3://salusv/reference/gsdd/_GSDD_TableList.txt'
    WITH CREDENTIALS :'AWS_CREDENTIALS'
    DELIMITER '|'
    TRUNCATECOLUMNS
    COMPUPDATE ON;




CREATE TABLE gsdd_Product_Storage (
 ProductID text,
 StorageID text
);

COPY gsdd_Product_Storage FROM 's3://salusv/reference/gsdd/Product_Storage.txt'
    WITH CREDENTIALS :'AWS_CREDENTIALS'
    DELIMITER '|'
    TRUNCATECOLUMNS
    COMPUPDATE ON;




CREATE TABLE gsdd_Branded_Name_Stub (
 BrandedNameStubID text,
 BrandedNameStub text
);

COPY gsdd_Branded_Name_Stub FROM 's3://salusv/reference/gsdd/Branded_Name_Stub.txt'
    WITH CREDENTIALS :'AWS_CREDENTIALS'
    DELIMITER '|'
    TRUNCATECOLUMNS
    COMPUPDATE ON;




CREATE TABLE gsdd_NCPDP_Unit (
 NCPDPUnitID text,
 UnitName text
);

COPY gsdd_NCPDP_Unit FROM 's3://salusv/reference/gsdd/NCPDP_Unit.txt'
    WITH CREDENTIALS :'AWS_CREDENTIALS'
    DELIMITER '|'
    TRUNCATECOLUMNS
    COMPUPDATE ON;




CREATE TABLE gsdd_Package_CMSName (
 PackageID text,
 CMSName text
);

COPY gsdd_Package_CMSName FROM 's3://salusv/reference/gsdd/Package_CMSName.txt'
    WITH CREDENTIALS :'AWS_CREDENTIALS'
    DELIMITER '|'
    TRUNCATECOLUMNS
    COMPUPDATE ON;




CREATE TABLE gsdd_Nondrug_Item (
 NondrugItemID text,
 ItemName text,
 ProductID text,
 GSFormID text,
 FDAFormID text,
 ImplicitRouteID text,
 SpecificNondrugItemID text
);

COPY gsdd_Nondrug_Item FROM 's3://salusv/reference/gsdd/Nondrug_Item.txt'
    WITH CREDENTIALS :'AWS_CREDENTIALS'
    DELIMITER '|'
    TRUNCATECOLUMNS
    COMPUPDATE ON;




CREATE TABLE gsdd_Package_Nondrug_Item (
 PackageID text,
 NondrugItemID text,
 ItemCount text,
 PackageItemUnitID text
);

COPY gsdd_Package_Nondrug_Item FROM 's3://salusv/reference/gsdd/Package_Nondrug_Item.txt'
    WITH CREDENTIALS :'AWS_CREDENTIALS'
    DELIMITER '|'
    TRUNCATECOLUMNS
    COMPUPDATE ON;




CREATE TABLE gsdd_Nondrug_Item_Version_Attributes (
 NondrugItemID text,
 Version text,
 NondrugItemVersionAttributeID text
);

COPY gsdd_Nondrug_Item_Version_Attributes FROM 's3://salusv/reference/gsdd/Nondrug_Item_Version_Attributes.txt'
    WITH CREDENTIALS :'AWS_CREDENTIALS'
    DELIMITER '|'
    TRUNCATECOLUMNS
    COMPUPDATE ON;




CREATE TABLE gsdd_Therapeutic_Concept_Tree (
 TherapeuticConceptTreeID text,
 ParentConceptTreeID text,
 TherapeuticConceptID text
);

COPY gsdd_Therapeutic_Concept_Tree FROM 's3://salusv/reference/gsdd/Therapeutic_Concept_Tree.txt'
    WITH CREDENTIALS :'AWS_CREDENTIALS'
    DELIMITER '|'
    TRUNCATECOLUMNS
    COMPUPDATE ON;




CREATE TABLE gsdd_AGS_Beers_Therapeutic_Category_Organ_System (
 TherapeuticCategoryOrganSystemId text,
 TherapeuticCategoryOrganSystemText text
);

COPY gsdd_AGS_Beers_Therapeutic_Category_Organ_System FROM 's3://salusv/reference/gsdd/AGS_Beers_Therapeutic_Category_Organ_System.txt'
    WITH CREDENTIALS :'AWS_CREDENTIALS'
    DELIMITER '|'
    TRUNCATECOLUMNS
    COMPUPDATE ON;




CREATE TABLE gsdd_NonSplittable_Product (
 MarketedProductID text,
 ProductID text,
 OrderableName text,
 NonSplittable text
);

COPY gsdd_NonSplittable_Product FROM 's3://salusv/reference/gsdd/NonSplittable_Product.txt'
    WITH CREDENTIALS :'AWS_CREDENTIALS'
    DELIMITER '|'
    TRUNCATECOLUMNS
    COMPUPDATE ON;




CREATE TABLE gsdd_GS_Form (
 GSFormID text,
 FormName text
);

COPY gsdd_GS_Form FROM 's3://salusv/reference/gsdd/GS_Form.txt'
    WITH CREDENTIALS :'AWS_CREDENTIALS'
    DELIMITER '|'
    TRUNCATECOLUMNS
    COMPUPDATE ON;




CREATE TABLE gsdd_Package_Version_Nondrug_Item_Version (
 PackageID text,
 PackageVersion text,
 NondrugItemID text,
 NondrugItemVersion text,
 CompanyID text
);

COPY gsdd_Package_Version_Nondrug_Item_Version FROM 's3://salusv/reference/gsdd/Package_Version_Nondrug_Item_Version.txt'
    WITH CREDENTIALS :'AWS_CREDENTIALS'
    DELIMITER '|'
    TRUNCATECOLUMNS
    COMPUPDATE ON;




CREATE TABLE gsdd_Storage (
 StorageID text,
 StorageConcept text
);

COPY gsdd_Storage FROM 's3://salusv/reference/gsdd/Storage.txt'
    WITH CREDENTIALS :'AWS_CREDENTIALS'
    DELIMITER '|'
    TRUNCATECOLUMNS
    COMPUPDATE ON;




CREATE TABLE gsdd_Nondrug_Item_Version_Attribute (
 NondrugItemVersionAttributeID text,
 AttributeName text
);

COPY gsdd_Nondrug_Item_Version_Attribute FROM 's3://salusv/reference/gsdd/Nondrug_Item_Version_Attribute.txt'
    WITH CREDENTIALS :'AWS_CREDENTIALS'
    DELIMITER '|'
    TRUNCATECOLUMNS
    COMPUPDATE ON;




CREATE TABLE gsdd_Active_Composition_Generic_Group_Active_Composition_Generic (
 ActiveCompositionGenericGroupID text,
 ActiveCompositionGenericID text
);

COPY gsdd_Active_Composition_Generic_Group_Active_Composition_Generic FROM 's3://salusv/reference/gsdd/Active_Composition_Generic_Group_Active_Composition_Generic.txt'
    WITH CREDENTIALS :'AWS_CREDENTIALS'
    DELIMITER '|'
    TRUNCATECOLUMNS
    COMPUPDATE ON;




CREATE TABLE gsdd_AGS_Beers_Table (
 TableId text,
 TableName text,
 TableDescription text
);

COPY gsdd_AGS_Beers_Table FROM 's3://salusv/reference/gsdd/AGS_Beers_Table.txt'
    WITH CREDENTIALS :'AWS_CREDENTIALS'
    DELIMITER '|'
    TRUNCATECOLUMNS
    COMPUPDATE ON;




CREATE TABLE gsdd_AGS_Beers_Criteria_Item (
 AGSBeersCriteriaItemId text,
 AGSBeersCriteriaGenericProductId text,
 RationalId text,
 QualityOfEvidenceId text,
 StrengthOfRecommendationId text,
 TherapeuticCategoryOrganSystemId text,
 DiseaseSyndromeStatementId text,
 ModifierAId text,
 ModifierBId text,
 ProfessionalNotes text
);

COPY gsdd_AGS_Beers_Criteria_Item FROM 's3://salusv/reference/gsdd/AGS_Beers_Criteria_Item.txt'
    WITH CREDENTIALS :'AWS_CREDENTIALS'
    DELIMITER '|'
    TRUNCATECOLUMNS
    COMPUPDATE ON;




CREATE TABLE gsdd_Maintenance_Medications (
 PackageID text,
 ProductID text,
 MarketedProductID text,
 SpecificProductID text,
 NDC11 text
);

COPY gsdd_Maintenance_Medications FROM 's3://salusv/reference/gsdd/Maintenance_Medications.txt'
    WITH CREDENTIALS :'AWS_CREDENTIALS'
    DELIMITER '|'
    TRUNCATECOLUMNS
    COMPUPDATE ON;




CREATE TABLE gsdd_Ingredient_Name (
 IngredientID text,
 IngredientName text,
 TallmanName text,
 BaseIngredientID text,
 isBaseIngredient text
);

COPY gsdd_Ingredient_Name FROM 's3://salusv/reference/gsdd/Ingredient_Name.txt'
    WITH CREDENTIALS :'AWS_CREDENTIALS'
    DELIMITER '|'
    TRUNCATECOLUMNS
    COMPUPDATE ON;




CREATE TABLE gsdd_Product_Generic_Name_Stub (
 ProductID text,
 GenericNameLong text,
 GenericNameShort text,
 GenericSynonym text
);

COPY gsdd_Product_Generic_Name_Stub FROM 's3://salusv/reference/gsdd/Product_Generic_Name_Stub.txt'
    WITH CREDENTIALS :'AWS_CREDENTIALS'
    DELIMITER '|'
    TRUNCATECOLUMNS
    COMPUPDATE ON;




CREATE TABLE gsdd_Product_Ingredient_Federal_Narcotic (
 ProductID text,
 IngredientID text
);

COPY gsdd_Product_Ingredient_Federal_Narcotic FROM 's3://salusv/reference/gsdd/Product_Ingredient_Federal_Narcotic.txt'
    WITH CREDENTIALS :'AWS_CREDENTIALS'
    DELIMITER '|'
    TRUNCATECOLUMNS
    COMPUPDATE ON;




CREATE TABLE gsdd_Drug_Item_Active_Ingredient (
 DrugItemID text,
 IngredientID text,
 Strength text,
 StrengthUnitCode text,
 PerVolume text,
 PerVolumeUnitCode text,
 DilutionNumerator text,
 DilutionDenominator text,
 BaseEquivalent text
);

COPY gsdd_Drug_Item_Active_Ingredient FROM 's3://salusv/reference/gsdd/Drug_Item_Active_Ingredient.txt'
    WITH CREDENTIALS :'AWS_CREDENTIALS'
    DELIMITER '|'
    TRUNCATECOLUMNS
    COMPUPDATE ON;




CREATE TABLE gsdd_Package_Item_Unit (
 PackageItemUnitID text,
 UnitName text
);

COPY gsdd_Package_Item_Unit FROM 's3://salusv/reference/gsdd/Package_Item_Unit.txt'
    WITH CREDENTIALS :'AWS_CREDENTIALS'
    DELIMITER '|'
    TRUNCATECOLUMNS
    COMPUPDATE ON;




CREATE TABLE gsdd_Implicit_Route (
 ImplicitRouteID text,
 Route text
);

COPY gsdd_Implicit_Route FROM 's3://salusv/reference/gsdd/Implicit_Route.txt'
    WITH CREDENTIALS :'AWS_CREDENTIALS'
    DELIMITER '|'
    TRUNCATECOLUMNS
    COMPUPDATE ON;




CREATE TABLE gsdd_Legend_Status (
 LegendStatusID text,
 StatusName text
);

COPY gsdd_Legend_Status FROM 's3://salusv/reference/gsdd/Legend_Status.txt'
    WITH CREDENTIALS :'AWS_CREDENTIALS'
    DELIMITER '|'
    TRUNCATECOLUMNS
    COMPUPDATE ON;




CREATE TABLE gsdd_Route_Of_Administration (
 RouteID text,
 RouteName text,
 SnomedCode text,
 SigRouteSynonym text,
 SigAbbreviation text
);

COPY gsdd_Route_Of_Administration FROM 's3://salusv/reference/gsdd/Route_Of_Administration.txt'
    WITH CREDENTIALS :'AWS_CREDENTIALS'
    DELIMITER '|'
    TRUNCATECOLUMNS
    COMPUPDATE ON;




CREATE TABLE gsdd_Package_Version (
 PackageIdentifier text,
 PackageIdentifierType text,
 FormattedPackageIdentifier text,
 PackageID text,
 Version text,
 NCPDPExceptionalCount text,
 NCPDPBillingUnitID text,
 NCPDPScriptFormCode text,
 OuterPackageUnitID text,
 InnerPackageCount text,
 InnerPackageUnitID text,
 ReplacedByPackageID text,
 ReplacedDate text,
 ProductID text,
 PackageVersionDescription text,
 PackageDescriptorText text,
 PreservativeFree text,
 PackageVersionOnMarketDate text,
 PackageVersionOffMarketDate text,
 UnformattedNDC10 text,
 PackageSize text
);

COPY gsdd_Package_Version FROM 's3://salusv/reference/gsdd/Package_Version.txt'
    WITH CREDENTIALS :'AWS_CREDENTIALS'
    DELIMITER '|'
    TRUNCATECOLUMNS
    COMPUPDATE ON;




CREATE TABLE gsdd_Drug_Item_Version_Attributes (
 DrugItemID text,
 Version text,
 DrugItemVersionAttributeID text
);

COPY gsdd_Drug_Item_Version_Attributes FROM 's3://salusv/reference/gsdd/Drug_Item_Version_Attributes.txt'
    WITH CREDENTIALS :'AWS_CREDENTIALS'
    DELIMITER '|'
    TRUNCATECOLUMNS
    COMPUPDATE ON;




CREATE TABLE gsdd_Flavor (
 FlavorID text,
 FlavorName text
);

COPY gsdd_Flavor FROM 's3://salusv/reference/gsdd/Flavor.txt'
    WITH CREDENTIALS :'AWS_CREDENTIALS'
    DELIMITER '|'
    TRUNCATECOLUMNS
    COMPUPDATE ON;




CREATE TABLE gsdd_Therapeutic_Concept_Tree_Specific_Product (
 TherapeuticConceptTreeID text,
 SpecificProductID text
);

COPY gsdd_Therapeutic_Concept_Tree_Specific_Product FROM 's3://salusv/reference/gsdd/Therapeutic_Concept_Tree_Specific_Product.txt'
    WITH CREDENTIALS :'AWS_CREDENTIALS'
    DELIMITER '|'
    TRUNCATECOLUMNS
    COMPUPDATE ON;




CREATE TABLE gsdd_GSTerms (
 GSTermId text,
 Name text,
 ConsumerDefinition text,
 AllowedForIndication text,
 AllowedForContraindication text,
 AllowedForAdverseReaction text
);

COPY gsdd_GSTerms FROM 's3://salusv/reference/gsdd/GSTerms.txt'
    WITH CREDENTIALS :'AWS_CREDENTIALS'
    DELIMITER '|'
    TRUNCATECOLUMNS
    COMPUPDATE ON;




CREATE TABLE gsdd_NonSplittable_Package (
 MarketedProductID text,
 PackageID text,
 OrderableName text,
 NonSplittable text
);

COPY gsdd_NonSplittable_Package FROM 's3://salusv/reference/gsdd/NonSplittable_Package.txt'
    WITH CREDENTIALS :'AWS_CREDENTIALS'
    DELIMITER '|'
    TRUNCATECOLUMNS
    COMPUPDATE ON;




CREATE TABLE gsdd_FDA_Form_NCIt_Term (
 FDAFormID text,
 FDAID text,
 FormName text,
 FormDescription text,
 FormIndex text,
 NCItQuantityUnitTermID text,
 NCItStrengthFormTermID text,
 NCItId text,
 NCItSubsetCode text,
 NCPDPSubsetPreferredTerm text,
 NCItCode text,
 NCPDPPreferredTerm text,
 NCItPreferredTerm text,
 NCItDefinition text
);

COPY gsdd_FDA_Form_NCIt_Term FROM 's3://salusv/reference/gsdd/FDA_Form_NCIt_Term.txt'
    WITH CREDENTIALS :'AWS_CREDENTIALS'
    DELIMITER '|'
    TRUNCATECOLUMNS
    COMPUPDATE ON;




CREATE TABLE gsdd_SSMS_Type (
 SSMSTypeID text,
 SourceType text
);

COPY gsdd_SSMS_Type FROM 's3://salusv/reference/gsdd/SSMS_Type.txt'
    WITH CREDENTIALS :'AWS_CREDENTIALS'
    DELIMITER '|'
    TRUNCATECOLUMNS
    COMPUPDATE ON;




CREATE TABLE gsdd_CVXCode (
 CVXCode text,
 DescriptionShort text,
 DescriptionLong text,
 VaccineStatus text,
 Notes text,
 LastUpdated text
);

COPY gsdd_CVXCode FROM 's3://salusv/reference/gsdd/CVXCode.txt'
    WITH CREDENTIALS :'AWS_CREDENTIALS'
    DELIMITER '|'
    TRUNCATECOLUMNS
    COMPUPDATE ON;




CREATE TABLE gsdd_Product_Attribute (
 ProductAttributeID text,
 AttributeName text
);

COPY gsdd_Product_Attribute FROM 's3://salusv/reference/gsdd/Product_Attribute.txt'
    WITH CREDENTIALS :'AWS_CREDENTIALS'
    DELIMITER '|'
    TRUNCATECOLUMNS
    COMPUPDATE ON;




CREATE TABLE gsdd_Product_Product_Attribute (
 ProductID text,
 ProductAttributeID text
);

COPY gsdd_Product_Product_Attribute FROM 's3://salusv/reference/gsdd/Product_Product_Attribute.txt'
    WITH CREDENTIALS :'AWS_CREDENTIALS'
    DELIMITER '|'
    TRUNCATECOLUMNS
    COMPUPDATE ON;




CREATE TABLE gsdd_ATC_Specific_Product (
 ATCCode text,
 SpecificProductId text
);

COPY gsdd_ATC_Specific_Product FROM 's3://salusv/reference/gsdd/ATC_Specific_Product.txt'
    WITH CREDENTIALS :'AWS_CREDENTIALS'
    DELIMITER '|'
    TRUNCATECOLUMNS
    COMPUPDATE ON;




CREATE TABLE gsdd_Package_SSMS (
 PackageID text,
 SpecificProductID text,
 BrandGenericStatusID text,
 SSMSTypeID text,
 SourceCount text
);

COPY gsdd_Package_SSMS FROM 's3://salusv/reference/gsdd/Package_SSMS.txt'
    WITH CREDENTIALS :'AWS_CREDENTIALS'
    DELIMITER '|'
    TRUNCATECOLUMNS
    COMPUPDATE ON;




CREATE TABLE gsdd_AGS_Beers_Criteria_Generic_Product_Product (
 ProductID text,
 AGSBeersCriteriaGenericProductId text,
 GenericProductId text,
 GenericProductClinicalID text
);

COPY gsdd_AGS_Beers_Criteria_Generic_Product_Product FROM 's3://salusv/reference/gsdd/AGS_Beers_Criteria_Generic_Product_Product.txt'
    WITH CREDENTIALS :'AWS_CREDENTIALS'
    DELIMITER '|'
    TRUNCATECOLUMNS
    COMPUPDATE ON;




CREATE TABLE gsdd_Drug_Item (
 DrugItemID text,
 ProductID text,
 ItemNameLong text,
 GSFormID text,
 FDAFormID text,
 ImplicitRouteID text,
 SpecificDrugItemID text
);

COPY gsdd_Drug_Item FROM 's3://salusv/reference/gsdd/Drug_Item.txt'
    WITH CREDENTIALS :'AWS_CREDENTIALS'
    DELIMITER '|'
    TRUNCATECOLUMNS
    COMPUPDATE ON;




CREATE TABLE gsdd_Nondrug_Item_Route (
 NondrugItemID text,
 RouteID text
);

COPY gsdd_Nondrug_Item_Route FROM 's3://salusv/reference/gsdd/Nondrug_Item_Route.txt'
    WITH CREDENTIALS :'AWS_CREDENTIALS'
    DELIMITER '|'
    TRUNCATECOLUMNS
    COMPUPDATE ON;




CREATE TABLE gsdd_FDA_Form (
 FDAFormID text,
 FormName text
);

COPY gsdd_FDA_Form FROM 's3://salusv/reference/gsdd/FDA_Form.txt'
    WITH CREDENTIALS :'AWS_CREDENTIALS'
    DELIMITER '|'
    TRUNCATECOLUMNS
    COMPUPDATE ON;




CREATE TABLE gsdd_Therapeutic_Concept_Tree_DENORM (
 TherapeuticConceptTreeID text,
 TherapeuticConceptID text,
 Parent1 text,
 Parent2 text,
 Parent3 text,
 Parent4 text,
 Parent5 text,
 Parent6 text
);

COPY gsdd_Therapeutic_Concept_Tree_DENORM FROM 's3://salusv/reference/gsdd/Therapeutic_Concept_Tree_DENORM.txt'
    WITH CREDENTIALS :'AWS_CREDENTIALS'
    DELIMITER '|'
    TRUNCATECOLUMNS
    COMPUPDATE ON;




CREATE TABLE gsdd_Coating (
 CoatingID text,
 CoatingName text
);

COPY gsdd_Coating FROM 's3://salusv/reference/gsdd/Coating.txt'
    WITH CREDENTIALS :'AWS_CREDENTIALS'
    DELIMITER '|'
    TRUNCATECOLUMNS
    COMPUPDATE ON;




CREATE TABLE gsdd_Inner_Package_Unit (
 InnerPackageUnitID text,
 UnitName text
);

COPY gsdd_Inner_Package_Unit FROM 's3://salusv/reference/gsdd/Inner_Package_Unit.txt'
    WITH CREDENTIALS :'AWS_CREDENTIALS'
    DELIMITER '|'
    TRUNCATECOLUMNS
    COMPUPDATE ON;




CREATE TABLE gsdd_Brand_Generic_Status (
 BrandGenericStatusID text,
 StatusName text
);

COPY gsdd_Brand_Generic_Status FROM 's3://salusv/reference/gsdd/Brand_Generic_Status.txt'
    WITH CREDENTIALS :'AWS_CREDENTIALS'
    DELIMITER '|'
    TRUNCATECOLUMNS
    COMPUPDATE ON;




CREATE TABLE gsdd_Drug_Item_Route (
 DrugItemID text,
 RouteID text
);

COPY gsdd_Drug_Item_Route FROM 's3://salusv/reference/gsdd/Drug_Item_Route.txt'
    WITH CREDENTIALS :'AWS_CREDENTIALS'
    DELIMITER '|'
    TRUNCATECOLUMNS
    COMPUPDATE ON;




