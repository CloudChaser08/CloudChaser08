DROP TABLE IF EXISTS ref_gsdd_product_indications;
DROP TABLE IF EXISTS ref_gsdd_product_contraindications;
DROP TABLE IF EXISTS ref_gsdd_Product_CMSName;
DROP TABLE IF EXISTS ref_gsdd_CVXCode_VaccineGroup;
DROP TABLE IF EXISTS ref_gsdd_AGS_Beers_Strength_Of_Recommendation;
DROP TABLE IF EXISTS ref_gsdd_Scoring;
DROP TABLE IF EXISTS ref_gsdd_Nondrug_Item_Version_Color;
DROP TABLE IF EXISTS ref_gsdd_Marketed_Product_RxNorm;
DROP TABLE IF EXISTS ref_gsdd_Package_Version_Drug_Item_Version;
DROP TABLE IF EXISTS ref_gsdd_Marketed_Product;
DROP TABLE IF EXISTS ref_gsdd_AGS_Beers_Disease_Syndrome_Statement;
DROP TABLE IF EXISTS ref_gsdd_Product_Strength_Route_Form;
DROP TABLE IF EXISTS ref_gsdd_Nondrug_Item_Ingredient;
DROP TABLE IF EXISTS ref_gsdd_AGS_Beers_Avoid_Caution;
DROP TABLE IF EXISTS ref_gsdd_Drug_Item_Version_Attribute;
DROP TABLE IF EXISTS ref_gsdd_Package_Drug_Item;
DROP TABLE IF EXISTS ref_gsdd_GSTerm_ICD10;
DROP TABLE IF EXISTS ref_gsdd_Company;
DROP TABLE IF EXISTS ref_gsdd_Nondrug_Item_Version;
DROP TABLE IF EXISTS ref_gsdd_Ingredient_RxNorm_UNII;
DROP TABLE IF EXISTS ref_gsdd_Script_Form;
DROP TABLE IF EXISTS ref_gsdd_Product_SSMS;
DROP TABLE IF EXISTS ref_gsdd_AGS_Beers_Modifier;
DROP TABLE IF EXISTS ref_gsdd_DESI_Status;
DROP TABLE IF EXISTS ref_gsdd_Product_Branded_Name_Stub;
DROP TABLE IF EXISTS ref_gsdd_DEA_Classification;
DROP TABLE IF EXISTS ref_gsdd_Package;
DROP TABLE IF EXISTS ref_gsdd_AGS_Beers_Disease_Syndrome_Category;
DROP TABLE IF EXISTS ref_gsdd_Product_Ingredient_Hazardous_Waste_Code;
DROP TABLE IF EXISTS ref_gsdd_GSTerm_SNOMED;
DROP TABLE IF EXISTS ref_gsdd_Product_CVXCode;
DROP TABLE IF EXISTS ref_gsdd_Product_Therapeutic_Equivalence;
DROP TABLE IF EXISTS ref_gsdd_ATC_Active_Composition_Generic_Group;
DROP TABLE IF EXISTS ref_gsdd_Therapeutic_Concept;
DROP TABLE IF EXISTS ref_gsdd_AGS_Beers_Quality_Of_Evidence;
DROP TABLE IF EXISTS ref_gsdd_Imprint_Location;
DROP TABLE IF EXISTS ref_gsdd_Product_Name_Type;
DROP TABLE IF EXISTS ref_gsdd_License_Type;
DROP TABLE IF EXISTS ref_gsdd_Outer_Package_Unit;
DROP TABLE IF EXISTS ref_gsdd_Product;
DROP TABLE IF EXISTS ref_gsdd_Drug_Item_Version;
DROP TABLE IF EXISTS ref_gsdd_Imprint;
DROP TABLE IF EXISTS ref_gsdd_Therapeutic_Equivalence;
DROP TABLE IF EXISTS ref_gsdd_AGS_Beers_Rationale;
DROP TABLE IF EXISTS ref_gsdd_GSTerm_ICD9;
DROP TABLE IF EXISTS ref_gsdd_Color;
DROP TABLE IF EXISTS ref_gsdd_Drug_Item_Version_Color;
DROP TABLE IF EXISTS ref_gsdd_Shape;
DROP TABLE IF EXISTS ref_gsdd_Unit;
DROP TABLE IF EXISTS ref_gsdd__GSDD_TableList;
DROP TABLE IF EXISTS ref_gsdd_Product_Storage;
DROP TABLE IF EXISTS ref_gsdd_Branded_Name_Stub;
DROP TABLE IF EXISTS ref_gsdd_NCPDP_Unit;
DROP TABLE IF EXISTS ref_gsdd_Package_CMSName;
DROP TABLE IF EXISTS ref_gsdd_Nondrug_Item;
DROP TABLE IF EXISTS ref_gsdd_Package_Nondrug_Item;
DROP TABLE IF EXISTS ref_gsdd_Nondrug_Item_Version_Attributes;
DROP TABLE IF EXISTS ref_gsdd_Therapeutic_Concept_Tree;
DROP TABLE IF EXISTS ref_gsdd_AGS_Beers_Therapeutic_Category_Organ_System;
DROP TABLE IF EXISTS ref_gsdd_NonSplittable_Product;
DROP TABLE IF EXISTS ref_gsdd_GS_Form;
DROP TABLE IF EXISTS ref_gsdd_Package_Version_Nondrug_Item_Version;
DROP TABLE IF EXISTS ref_gsdd_Storage;
DROP TABLE IF EXISTS ref_gsdd_Nondrug_Item_Version_Attribute;
DROP TABLE IF EXISTS ref_gsdd_Active_Composition_Generic_Group_Active_Composition_Generic;
DROP TABLE IF EXISTS ref_gsdd_AGS_Beers_Table;
DROP TABLE IF EXISTS ref_gsdd_AGS_Beers_Criteria_Item;
DROP TABLE IF EXISTS ref_gsdd_Maintenance_Medications;
DROP TABLE IF EXISTS ref_gsdd_Ingredient_Name;
DROP TABLE IF EXISTS ref_gsdd_Product_Generic_Name_Stub;
DROP TABLE IF EXISTS ref_gsdd_Product_Ingredient_Federal_Narcotic;
DROP TABLE IF EXISTS ref_gsdd_Drug_Item_Active_Ingredient;
DROP TABLE IF EXISTS ref_gsdd_Package_Item_Unit;
DROP TABLE IF EXISTS ref_gsdd_Implicit_Route;
DROP TABLE IF EXISTS ref_gsdd_Legend_Status;
DROP TABLE IF EXISTS ref_gsdd_Route_Of_Administration;
DROP TABLE IF EXISTS ref_gsdd_Package_Version;
DROP TABLE IF EXISTS ref_gsdd_Drug_Item_Version_Attributes;
DROP TABLE IF EXISTS ref_gsdd_Flavor;
DROP TABLE IF EXISTS ref_gsdd_Therapeutic_Concept_Tree_Specific_Product;
DROP TABLE IF EXISTS ref_gsdd_GSTerms;
DROP TABLE IF EXISTS ref_gsdd_NonSplittable_Package;
DROP TABLE IF EXISTS ref_gsdd_FDA_Form_NCIt_Term;
DROP TABLE IF EXISTS ref_gsdd_SSMS_Type;
DROP TABLE IF EXISTS ref_gsdd_CVXCode;
DROP TABLE IF EXISTS ref_gsdd_Product_Attribute;
DROP TABLE IF EXISTS ref_gsdd_Product_Product_Attribute;
DROP TABLE IF EXISTS ref_gsdd_ATC_Specific_Product;
DROP TABLE IF EXISTS ref_gsdd_Package_SSMS;
DROP TABLE IF EXISTS ref_gsdd_AGS_Beers_Criteria_Generic_Product_Product;
DROP TABLE IF EXISTS ref_gsdd_Drug_Item;
DROP TABLE IF EXISTS ref_gsdd_Nondrug_Item_Route;
DROP TABLE IF EXISTS ref_gsdd_FDA_Form;
DROP TABLE IF EXISTS ref_gsdd_Therapeutic_Concept_Tree_DENORM;
DROP TABLE IF EXISTS ref_gsdd_Coating;
DROP TABLE IF EXISTS ref_gsdd_Inner_Package_Unit;
DROP TABLE IF EXISTS ref_gsdd_Brand_Generic_Status;
DROP TABLE IF EXISTS ref_gsdd_Drug_Item_Route;

CREATE TABLE ref_gsdd_product_indications(
 ProductID string,
 GSTermID string,
 MinAge string,
 MaxAge string,
 AgeUnit string,
 Gender string,
 IsOffLabel string,
 RouteCode string,
 SectionID string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
LOCATION 's3a://salusv/reference/gsdd/Product_Indications/'
;

CREATE TABLE ref_gsdd_product_contraindications(
 ProductID string,
 GSTermID string,
 MinAge string,
 MaxAge string,
 AgeUnits string,
 Gender string,
 IsContraindication string,
 IsBoxedWarning string,
 SectionID string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
LOCATION 's3a://salusv/reference/gsdd/Product_Contraindications/'
;

CREATE TABLE ref_gsdd_Product_CMSName (
 ProductID string,
 CMSName string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
LOCATION 's3a://salusv/reference/gsdd/Product_CMSName/'
;

CREATE TABLE ref_gsdd_CVXCode_VaccineGroup (
 CVXCode string,
 VaccineGroup string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
LOCATION 's3a://salusv/reference/gsdd/CVXCode_VaccineGroup/'
;

CREATE TABLE ref_gsdd_AGS_Beers_Strength_Of_Recommendation (
 StrengthOfRecommendationId string,
 StrengthOfRecommendationText string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
LOCATION 's3a://salusv/reference/gsdd/AGS_Beers_Strength_Of_Recommendation/'
;

CREATE TABLE ref_gsdd_Scoring (
 ScoringID string,
 ScoringName string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
LOCATION 's3a://salusv/reference/gsdd/Scoring/'

;

CREATE TABLE ref_gsdd_Nondrug_Item_Version_Color (
 NondrugItemID string,
 Version string,
 ColorID string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
LOCATION 's3a://salusv/reference/gsdd/Nondrug_Item_Version_Color/'
;

CREATE TABLE ref_gsdd_Marketed_Product_RxNorm (
 MarketedProductID string,
 RXCUI string,
 RxNormName string,
 RxNormType string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
LOCATION 's3a://salusv/reference/gsdd/Marketed_Product_RxNorm/'
;

CREATE TABLE ref_gsdd_Package_Version_Drug_Item_Version (
 PackageID string,
 PackageVersion string,
 DrugItemID string,
 DrugItemVersion string,
 CompanyID string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
LOCATION 's3a://salusv/reference/gsdd/Package_Version_Drug_Item_Version/'
;

CREATE TABLE ref_gsdd_Marketed_Product (
 MarketedProductID string,
 Name string,
 CompanyID string,
 SpecificProductID string,
 Brand string,
 Repackaged string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
LOCATION 's3a://salusv/reference/gsdd/Marketed_Product/'
;

CREATE TABLE ref_gsdd_AGS_Beers_Disease_Syndrome_Statement (
 DiseaseSyndromeStatementId string,
 DiseaseSyndromeStatementText string,
 DiseaseSyndromeCategoryId string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
LOCATION 's3a://salusv/reference/gsdd/AGS_Beers_Disease_Syndrome_Statement/'
;

CREATE TABLE ref_gsdd_Product_Strength_Route_Form (
 ProductID string,
 IngredientID string,
 IngredientName string,
 BaseIngredientID string,
 BaseIngredient string,
 TallmanName string,
 Strength string,
 StrengthUnitCode string,
 PerVolume string,
 PerVolumeUnitCode string,
 RouteID string,
 RouteName string,
 FDAFormID string,
 FDAForm string,
 GSFormID string,
 GSForm string,
 AlternativeFormID string,
 AlternativeFormName string,
 ClinicalDoseFormID string,
 ClinicalDoseFormName string,
 NormalizedFormID string,
 NormalizedFormName string,
 TopLevelDoseFormID string,
 TopLevelDoseFormName string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
LOCATION 's3a://salusv/reference/gsdd/Product_Strength_Route_Form/'

;

CREATE TABLE ref_gsdd_Nondrug_Item_Ingredient (
 NondrugItemID string,
 IngredientID string,
 Strength string,
 StrengthUnitCode string,
 PerVolume string,
 PerVolumeUnitCode string,
 DilutionNumerator string,
 DilutionDenominator string,
 BaseEquivalent string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
LOCATION 's3a://salusv/reference/gsdd/Nondrug_Item_Ingredient/'
;

CREATE TABLE ref_gsdd_AGS_Beers_Avoid_Caution (
 AvoidCautionId string,
 AvoidCautionText string,
 TableId string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
LOCATION 's3a://salusv/reference/gsdd/AGS_Beers_Avoid_Caution/'

;

CREATE TABLE ref_gsdd_Drug_Item_Version_Attribute (
 DrugItemVersionAttributeID string,
 AttributeName string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
LOCATION 's3a://salusv/reference/gsdd/Drug_Item_Version_Attribute/'
;

CREATE TABLE ref_gsdd_Package_Drug_Item (
 PackageID string,
 DrugItemID string,
 ItemCount string,
 PackageItemUnitID string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
LOCATION 's3a://salusv/reference/gsdd/Package_Drug_Item/'
;

CREATE TABLE ref_gsdd_GSTerm_ICD10 (
 ICD10_Code string,
 GSTermId string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
LOCATION 's3a://salusv/reference/gsdd/GSTerm_ICD10/'
;

CREATE TABLE ref_gsdd_Company (
 CompanyID string,
 CompanyName string,
 CompanyNameShort string,
 LabelerCode string,
 ParentCompanyID string,
 MVX string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
LOCATION 's3a://salusv/reference/gsdd/Company/'
;

CREATE TABLE ref_gsdd_Nondrug_Item_Version (
 NondrugItemID string,
 Version string,
 Description string,
 CoatingID string,
 FlavorID string,
 ShapeID string,
 ScoringID string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
LOCATION 's3a://salusv/reference/gsdd/Nondrug_Item_Version/'
;

CREATE TABLE ref_gsdd_Ingredient_RxNorm_UNII (
 IngredientID string,
 RxNormCode string,
 UniiCode string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
LOCATION 's3a://salusv/reference/gsdd/Ingredient_RxNorm_UNII/'
    

;

CREATE TABLE ref_gsdd_Script_Form (
 ScriptFormID string,
 NCPDPScriptFormCode string,
 FormName string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
LOCATION 's3a://salusv/reference/gsdd/Script_Form/'
    

;

CREATE TABLE ref_gsdd_Product_SSMS (
 ProductID string,
 SpecificProductID string,
 BrandGenericStatusID string,
 SSMSTypeID string,
 SourceCount string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
LOCATION 's3a://salusv/reference/gsdd/Product_SSMS/'
    

;

CREATE TABLE ref_gsdd_AGS_Beers_Modifier (
 ModifierId string,
 ModifierType string,
 ModifierText string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
LOCATION 's3a://salusv/reference/gsdd/AGS_Beers_Modifier/'
    

;

CREATE TABLE ref_gsdd_DESI_Status (
 DESIStatusID string,
 StatusName string,
 CMSID string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
LOCATION 's3a://salusv/reference/gsdd/DESI_Status/'
    

;

CREATE TABLE ref_gsdd_Product_Branded_Name_Stub (
 ProductID string,
 BrandedNameStubID string,
 Defaults string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
LOCATION 's3a://salusv/reference/gsdd/Product_Branded_Name_Stub/'
    

;

CREATE TABLE ref_gsdd_DEA_Classification (
 DEAClassificationID string,
 Classification string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
LOCATION 's3a://salusv/reference/gsdd/DEA_Classification/'
    

;

CREATE TABLE ref_gsdd_Package (
 PackageID string,
 NDC10 string,
 NDC11 string,
 UPC string,
 NHRIC string,
 GTIN12 string,
 GTIN14 string,
 PIN string,
 NCPDPExceptionalCount string,
 NCPDPBillingUnitID string,
 NCPDPScriptFormCode string,
 OuterPackageUnitID string,
 InnerPackageCount string,
 InnerPackageUnitID string,
 ReplacedByPackageID string,
 ReplacedDate string,
 ProductID string,
 PackageDescription string,
 PackageDescriptorText string,
 PreservativeFree string,
 OffMarket string,
 LotExpiry string,
 PackageOnMarketDate string,
 PackageSize string,
 NotSplittable string,
 ShortCycleDispensing string,
 ShortCycleDispensingIsManual string,
 MinimumDispenseQuantity string,
 MinimumDispenseQuantityIsManual string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
LOCATION 's3a://salusv/reference/gsdd/Package/'
    

;

CREATE TABLE ref_gsdd_AGS_Beers_Disease_Syndrome_Category (
 DiseaseSyndromeCategoryId string,
 DiseaseSyndromeCategoryText string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
LOCATION 's3a://salusv/reference/gsdd/AGS_Beers_Disease_Syndrome_Category/'
    

;

CREATE TABLE ref_gsdd_Product_Ingredient_Hazardous_Waste_Code (
 ProductID string,
 IngredientID string,
 Substance string,
 P_Code string,
 P_CodeQualifier string,
 P_CodeHazardousWasteCode string,
 F_Code string,
 F_CodeHazardousWasteCode string,
 K_Code string,
 K_CodeHazardousWasteCode string,
 U_Code string,
 U_CodeHazardousWasteCode string,
 U_CodeQualifier string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
LOCATION 's3a://salusv/reference/gsdd/Product_Ingredient_Hazardous_Waste_Code/'
    

;

CREATE TABLE ref_gsdd_GSTerm_SNOMED (
 SNOMED_Code string,
 GSTermId string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
LOCATION 's3a://salusv/reference/gsdd/GSTerm_SNOMED/'
    

;

CREATE TABLE ref_gsdd_Product_CVXCode (
 ProductID string,
 CVXCode string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
LOCATION 's3a://salusv/reference/gsdd/Product_CVXCode/'
    

;

CREATE TABLE ref_gsdd_Product_Therapeutic_Equivalence (
 ProductID string,
 EquivalenceCode string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
LOCATION 's3a://salusv/reference/gsdd/Product_Therapeutic_Equivalence/'
    

;

CREATE TABLE ref_gsdd_ATC_Active_Composition_Generic_Group (
 ATCCode string,
 ActiveCompositionGenericGroupID string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
LOCATION 's3a://salusv/reference/gsdd/ATC_Active_Composition_Generic_Group/'
    

;

CREATE TABLE ref_gsdd_Therapeutic_Concept (
 TherapeuticConceptID string,
 ConceptName string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
LOCATION 's3a://salusv/reference/gsdd/Therapeutic_Concept/'
    

;

CREATE TABLE ref_gsdd_AGS_Beers_Quality_Of_Evidence (
 QualityOfEvidenceId string,
 QualityOfEvidenceText string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
LOCATION 's3a://salusv/reference/gsdd/AGS_Beers_Quality_Of_Evidence/'
    

;

CREATE TABLE ref_gsdd_Imprint_Location (
 ImprintLocationID string,
 ImprintLocationDescription string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
LOCATION 's3a://salusv/reference/gsdd/Imprint_Location/'
    

;

CREATE TABLE ref_gsdd_Product_Name_Type (
 ProductNameTypeID string,
 TypeName string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
LOCATION 's3a://salusv/reference/gsdd/Product_Name_Type/'
    

;

CREATE TABLE ref_gsdd_License_Type (
 LicenseTypeID string,
 LicenseTypeName string,
 LicenseTypeDescription string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
LOCATION 's3a://salusv/reference/gsdd/License_Type/'
    

;

CREATE TABLE ref_gsdd_Outer_Package_Unit (
 OuterPackageUnitID string,
 UnitName string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
LOCATION 's3a://salusv/reference/gsdd/Outer_Package_Unit/'
    

;

CREATE TABLE ref_gsdd_Product (
 ProductID string,
 NDC9 string,
 ProductNameLong string,
 ProductNameTypeID string,
 ProductNameShort string,
 ePrescribingName string,
 MarketerID string,
 DESIStatusID string,
 DEAClassificationID string,
 BrandGenericStatusID string,
 LegendStatusID string,
 MarketedProductID string,
 CP_NUM string,
 OffMarketDate string,
 ReplacedByProductID string,
 LicenseTypeID string,
 Repackaged string,
 Innovator string,
 PrivateLabel string,
 ProductOnMarketDate string,
 BulkChemical string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
LOCATION 's3a://salusv/reference/gsdd/Product/'
    

;

CREATE TABLE ref_gsdd_Drug_Item_Version (
 DrugItemID string,
 Version string,
 Description string,
 CoatingID string,
 FlavorID string,
 ShapeID string,
 ScoringID string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
LOCATION 's3a://salusv/reference/gsdd/Drug_Item_Version/'
    

;

CREATE TABLE ref_gsdd_Imprint (
 DrugItemID string,
 Version string,
 ImprintLocationID string,
 ImprintText string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
LOCATION 's3a://salusv/reference/gsdd/Imprint/'
    

;

CREATE TABLE ref_gsdd_Therapeutic_Equivalence (
 EquivalenceCode string,
 EquivalenceDescription string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
LOCATION 's3a://salusv/reference/gsdd/Therapeutic_Equivalence/'
    

;

CREATE TABLE ref_gsdd_AGS_Beers_Rationale (
 RationalId string,
 RationalText string,
 AvoidCautionId string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
LOCATION 's3a://salusv/reference/gsdd/AGS_Beers_Rationale/'
    

;

CREATE TABLE ref_gsdd_GSTerm_ICD9 (
 ICD9_Code string,
 GSTermId string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
LOCATION 's3a://salusv/reference/gsdd/GSTerm_ICD9/'
    

;

CREATE TABLE ref_gsdd_Color (
 ColorID string,
 ColorName string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
LOCATION 's3a://salusv/reference/gsdd/Color/'
    

;

CREATE TABLE ref_gsdd_Drug_Item_Version_Color (
 DrugItemID string,
 Version string,
 ColorID string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
LOCATION 's3a://salusv/reference/gsdd/Drug_Item_Version_Color/'
    

;

CREATE TABLE ref_gsdd_Shape (
 ShapeID string,
 ShapeName string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
LOCATION 's3a://salusv/reference/gsdd/Shape/'
    

;

CREATE TABLE ref_gsdd_Unit (
 UnitCode string,
 UnitName string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
LOCATION 's3a://salusv/reference/gsdd/Unit/'
    

;

CREATE TABLE ref_gsdd__GSDD_TableList (
 leName string,
 Description string,
 ModuleName string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
LOCATION 's3a://salusv/reference/gsdd/_GSDD_TableList/'
    

;

CREATE TABLE ref_gsdd_Product_Storage (
 ProductID string,
 StorageID string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
LOCATION 's3a://salusv/reference/gsdd/Product_Storage/'
    

;

CREATE TABLE ref_gsdd_Branded_Name_Stub (
 BrandedNameStubID string,
 BrandedNameStub string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
LOCATION 's3a://salusv/reference/gsdd/Branded_Name_Stub/'
    

;

CREATE TABLE ref_gsdd_NCPDP_Unit (
 NCPDPUnitID string,
 UnitName string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
LOCATION 's3a://salusv/reference/gsdd/NCPDP_Unit/'
    

;

CREATE TABLE ref_gsdd_Package_CMSName (
 PackageID string,
 CMSName string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
LOCATION 's3a://salusv/reference/gsdd/Package_CMSName/'
    

;

CREATE TABLE ref_gsdd_Nondrug_Item (
 NondrugItemID string,
 ItemName string,
 ProductID string,
 GSFormID string,
 FDAFormID string,
 ImplicitRouteID string,
 SpecificNondrugItemID string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
LOCATION 's3a://salusv/reference/gsdd/Nondrug_Item/'
    

;

CREATE TABLE ref_gsdd_Package_Nondrug_Item (
 PackageID string,
 NondrugItemID string,
 ItemCount string,
 PackageItemUnitID string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
LOCATION 's3a://salusv/reference/gsdd/Package_Nondrug_Item/'
    

;

CREATE TABLE ref_gsdd_Nondrug_Item_Version_Attributes (
 NondrugItemID string,
 Version string,
 NondrugItemVersionAttributeID string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
LOCATION 's3a://salusv/reference/gsdd/Nondrug_Item_Version_Attributes/'
    

;

CREATE TABLE ref_gsdd_Therapeutic_Concept_Tree (
 TherapeuticConceptTreeID string,
 ParentConceptTreeID string,
 TherapeuticConceptID string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
LOCATION 's3a://salusv/reference/gsdd/Therapeutic_Concept_Tree/'
    

;

CREATE TABLE ref_gsdd_AGS_Beers_Therapeutic_Category_Organ_System (
 TherapeuticCategoryOrganSystemId string,
 TherapeuticCategoryOrganSystemText string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
LOCATION 's3a://salusv/reference/gsdd/AGS_Beers_Therapeutic_Category_Organ_System/'
    

;

CREATE TABLE ref_gsdd_NonSplittable_Product (
 MarketedProductID string,
 ProductID string,
 OrderableName string,
 NonSplittable string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
LOCATION 's3a://salusv/reference/gsdd/NonSplittable_Product/'
    

;

CREATE TABLE ref_gsdd_GS_Form (
 GSFormID string,
 FormName string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
LOCATION 's3a://salusv/reference/gsdd/GS_Form/'
    

;

CREATE TABLE ref_gsdd_Package_Version_Nondrug_Item_Version (
 PackageID string,
 PackageVersion string,
 NondrugItemID string,
 NondrugItemVersion string,
 CompanyID string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
LOCATION 's3a://salusv/reference/gsdd/Package_Version_Nondrug_Item_Version/'
    

;

CREATE TABLE ref_gsdd_Storage (
 StorageID string,
 StorageConcept string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
LOCATION 's3a://salusv/reference/gsdd/Storage/'
    

;

CREATE TABLE ref_gsdd_Nondrug_Item_Version_Attribute (
 NondrugItemVersionAttributeID string,
 AttributeName string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
LOCATION 's3a://salusv/reference/gsdd/Nondrug_Item_Version_Attribute/'
    

;

CREATE TABLE ref_gsdd_Active_Composition_Generic_Group_Active_Composition_Generic (
 ActiveCompositionGenericGroupID string,
 ActiveCompositionGenericID string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
LOCATION 's3a://salusv/reference/gsdd/Active_Composition_Generic_Group_Active_Composition_Generic/'
    

;

CREATE TABLE ref_gsdd_AGS_Beers_Table (
 TableId string,
 TableName string,
 TableDescription string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
LOCATION 's3a://salusv/reference/gsdd/AGS_Beers_Table/'
    

;

CREATE TABLE ref_gsdd_AGS_Beers_Criteria_Item (
 AGSBeersCriteriaItemId string,
 AGSBeersCriteriaGenericProductId string,
 RationalId string,
 QualityOfEvidenceId string,
 StrengthOfRecommendationId string,
 TherapeuticCategoryOrganSystemId string,
 DiseaseSyndromeStatementId string,
 ModifierAId string,
 ModifierBId string,
 ProfessionalNotes string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
LOCATION 's3a://salusv/reference/gsdd/AGS_Beers_Criteria_Item/'
    

;

CREATE TABLE ref_gsdd_Maintenance_Medications (
 PackageID string,
 ProductID string,
 MarketedProductID string,
 SpecificProductID string,
 NDC11 string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
LOCATION 's3a://salusv/reference/gsdd/Maintenance_Medications/'
    

;

CREATE TABLE ref_gsdd_Ingredient_Name (
 IngredientID string,
 IngredientName string,
 TallmanName string,
 BaseIngredientID string,
 isBaseIngredient string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
LOCATION 's3a://salusv/reference/gsdd/Ingredient_Name/'
    

;

CREATE TABLE ref_gsdd_Product_Generic_Name_Stub (
 ProductID string,
 GenericNameLong string,
 GenericNameShort string,
 GenericSynonym string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
LOCATION 's3a://salusv/reference/gsdd/Product_Generic_Name_Stub/'
    

;

CREATE TABLE ref_gsdd_Product_Ingredient_Federal_Narcotic (
 ProductID string,
 IngredientID string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
LOCATION 's3a://salusv/reference/gsdd/Product_Ingredient_Federal_Narcotic/'
    

;

CREATE TABLE ref_gsdd_Drug_Item_Active_Ingredient (
 DrugItemID string,
 IngredientID string,
 Strength string,
 StrengthUnitCode string,
 PerVolume string,
 PerVolumeUnitCode string,
 DilutionNumerator string,
 DilutionDenominator string,
 BaseEquivalent string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
LOCATION 's3a://salusv/reference/gsdd/Drug_Item_Active_Ingredient/'
    

;

CREATE TABLE ref_gsdd_Package_Item_Unit (
 PackageItemUnitID string,
 UnitName string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
LOCATION 's3a://salusv/reference/gsdd/Package_Item_Unit/'
    

;

CREATE TABLE ref_gsdd_Implicit_Route (
 ImplicitRouteID string,
 Route string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
LOCATION 's3a://salusv/reference/gsdd/Implicit_Route/'
    

;

CREATE TABLE ref_gsdd_Legend_Status (
 LegendStatusID string,
 StatusName string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
LOCATION 's3a://salusv/reference/gsdd/Legend_Status/'
    

;

CREATE TABLE ref_gsdd_Route_Of_Administration (
 RouteID string,
 RouteName string,
 SnomedCode string,
 SigRouteSynonym string,
 SigAbbreviation string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
LOCATION 's3a://salusv/reference/gsdd/Route_Of_Administration/'
    

;

CREATE TABLE ref_gsdd_Package_Version (
 PackageIdentifier string,
 PackageIdentifierType string,
 FormattedPackageIdentifier string,
 PackageID string,
 Version string,
 NCPDPExceptionalCount string,
 NCPDPBillingUnitID string,
 NCPDPScriptFormCode string,
 OuterPackageUnitID string,
 InnerPackageCount string,
 InnerPackageUnitID string,
 ReplacedByPackageID string,
 ReplacedDate string,
 ProductID string,
 PackageVersionDescription string,
 PackageDescriptorText string,
 PreservativeFree string,
 PackageVersionOnMarketDate string,
 PackageVersionOffMarketDate string,
 UnformattedNDC10 string,
 PackageSize string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
LOCATION 's3a://salusv/reference/gsdd/Package_Version/'
    

;

CREATE TABLE ref_gsdd_Drug_Item_Version_Attributes (
 DrugItemID string,
 Version string,
 DrugItemVersionAttributeID string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
LOCATION 's3a://salusv/reference/gsdd/Drug_Item_Version_Attributes/'
    

;

CREATE TABLE ref_gsdd_Flavor (
 FlavorID string,
 FlavorName string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
LOCATION 's3a://salusv/reference/gsdd/Flavor/'
    

;

CREATE TABLE ref_gsdd_Therapeutic_Concept_Tree_Specific_Product (
 TherapeuticConceptTreeID string,
 SpecificProductID string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
LOCATION 's3a://salusv/reference/gsdd/Therapeutic_Concept_Tree_Specific_Product/'
    

;

CREATE TABLE ref_gsdd_GSTerms (
 GSTermId string,
 Name string,
 ConsumerDefinition string,
 AllowedForIndication string,
 AllowedForContraindication string,
 AllowedForAdverseReaction string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
LOCATION 's3a://salusv/reference/gsdd/GSTerms/'
    

;

CREATE TABLE ref_gsdd_NonSplittable_Package (
 MarketedProductID string,
 PackageID string,
 OrderableName string,
 NonSplittable string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
LOCATION 's3a://salusv/reference/gsdd/NonSplittable_Package/'
    

;

CREATE TABLE ref_gsdd_FDA_Form_NCIt_Term (
 FDAFormID string,
 FDAID string,
 FormName string,
 FormDescription string,
 FormIndex string,
 NCItQuantityUnitTermID string,
 NCItStrengthFormTermID string,
 NCItId string,
 NCItSubsetCode string,
 NCPDPSubsetPreferredTerm string,
 NCItCode string,
 NCPDPPreferredTerm string,
 NCItPreferredTerm string,
 NCItDefinition string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
LOCATION 's3a://salusv/reference/gsdd/FDA_Form_NCIt_Term/'
    

;

CREATE TABLE ref_gsdd_SSMS_Type (
 SSMSTypeID string,
 SourceType string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
LOCATION 's3a://salusv/reference/gsdd/SSMS_Type/'
    

;

CREATE TABLE ref_gsdd_CVXCode (
 CVXCode string,
 DescriptionShort string,
 DescriptionLong string,
 VaccineStatus string,
 Notes string,
 LastUpdated string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
LOCATION 's3a://salusv/reference/gsdd/CVXCode/'
    

;

CREATE TABLE ref_gsdd_Product_Attribute (
 ProductAttributeID string,
 AttributeName string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
LOCATION 's3a://salusv/reference/gsdd/Product_Attribute/'
    

;

CREATE TABLE ref_gsdd_Product_Product_Attribute (
 ProductID string,
 ProductAttributeID string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
LOCATION 's3a://salusv/reference/gsdd/Product_Product_Attribute/'
    

;

CREATE TABLE ref_gsdd_ATC_Specific_Product (
 ATCCode string,
 SpecificProductId string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
LOCATION 's3a://salusv/reference/gsdd/ATC_Specific_Product/'
;

CREATE TABLE ref_gsdd_Package_SSMS (
 PackageID string,
 SpecificProductID string,
 BrandGenericStatusID string,
 SSMSTypeID string,
 SourceCount string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
LOCATION 's3a://salusv/reference/gsdd/Package_SSMS/'
;

CREATE TABLE ref_gsdd_AGS_Beers_Criteria_Generic_Product_Product (
 ProductID string,
 AGSBeersCriteriaGenericProductId string,
 GenericProductId string,
 GenericProductClinicalID string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
LOCATION 's3a://salusv/reference/gsdd/AGS_Beers_Criteria_Generic_Product_Product/'
;

CREATE TABLE ref_gsdd_Drug_Item (
 DrugItemID string,
 ProductID string,
 ItemNameLong string,
 GSFormID string,
 FDAFormID string,
 ImplicitRouteID string,
 SpecificDrugItemID string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
LOCATION 's3a://salusv/reference/gsdd/Drug_Item/'
;

CREATE TABLE ref_gsdd_Nondrug_Item_Route (
 NondrugItemID string,
 RouteID string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
LOCATION 's3a://salusv/reference/gsdd/Nondrug_Item_Route/'
;

CREATE TABLE ref_gsdd_FDA_Form (
 FDAFormID string,
 FormName string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
LOCATION 's3a://salusv/reference/gsdd/FDA_Form/'
;

CREATE TABLE ref_gsdd_Therapeutic_Concept_Tree_DENORM (
 TherapeuticConceptTreeID string,
 TherapeuticConceptID string,
 Parent1 string,
 Parent2 string,
 Parent3 string,
 Parent4 string,
 Parent5 string,
 Parent6 string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
LOCATION 's3a://salusv/reference/gsdd/Therapeutic_Concept_Tree_DENORM/'
;

CREATE TABLE ref_gsdd_Coating (
 CoatingID string,
 CoatingName string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
LOCATION 's3a://salusv/reference/gsdd/Coating/'
;

CREATE TABLE ref_gsdd_Inner_Package_Unit (
 InnerPackageUnitID string,
 UnitName string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
LOCATION 's3a://salusv/reference/gsdd/Inner_Package_Unit/'
;

CREATE TABLE ref_gsdd_Brand_Generic_Status (
 BrandGenericStatusID string,
 StatusName string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
LOCATION 's3a://salusv/reference/gsdd/Brand_Generic_Status/'
;

CREATE TABLE ref_gsdd_Drug_Item_Route (
 DrugItemID string,
 RouteID string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
LOCATION 's3a://salusv/reference/gsdd/Drug_Item_Route/'
;
