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
 productid string,
 gstermid string,
 minage string,
 maxage string,
 ageunit string,
 gender string,
 isofflabel string,
 routecode string,
 sectionid string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
LOCATION 's3a://salusv/reference/gsdd/Product_Indications/'
;

CREATE TABLE ref_gsdd_product_contraindications(
 productid string,
 gstermid string,
 minage string,
 maxage string,
 ageunits string,
 gender string,
 iscontraindication string,
 isboxedwarning string,
 sectionid string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
LOCATION 's3a://salusv/reference/gsdd/Product_Contraindications/'
;

CREATE TABLE ref_gsdd_Product_CMSName (
 productid string,
 cmsname string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
LOCATION 's3a://salusv/reference/gsdd/Product_CMSName/'
;

CREATE TABLE ref_gsdd_CVXCode_VaccineGroup (
 cvxcode string,
 vaccinegroup string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
LOCATION 's3a://salusv/reference/gsdd/CVXCode_VaccineGroup/'
;

CREATE TABLE ref_gsdd_AGS_Beers_Strength_Of_Recommendation (
 strengthofrecommendationid string,
 strengthofrecommendationtext string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
LOCATION 's3a://salusv/reference/gsdd/AGS_Beers_Strength_Of_Recommendation/'
;

CREATE TABLE ref_gsdd_Scoring (
 scoringid string,
 scoringname string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
LOCATION 's3a://salusv/reference/gsdd/Scoring/'
;

CREATE TABLE ref_gsdd_Nondrug_Item_Version_Color (
 nondrugitemid string,
 version string,
 colorid string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
LOCATION 's3a://salusv/reference/gsdd/Nondrug_Item_Version_Color/'
;

CREATE TABLE ref_gsdd_Marketed_Product_RxNorm (
 marketedproductid string,
 rxcui string,
 rxnormname string,
 rxnormtype string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
LOCATION 's3a://salusv/reference/gsdd/Marketed_Product_RxNorm/'
;

CREATE TABLE ref_gsdd_Package_Version_Drug_Item_Version (
 packageid string,
 packageversion string,
 drugitemid string,
 drugitemversion string,
 companyid string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
LOCATION 's3a://salusv/reference/gsdd/Package_Version_Drug_Item_Version/'
;

CREATE TABLE ref_gsdd_Marketed_Product (
 marketedproductid string,
 name string,
 companyid string,
 specificproductid string,
 brand string,
 repackaged string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
LOCATION 's3a://salusv/reference/gsdd/Marketed_Product/'
;

CREATE TABLE ref_gsdd_AGS_Beers_Disease_Syndrome_Statement (
 diseasesyndromestatementid string,
 diseasesyndromestatementtext string,
 diseasesyndromecategoryid string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
LOCATION 's3a://salusv/reference/gsdd/AGS_Beers_Disease_Syndrome_Statement/'
;

CREATE TABLE ref_gsdd_Product_Strength_Route_Form (
 productid string,
 ingredientid string,
 ingredientname string,
 baseingredientid string,
 baseingredient string,
 tallmanname string,
 strength string,
 strengthunitcode string,
 pervolume string,
 pervolumeunitcode string,
 routeid string,
 routename string,
 fdaformid string,
 fdaform string,
 gsformid string,
 gsform string,
 alternativeformid string,
 alternativeformname string,
 clinicaldoseformid string,
 clinicaldoseformname string,
 normalizedformid string,
 normalizedformname string,
 topleveldoseformid string,
 topleveldoseformname string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
LOCATION 's3a://salusv/reference/gsdd/Product_Strength_Route_Form/'
;

CREATE TABLE ref_gsdd_Nondrug_Item_Ingredient (
 nondrugitemid string,
 ingredientid string,
 strength string,
 strengthunitcode string,
 pervolume string,
 pervolumeunitcode string,
 dilutionnumerator string,
 dilutiondenominator string,
 baseequivalent string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
LOCATION 's3a://salusv/reference/gsdd/Nondrug_Item_Ingredient/'
;

CREATE TABLE ref_gsdd_AGS_Beers_Avoid_Caution (
 avoidcautionid string,
 avoidcautiontext string,
 tableid string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
LOCATION 's3a://salusv/reference/gsdd/AGS_Beers_Avoid_Caution/'
;

CREATE TABLE ref_gsdd_Drug_Item_Version_Attribute (
 drugitemversionattributeid string,
 attributename string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
LOCATION 's3a://salusv/reference/gsdd/Drug_Item_Version_Attribute/'
;

CREATE TABLE ref_gsdd_Package_Drug_Item (
 packageid string,
 drugitemid string,
 itemcount string,
 packageitemunitid string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
LOCATION 's3a://salusv/reference/gsdd/Package_Drug_Item/'
;

CREATE TABLE ref_gsdd_GSTerm_ICD10 (
 icd10_code string,
 gstermid string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
LOCATION 's3a://salusv/reference/gsdd/GSTerm_ICD10/'
;

CREATE TABLE ref_gsdd_Company (
 companyid string,
 companyname string,
 companynameshort string,
 labelercode string,
 parentcompanyid string,
 mvx string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
LOCATION 's3a://salusv/reference/gsdd/Company/'
;

CREATE TABLE ref_gsdd_Nondrug_Item_Version (
 nondrugitemid string,
 version string,
 description string,
 coatingid string,
 flavorid string,
 shapeid string,
 scoringid string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
LOCATION 's3a://salusv/reference/gsdd/Nondrug_Item_Version/'
;

CREATE TABLE ref_gsdd_Ingredient_RxNorm_UNII (
 ingredientid string,
 rxnormcode string,
 uniicode string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
LOCATION 's3a://salusv/reference/gsdd/Ingredient_RxNorm_UNII/'
;

CREATE TABLE ref_gsdd_Script_Form (
 scriptformid string,
 ncpdpscriptformcode string,
 formname string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
LOCATION 's3a://salusv/reference/gsdd/Script_Form/'
;

CREATE TABLE ref_gsdd_Product_SSMS (
 productid string,
 specificproductid string,
 brandgenericstatusid string,
 ssmstypeid string,
 sourcecount string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
LOCATION 's3a://salusv/reference/gsdd/Product_SSMS/'
;

CREATE TABLE ref_gsdd_AGS_Beers_Modifier (
 modifierid string,
 modifiertype string,
 modifiertext string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
LOCATION 's3a://salusv/reference/gsdd/AGS_Beers_Modifier/'
;

CREATE TABLE ref_gsdd_DESI_Status (
 desistatusid string,
 statusname string,
 cmsid string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
LOCATION 's3a://salusv/reference/gsdd/DESI_Status/'
;

CREATE TABLE ref_gsdd_Product_Branded_Name_Stub (
 productid string,
 brandednamestubid string,
 defaults string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
LOCATION 's3a://salusv/reference/gsdd/Product_Branded_Name_Stub/'
;

CREATE TABLE ref_gsdd_DEA_Classification (
 deaclassificationid string,
 classification string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
LOCATION 's3a://salusv/reference/gsdd/DEA_Classification/'
;

CREATE TABLE ref_gsdd_Package (
 packageid string,
 ndc10 string,
 ndc11 string,
 upc string,
 nhric string,
 gtin12 string,
 gtin14 string,
 pin string,
 ncpdpexceptionalcount string,
 ncpdpbillingunitid string,
 ncpdpscriptformcode string,
 outerpackageunitid string,
 innerpackagecount string,
 innerpackageunitid string,
 replacedbypackageid string,
 replaceddate string,
 productid string,
 packagedescription string,
 packagedescriptortext string,
 preservativefree string,
 offmarket string,
 lotexpiry string,
 packageonmarketdate string,
 packagesize string,
 notsplittable string,
 shortcycledispensing string,
 shortcycledispensingismanual string,
 minimumdispensequantity string,
 minimumdispensequantityismanual string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
LOCATION 's3a://salusv/reference/gsdd/Package/'
;

CREATE TABLE ref_gsdd_AGS_Beers_Disease_Syndrome_Category (
 diseasesyndromecategoryid string,
 diseasesyndromecategorytext string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
LOCATION 's3a://salusv/reference/gsdd/AGS_Beers_Disease_Syndrome_Category/'
;

CREATE TABLE ref_gsdd_Product_Ingredient_Hazardous_Waste_Code (
 productid string,
 ingredientid string,
 substance string,
 p_code string,
 p_codequalifier string,
 p_codehazardouswastecode string,
 f_code string,
 f_codehazardouswastecode string,
 k_code string,
 k_codehazardouswastecode string,
 u_code string,
 u_codehazardouswastecode string,
 u_codequalifier string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
LOCATION 's3a://salusv/reference/gsdd/Product_Ingredient_Hazardous_Waste_Code/'
;

CREATE TABLE ref_gsdd_GSTerm_SNOMED (
 snomed_code string,
 gstermid string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
LOCATION 's3a://salusv/reference/gsdd/GSTerm_SNOMED/'
;

CREATE TABLE ref_gsdd_Product_CVXCode (
 productid string,
 cvxcode string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
LOCATION 's3a://salusv/reference/gsdd/Product_CVXCode/'
;

CREATE TABLE ref_gsdd_Product_Therapeutic_Equivalence (
 productid string,
 equivalencecode string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
LOCATION 's3a://salusv/reference/gsdd/Product_Therapeutic_Equivalence/'
;

CREATE TABLE ref_gsdd_ATC_Active_Composition_Generic_Group (
 atccode string,
 activecompositiongenericgroupid string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
LOCATION 's3a://salusv/reference/gsdd/ATC_Active_Composition_Generic_Group/'
;

CREATE TABLE ref_gsdd_Therapeutic_Concept (
 therapeuticconceptid string,
 conceptname string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
LOCATION 's3a://salusv/reference/gsdd/Therapeutic_Concept/'
;

CREATE TABLE ref_gsdd_AGS_Beers_Quality_Of_Evidence (
 qualityofevidenceid string,
 qualityofevidencetext string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
LOCATION 's3a://salusv/reference/gsdd/AGS_Beers_Quality_Of_Evidence/'
;

CREATE TABLE ref_gsdd_Imprint_Location (
 imprintlocationid string,
 imprintlocationdescription string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
LOCATION 's3a://salusv/reference/gsdd/Imprint_Location/'
;

CREATE TABLE ref_gsdd_Product_Name_Type (
 productnametypeid string,
 typename string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
LOCATION 's3a://salusv/reference/gsdd/Product_Name_Type/'
;

CREATE TABLE ref_gsdd_License_Type (
 licensetypeid string,
 licensetypename string,
 licensetypedescription string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
LOCATION 's3a://salusv/reference/gsdd/License_Type/'
;

CREATE TABLE ref_gsdd_Outer_Package_Unit (
 outerpackageunitid string,
 unitname string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
LOCATION 's3a://salusv/reference/gsdd/Outer_Package_Unit/'
;

CREATE TABLE ref_gsdd_Product (
 productid string,
 ndc9 string,
 productnamelong string,
 productnametypeid string,
 productnameshort string,
 eprescribingname string,
 marketerid string,
 desistatusid string,
 deaclassificationid string,
 brandgenericstatusid string,
 legendstatusid string,
 marketedproductid string,
 cp_num string,
 offmarketdate string,
 replacedbyproductid string,
 licensetypeid string,
 repackaged string,
 innovator string,
 privatelabel string,
 productonmarketdate string,
 bulkchemical string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
LOCATION 's3a://salusv/reference/gsdd/Product/'
;

CREATE TABLE ref_gsdd_Drug_Item_Version (
 drugitemid string,
 version string,
 description string,
 coatingid string,
 flavorid string,
 shapeid string,
 scoringid string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
LOCATION 's3a://salusv/reference/gsdd/Drug_Item_Version/'
;

CREATE TABLE ref_gsdd_Imprint (
 drugitemid string,
 version string,
 imprintlocationid string,
 imprinttext string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
LOCATION 's3a://salusv/reference/gsdd/Imprint/'
;

CREATE TABLE ref_gsdd_Therapeutic_Equivalence (
 equivalencecode string,
 equivalencedescription string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
LOCATION 's3a://salusv/reference/gsdd/Therapeutic_Equivalence/'
;

CREATE TABLE ref_gsdd_AGS_Beers_Rationale (
 rationalid string,
 rationaltext string,
 avoidcautionid string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
LOCATION 's3a://salusv/reference/gsdd/AGS_Beers_Rationale/'
;

CREATE TABLE ref_gsdd_GSTerm_ICD9 (
 icd9_code string,
 gstermid string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
LOCATION 's3a://salusv/reference/gsdd/GSTerm_ICD9/'
;

CREATE TABLE ref_gsdd_Color (
 colorid string,
 colorname string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
LOCATION 's3a://salusv/reference/gsdd/Color/'
;

CREATE TABLE ref_gsdd_Drug_Item_Version_Color (
 drugitemid string,
 version string,
 colorid string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
LOCATION 's3a://salusv/reference/gsdd/Drug_Item_Version_Color/'
;

CREATE TABLE ref_gsdd_Shape (
 shapeid string,
 shapename string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
LOCATION 's3a://salusv/reference/gsdd/Shape/'
;

CREATE TABLE ref_gsdd_Unit (
 unitcode string,
 unitname string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
LOCATION 's3a://salusv/reference/gsdd/Unit/'
;

CREATE TABLE ref_gsdd__GSDD_TableList (
 lename string,
 description string,
 modulename string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
LOCATION 's3a://salusv/reference/gsdd/_GSDD_TableList/'
;

CREATE TABLE ref_gsdd_Product_Storage (
 productid string,
 storageid string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
LOCATION 's3a://salusv/reference/gsdd/Product_Storage/'
;

CREATE TABLE ref_gsdd_Branded_Name_Stub (
 brandednamestubid string,
 brandednamestub string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
LOCATION 's3a://salusv/reference/gsdd/Branded_Name_Stub/'
;

CREATE TABLE ref_gsdd_NCPDP_Unit (
 ncpdpunitid string,
 unitname string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
LOCATION 's3a://salusv/reference/gsdd/NCPDP_Unit/'
;

CREATE TABLE ref_gsdd_Package_CMSName (
 packageid string,
 cmsname string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
LOCATION 's3a://salusv/reference/gsdd/Package_CMSName/'
;

CREATE TABLE ref_gsdd_Nondrug_Item (
 nondrugitemid string,
 itemname string,
 productid string,
 gsformid string,
 fdaformid string,
 implicitrouteid string,
 specificnondrugitemid string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
LOCATION 's3a://salusv/reference/gsdd/Nondrug_Item/'
;

CREATE TABLE ref_gsdd_Package_Nondrug_Item (
 packageid string,
 nondrugitemid string,
 itemcount string,
 packageitemunitid string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
LOCATION 's3a://salusv/reference/gsdd/Package_Nondrug_Item/'
;

CREATE TABLE ref_gsdd_Nondrug_Item_Version_Attributes (
 nondrugitemid string,
 version string,
 nondrugitemversionattributeid string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
LOCATION 's3a://salusv/reference/gsdd/Nondrug_Item_Version_Attributes/'
;

CREATE TABLE ref_gsdd_Therapeutic_Concept_Tree (
 therapeuticconcepttreeid string,
 parentconcepttreeid string,
 therapeuticconceptid string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
LOCATION 's3a://salusv/reference/gsdd/Therapeutic_Concept_Tree/'
;

CREATE TABLE ref_gsdd_AGS_Beers_Therapeutic_Category_Organ_System (
 therapeuticcategoryorgansystemid string,
 therapeuticcategoryorgansystemtext string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
LOCATION 's3a://salusv/reference/gsdd/AGS_Beers_Therapeutic_Category_Organ_System/'
;

CREATE TABLE ref_gsdd_NonSplittable_Product (
 marketedproductid string,
 productid string,
 orderablename string,
 nonsplittable string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
LOCATION 's3a://salusv/reference/gsdd/NonSplittable_Product/'
;

CREATE TABLE ref_gsdd_GS_Form (
 gsformid string,
 formname string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
LOCATION 's3a://salusv/reference/gsdd/GS_Form/'
;

CREATE TABLE ref_gsdd_Package_Version_Nondrug_Item_Version (
 packageid string,
 packageversion string,
 nondrugitemid string,
 nondrugitemversion string,
 companyid string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
LOCATION 's3a://salusv/reference/gsdd/Package_Version_Nondrug_Item_Version/'
;

CREATE TABLE ref_gsdd_Storage (
 storageid string,
 storageconcept string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
LOCATION 's3a://salusv/reference/gsdd/Storage/'
;

CREATE TABLE ref_gsdd_Nondrug_Item_Version_Attribute (
 nondrugitemversionattributeid string,
 attributename string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
LOCATION 's3a://salusv/reference/gsdd/Nondrug_Item_Version_Attribute/'
;

CREATE TABLE ref_gsdd_Active_Composition_Generic_Group_Active_Composition_Generic (
 activecompositiongenericgroupid string,
 activecompositiongenericid string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
LOCATION 's3a://salusv/reference/gsdd/Active_Composition_Generic_Group_Active_Composition_Generic/'
;

CREATE TABLE ref_gsdd_AGS_Beers_Table (
 tableid string,
 tablename string,
 tabledescription string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
LOCATION 's3a://salusv/reference/gsdd/AGS_Beers_Table/'
;

CREATE TABLE ref_gsdd_AGS_Beers_Criteria_Item (
 agsbeerscriteriaitemid string,
 agsbeerscriteriagenericproductid string,
 rationalid string,
 qualityofevidenceid string,
 strengthofrecommendationid string,
 therapeuticcategoryorgansystemid string,
 diseasesyndromestatementid string,
 modifieraid string,
 modifierbid string,
 professionalnotes string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
LOCATION 's3a://salusv/reference/gsdd/AGS_Beers_Criteria_Item/'
;

CREATE TABLE ref_gsdd_Maintenance_Medications (
 packageid string,
 productid string,
 marketedproductid string,
 specificproductid string,
 ndc11 string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
LOCATION 's3a://salusv/reference/gsdd/Maintenance_Medications/'
;

CREATE TABLE ref_gsdd_Ingredient_Name (
 ingredientid string,
 ingredientname string,
 tallmanname string,
 baseingredientid string,
 isbaseingredient string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
LOCATION 's3a://salusv/reference/gsdd/Ingredient_Name/'
;

CREATE TABLE ref_gsdd_Product_Generic_Name_Stub (
 productid string,
 genericnamelong string,
 genericnameshort string,
 genericsynonym string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
LOCATION 's3a://salusv/reference/gsdd/Product_Generic_Name_Stub/'
;

CREATE TABLE ref_gsdd_Product_Ingredient_Federal_Narcotic (
 productid string,
 ingredientid string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
LOCATION 's3a://salusv/reference/gsdd/Product_Ingredient_Federal_Narcotic/'
;

CREATE TABLE ref_gsdd_Drug_Item_Active_Ingredient (
 drugitemid string,
 ingredientid string,
 strength string,
 strengthunitcode string,
 pervolume string,
 pervolumeunitcode string,
 dilutionnumerator string,
 dilutiondenominator string,
 baseequivalent string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
LOCATION 's3a://salusv/reference/gsdd/Drug_Item_Active_Ingredient/'
;

CREATE TABLE ref_gsdd_Package_Item_Unit (
 packageitemunitid string,
 unitname string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
LOCATION 's3a://salusv/reference/gsdd/Package_Item_Unit/'
;

CREATE TABLE ref_gsdd_Implicit_Route (
 implicitrouteid string,
 route string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
LOCATION 's3a://salusv/reference/gsdd/Implicit_Route/'
;

CREATE TABLE ref_gsdd_Legend_Status (
 legendstatusid string,
 statusname string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
LOCATION 's3a://salusv/reference/gsdd/Legend_Status/'
;

CREATE TABLE ref_gsdd_Route_Of_Administration (
 routeid string,
 routename string,
 snomedcode string,
 sigroutesynonym string,
 sigabbreviation string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
LOCATION 's3a://salusv/reference/gsdd/Route_Of_Administration/'
;

CREATE TABLE ref_gsdd_Package_Version (
 packageidentifier string,
 packageidentifiertype string,
 formattedpackageidentifier string,
 packageid string,
 version string,
 ncpdpexceptionalcount string,
 ncpdpbillingunitid string,
 ncpdpscriptformcode string,
 outerpackageunitid string,
 innerpackagecount string,
 innerpackageunitid string,
 replacedbypackageid string,
 replaceddate string,
 productid string,
 packageversiondescription string,
 packagedescriptortext string,
 preservativefree string,
 packageversiononmarketdate string,
 packageversionoffmarketdate string,
 unformattedndc10 string,
 packagesize string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
LOCATION 's3a://salusv/reference/gsdd/Package_Version/'
;

CREATE TABLE ref_gsdd_Drug_Item_Version_Attributes (
 drugitemid string,
 version string,
 drugitemversionattributeid string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
LOCATION 's3a://salusv/reference/gsdd/Drug_Item_Version_Attributes/'
;

CREATE TABLE ref_gsdd_Flavor (
 flavorid string,
 flavorname string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
LOCATION 's3a://salusv/reference/gsdd/Flavor/'
;

CREATE TABLE ref_gsdd_Therapeutic_Concept_Tree_Specific_Product (
 therapeuticconcepttreeid string,
 specificproductid string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
LOCATION 's3a://salusv/reference/gsdd/Therapeutic_Concept_Tree_Specific_Product/'
;

CREATE TABLE ref_gsdd_GSTerms (
 gstermid string,
 name string,
 consumerdefinition string,
 allowedforindication string,
 allowedforcontraindication string,
 allowedforadversereaction string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
LOCATION 's3a://salusv/reference/gsdd/GSTerms/'
;

CREATE TABLE ref_gsdd_NonSplittable_Package (
 marketedproductid string,
 packageid string,
 orderablename string,
 nonsplittable string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
LOCATION 's3a://salusv/reference/gsdd/NonSplittable_Package/'
;

CREATE TABLE ref_gsdd_FDA_Form_NCIt_Term (
 fdaformid string,
 fdaid string,
 formname string,
 formdescription string,
 formindex string,
 ncitquantityunittermid string,
 ncitstrengthformtermid string,
 ncitid string,
 ncitsubsetcode string,
 ncpdpsubsetpreferredterm string,
 ncitcode string,
 ncpdppreferredterm string,
 ncitpreferredterm string,
 ncitdefinition string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
LOCATION 's3a://salusv/reference/gsdd/FDA_Form_NCIt_Term/'
;

CREATE TABLE ref_gsdd_SSMS_Type (
 ssmstypeid string,
 sourcetype string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
LOCATION 's3a://salusv/reference/gsdd/SSMS_Type/'
;

CREATE TABLE ref_gsdd_CVXCode (
 cvxcode string,
 descriptionshort string,
 descriptionlong string,
 vaccinestatus string,
 notes string,
 lastupdated string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
LOCATION 's3a://salusv/reference/gsdd/CVXCode/'
;

CREATE TABLE ref_gsdd_Product_Attribute (
 productattributeid string,
 attributename string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
LOCATION 's3a://salusv/reference/gsdd/Product_Attribute/'
;

CREATE TABLE ref_gsdd_Product_Product_Attribute (
 productid string,
 productattributeid string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
LOCATION 's3a://salusv/reference/gsdd/Product_Product_Attribute/'
;

CREATE TABLE ref_gsdd_ATC_Specific_Product (
 atccode string,
 specificproductid string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
LOCATION 's3a://salusv/reference/gsdd/ATC_Specific_Product/'
;

CREATE TABLE ref_gsdd_Package_SSMS (
 packageid string,
 specificproductid string,
 brandgenericstatusid string,
 ssmstypeid string,
 sourcecount string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
LOCATION 's3a://salusv/reference/gsdd/Package_SSMS/'
;

CREATE TABLE ref_gsdd_AGS_Beers_Criteria_Generic_Product_Product (
 productid string,
 agsbeerscriteriagenericproductid string,
 genericproductid string,
 genericproductclinicalid string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
LOCATION 's3a://salusv/reference/gsdd/AGS_Beers_Criteria_Generic_Product_Product/'
;

CREATE TABLE ref_gsdd_Drug_Item (
 drugitemid string,
 productid string,
 itemnamelong string,
 gsformid string,
 fdaformid string,
 implicitrouteid string,
 specificdrugitemid string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
LOCATION 's3a://salusv/reference/gsdd/Drug_Item/'
;

CREATE TABLE ref_gsdd_Nondrug_Item_Route (
 nondrugitemid string,
 routeid string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
LOCATION 's3a://salusv/reference/gsdd/Nondrug_Item_Route/'
;

CREATE TABLE ref_gsdd_FDA_Form (
 fdaformid string,
 formname string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
LOCATION 's3a://salusv/reference/gsdd/FDA_Form/'
;

CREATE TABLE ref_gsdd_Therapeutic_Concept_Tree_DENORM (
 therapeuticconcepttreeid string,
 therapeuticconceptid string,
 parent1 string,
 parent2 string,
 parent3 string,
 parent4 string,
 parent5 string,
 parent6 string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
LOCATION 's3a://salusv/reference/gsdd/Therapeutic_Concept_Tree_DENORM/'
;

CREATE TABLE ref_gsdd_Coating (
 coatingid string,
 coatingname string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
LOCATION 's3a://salusv/reference/gsdd/Coating/'
;

CREATE TABLE ref_gsdd_Inner_Package_Unit (
 innerpackageunitid string,
 unitname string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
LOCATION 's3a://salusv/reference/gsdd/Inner_Package_Unit/'
;

CREATE TABLE ref_gsdd_Brand_Generic_Status (
 brandgenericstatusid string,
 statusname string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
LOCATION 's3a://salusv/reference/gsdd/Brand_Generic_Status/'
;

CREATE TABLE ref_gsdd_Drug_Item_Route (
 drugitemid string,
 routeid string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
LOCATION 's3a://salusv/reference/gsdd/Drug_Item_Route/'
;
