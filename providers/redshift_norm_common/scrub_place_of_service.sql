-- These places of service pose a risk of revealing the patient's residence, set them
-- to unkown, and remove data about them
-- 5 Indian Health Service Free-standing Facility
-- 6 Indian Health Service Provider-based Facility
-- 7 Tribal 638 Free-Standing Facility
-- 8 Tribal 638 Provider-based Facility
-- 9 Prison/Correctional Facility
-- 12 Home
-- 13 Assissted Living Facility
-- 14 Group Home
-- 33 Custodial Care Facility

UPDATE :table_name
SET place_of_service_std_id=99,
prov_rendering_npi=NULL,
prov_rendering_tax_id=NULL,
prov_rendering_ssn=NULL,
prov_rendering_state_license=NULL,
prov_rendering_upin=NULL,
prov_rendering_commercial_id=NULL,
prov_rendering_name_1=NULL,
prov_rendering_name_2=NULL,
prov_rendering_address_1=NULL,
prov_rendering_address_2=NULL,
prov_rendering_city=NULL,
prov_rendering_state=NULL,
prov_rendering_zip=NULL,
--prov_rendering_vendor_id=NULL,
--prov_rendering_dea_id=NULL,
prov_billing_npi=NULL,
prov_billing_tax_id=NULL,
prov_billing_ssn=NULL,
prov_billing_state_license=NULL,
prov_billing_upin=NULL,
prov_billing_commercial_id=NULL,
prov_billing_name_1=NULL,
prov_billing_name_2=NULL,
prov_billing_address_1=NULL,
prov_billing_address_2=NULL,
prov_billing_city=NULL,
prov_billing_state=NULL,
prov_billing_zip=NULL,
--prov_billing_vendor_id=NULL,
--prov_billing_dea_id=NULL,
prov_referring_npi=NULL,
prov_referring_tax_id=NULL,
prov_referring_ssn=NULL,
prov_referring_state_license=NULL,
prov_referring_upin=NULL,
prov_referring_commercial_id=NULL,
prov_referring_name_1=NULL,
prov_referring_name_2=NULL,
prov_referring_address_1=NULL,
prov_referring_address_2=NULL,
prov_referring_city=NULL,
prov_referring_state=NULL,
prov_referring_zip=NULL,
--prov_referring_vendor_id=NULL,
--prov_referring_dea_id=NULL,
prov_facility_npi=NULL,
prov_facility_tax_id=NULL,
prov_facility_ssn=NULL,
prov_facility_state_license=NULL,
prov_facility_upin=NULL,
prov_facility_commercial_id=NULL,
prov_facility_name_1=NULL,
prov_facility_name_2=NULL,
prov_facility_address_1=NULL,
prov_facility_address_2=NULL,
prov_facility_city=NULL,
prov_facility_state=NULL,
prov_facility_zip=NULL,
--prov_facility_vendor_id=NULL,
--prov_facility_dea_id=NULL,
IF place_of_service_std_id IN (5, 6, 7, 8, 9, 12, 13, 14, 33);

