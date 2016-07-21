
DROP TABLE IF EXISTS ndc_code;

CREATE TABLE ndc_code AS
SELECT a.ndc_code, a.package_description, b.product_type, b.proprietary_name, b.proprietary_name_suffix,
       b.nonproprietary_name, b.dosage_form_name, b.route_name, b.start_marketing_date, b.end_marketing_date,
       b.marketing_category_name, b.labeler_name, b.substance_name, b.active_numerator_strength, b.active_ingred_unit,
       b.pharm_classes
FROM
  (SELECT n.*,
        CASE WHEN substring(ndc_package_code from 5 for 1)='-' and substring(ndc_package_code from 10 for 1)='-' then
              '0'||replace(ndc_package_code,'-','')
           WHEN substring(ndc_package_code from 6 for 1)='-' and substring(ndc_package_code from 10 for 1)='-' then
              substring(ndc_package_code from 1 for 5)||'0'||replace(substring(ndc_package_code from 7 for 6),'-','')
           WHEN substring(ndc_package_code from 6 for 1)='-' and substring(ndc_package_code from 11 for 1)='-' then
              replace(substring(ndc_package_code from 1 for 10),'-','')||'0'||substring(ndc_package_code from 12 for 1)
           ELSE 'ERROR' end as ndc_code
    from ndc_package n
   ) a
   LEFT JOIN ndc_product b on a.product_ndc=b.product_ndc;
