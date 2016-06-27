create table ndc_gsdd_indication as
select distinct x.icd10_code, x.gstermid, x.gstermname, x.consumerdefinition, x.allowedforindication, x.allowedforcontraindication,     x.allowedforadversereaction, x.minage, x.maxage, x.ageunit, x.gender, x.isofflabel, d.routename, e.genericnamelong, e.genericnameshort,     e.genericsynonym, y.conceptname
from
  (select distinct a.code as icd10_code, c.gstermid, b.name as gstermname, b.consumerdefinition, b.allowedforindication, b.allowedforcontraindication,      b.allowedforadversereaction, c.minage, c.maxage, c.ageunit, c.gender, c.isofflabel, trim(both ' ' from c.routecode) as routeid, c.productid
   from
    (select replace(icd10_code,'.','') as code, gstermid
     from gsdd_gsterm_icd10
    ) a
    INNER JOIN 
    (select *
     from gsdd_gsterms
     where allowedforindication like 'True%'
    ) b on a.gstermid=b.gstermid
    INNER JOIN gsdd_product_indications c on b.gstermid=c.gstermid
  ) x
  LEFT JOIN gsdd_route_of_administration d on x.routeid=trim(both ' ' from d.routeid)
  LEFT JOIN gsdd_product_generic_name_stub e on x.productid=e.productid
  LEFT JOIN
  (select distinct f.productid, j.conceptname
   from GSDD_PRODUCT f 
      inner join gsdd_product_ssms g on f.productid=g.productid
      inner join gsdd_therapeutic_concept_tree_specific_product h on g.specificproductid=h.specificproductid
      inner join gsdd_therapeutic_concept_tree_denorm i on h.therapeuticconcepttreeid=i.therapeuticconcepttreeid
      inner join gsdd_therapeutic_concept j on i.therapeuticconceptid=j.therapeuticconceptid
  ) y on x.productid=y.productid
order by x.icd10_code;

