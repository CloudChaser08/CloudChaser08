drop table if exists ndc_temp0;

create temporary view ndc_temp0 as
select distinct d.ndc11, a.name, a.consumerdefinition, regexp_replace(e.icd10_code,'.','') as icd10_code
from ref_gsdd_gsterms a
    inner join ref_gsdd_product_indications b on a.gstermid=b.gstermid
    inner join ref_gsdd_product c on b.productid=c.productid
    inner join ref_gsdd_package d on c.productid=d.productid
    inner join ref_gsdd_gsterm_icd10 e on a.gstermid=e.gstermid
where d.ndc11 is not null and d.ndc11<>' ';


drop table if exists marketplace_ndc_icd10_xwalk;

create temporary view marketplace_ndc_icd10_xwalk as
select distinct ndc11, icd10_code
from ndc_temp0;


drop table if exists ndc_temp1;

create temporary view ndc_temp1 as
select distinct a.*, d.conceptname as level2, 
    CASE WHEN maxlev=2 then b.conceptname ELSE e.conceptname end as level3,
    CASE WHEN maxlev=2 then NULL
        WHEN maxlev=3 then b.conceptname 
        ELSE f.conceptname end as level4,
    CASE WHEN maxlev<=3 then NULL
        WHEN maxlev=4 then b.conceptname
        ELSE g.conceptname end as level5,
    CASE WHEN maxlev<=4 then NULL
        WHEN maxlev=5 then b.conceptname
        ELSE h.conceptname end as level6
from 
    (select x.*, 
        CASE WHEN parent6<>'' then 6 
            WHEN parent5<>'' then 5 
            WHEN parent4<>'' then 4 
            WHEN parent3<>'' then 3
            WHEN parent2<>'' then 2 else -1 end as maxlev
    from ref_gsdd_therapeutic_concept_tree_denorm x
    ) a
    left join ref_gsdd_therapeutic_concept b on a.therapeuticconceptid=b.therapeuticconceptid
    left join ref_gsdd_therapeutic_concept c on c.therapeuticconceptid=a.parent1
    left join ref_gsdd_therapeutic_concept d on d.therapeuticconceptid=a.parent2
    left join ref_gsdd_therapeutic_concept e on e.therapeuticconceptid=a.parent3
    left join ref_gsdd_therapeutic_concept f on f.therapeuticconceptid=a.parent4
    left join ref_gsdd_therapeutic_concept g on g.therapeuticconceptid=a.parent5
    left join ref_gsdd_therapeutic_concept h on h.therapeuticconceptid=a.parent6;


drop table if exists ndc_temp2;

create temporary view ndc_temp2 as
select distinct b.productid, d.cp_num, c.level2, level3, level4, level5, level6, upper(i.genericnamelong) as genericnamelong
from ref_gsdd_therapeutic_concept_tree_specific_product a
    left join ref_gsdd_product_ssms b on a.specificproductid=b.specificproductid
    left join ndc_temp1 c on a.therapeuticconcepttreeid=c.therapeuticconcepttreeid
    left join ref_gsdd_product d on b.productid=d.productid
    left join ref_gsdd_product_generic_name_stub i on d.productid=i.productid;


drop table if exists ndc_temp3;

create temporary view ndc_temp3 as
select distinct a.productid, a.cp_num, b.ndc11
from ref_gsdd_product a
    left join ref_gsdd_package b on a.productid=b.productid
    where ndc11<>'';


drop table if exists ndc_temp4;

create temporary view ndc_temp4 as
select distinct a.ndc_code, 
    CASE WHEN a.substance_name<>'' and a.substance_name<>' ' and a.substance_name is not null then upper(a.substance_name) 
        WHEN a.nonproprietary_name<>'' and a.nonproprietary_name<>' ' and a.nonproprietary_name is not null then upper(a.nonproprietary_name) 
        WHEN a.proprietary_name<>'' and a.proprietary_name<>' ' and a.proprietary_name is not null then upper(proprietary_name) 
        END AS drug, 
    upper(dosage_form_name) as dosage_form_name, upper(route_name) as route_name,
    c.genericnamelong, a.product_type as level1, 
    CASE WHEN c.productid<>'' then c.level2 ELSE d.level2 end as level2,
    CASE WHEN c.productid<>'' then c.level3 ELSE d.level3 end as level3,
    CASE WHEN c.productid<>'' then c.level4 ELSE d.level4 end as level4,
    CASE WHEN c.productid<>'' then c.level5 ELSE d.level5 end as level5,
    CASE WHEN c.productid<>'' then c.level6 ELSE d.level6 end as level6
from default.ref_ndc_code a
    left join ndc_temp3 b on a.ndc_code=b.ndc11
    left join (select * from ndc_temp2 where productid is not null and productid<>'' and productid<>' ') c on b.productid=c.productid
    left join (select * from ndc_temp2 where cp_num is not null and cp_num<>'' and cp_num<>' ' and cp_num not in ('3762')) d on b.cp_num=d.cp_num;


drop table if exists ndc_temp5;

create temporary view ndc_temp5 as
select distinct drug, route_name, level1, level2, level3, level4, level5, level6
from ndc_temp4
where drug<>'' and drug<>' ' and drug is not null and level2 is not null;

drop table if exists ndc_temp6;

create temporary view ndc_temp6 as
select distinct a.ndc_code, a.drug, a.genericnamelong, a.level1,
    CASE WHEN a.level2 is null then c.level2 ELSE a.level2 end as level2,
    CASE WHEN a.level3 is null then c.level3 ELSE a.level3 end as level3,
    CASE WHEN a.level4 is null then c.level4 ELSE a.level4 end as level4,
    CASE WHEN a.level5 is null then c.level5 ELSE a.level5 end as level5,
    CASE WHEN a.level6 is null then c.level6 ELSE a.level6 end as level6
from ndc_temp4 a
    left join ndc_temp5 c on a.drug=c.drug and a.route_name=c.route_name and a.level1=c.level1;

drop table if exists ndc_temp7;

create temporary view ndc_temp7 as
select distinct z.ndc_code, trim(concat(upper(z.proprietary_name), ' ', upper(z.proprietary_name_suffix))) as proprietary_name, upper(z.nonproprietary_name) as nonproprietary_name, upper(z.substance_name) as substance_name, upper(z.dosage_form_name) as dosage_form_name, upper(z.route_name) as route_name,     upper(z.pharm_classes) as pharm_class, x.level1, x.level2, x.level3, x.level4, x.level5, x.level6, x.genericnamelong
from default.ref_ndc_code z
    left join ndc_temp6 x on z.ndc_code=x.ndc_code;

drop table if exists ndc_temp8;
create temporary view ndc_temp8 as
select ndc_code, COALESCE(level3, 'Other') as level1, COALESCE(level4, 'Other') as level2,
    CASE WHEN substance_name is not null and substance_name<>'' and substance_name<>' ' then substance_name
        WHEN nonproprietary_name is not null and nonproprietary_name<>'' and nonproprietary_name<>' ' then nonproprietary_name
        ELSE COALESCE(genericnamelong, 'Other') end as level3,
    COALESCE(proprietary_name, 'Other') as level4
from ndc_temp7 where level1<>'HUMAN OTC DRUG';


drop table if exists marketplace_ndc_new;
create temporary view marketplace_ndc_new as
select distinct * from ndc_temp8;
