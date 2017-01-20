-- this is from reyna

/* GET LIST OF PATIENTS WITH A VISIT IN THE RIGHT TIME PERIOD*/
create table as_enc as
select distinct hvid, encounterid
from encounters
where encounterDTTM between '2015-10-01' and '2016-09-30'


/* GET DIAGS */
create table as_diags as
select hvid, code
from 
	(select distinct hvid, billingicd10code as code, encounterid
	from orders
	UNION
	select distinct hvid, icd10 as code, encounterid
	from problems
	where errorflag='N'
	) a
	inner join as_enc b on a.hvid=b.hvid


/* GET PROCS */
create table as_procs as
select hvid, code
from 
	(select distinct hvid, cpt4 as code, encounterid
	from orders
	UNION
	select distinct hvid, hcpcs as code, encounterid
	from orders
	UNION
	select distinct hvid, cptcode as code, encounterid
	from problems
	where errorflag='N'
	) a
	inner join as_enc b on a.hvid=b.hvid and a.encounterid=b.encounterid


/* GET DRUGS */
create table as_ndc as
select hvid, code
from 
	(select distinct hvid, ndc, encounterid
	from medications
	where errorflag='N'
	UNION
	select distinct hvid, ndc, encounterid
	from vaccines
	) a
	inner join as_enc b on a.hvid=b.hvid and a.encounterid=b.encounterid


/* GET LOINC */
create table as_loinc as
select distinct hvid, loinc as code
from results a
	inner join as_enc b on a.hvid=b.hvid and a.encounterid=b.encounterid


