# rdimp
Reference Data Import Routines

## Data Types

### HCPCS
Table I use to look up the description of the procedure code- in this case specifically HCPCS codes
HCPCS are alphanumeric codes

Pronounced “hick picks”

### ICD9 (CMS32_*)
Table I use to look up the description of the diagnosis (dx) and procedure (sg) codes
Note that they’re referring to the procedure codes as “sg” which is weird but I guess that’s how they do it- typically procedure is abbreviated prc or px
HCPCS and ICD9 are types of procedure codes and we do use both, kind of confusing at first
Luckily there’s only 1 type of diagnosis code

### NDC (product & package)
Table I use to look up the description of the NDC codes
NDC codes exist on all drugs and is the system that pharmacies use to track your prescription
We will probably end up going with First Databank (FDB) to maintain our NDC codes since they’re updated every day but this file will get me started

### CCS
Table that tells me which diagnosis (dx) and procedure (px) codes we can group together because they represent the same disease
These are the code look up values like above
This is a separate company that created and maintains a system to group together the real codes into disease areas
Unless you’re a nurse/doctor this is impossible to do on your own which is why we use a something like this, so we don’t have to sit around trying to find every code for diabetes (e.g.)

### Place of Service
Table I use to look up the the location of where the patient was treated
