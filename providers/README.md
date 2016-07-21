# rdimp
Reference Data Import Routines

## Data Types

### HCPCS

This is used to look up the description of procedure codes, in this case specifically HCPCS codes. HCPCS are alphanumeric codes.

Pronounced "hick picks."

### ICD9 (CMS32_*)

Used to look up the description of the diagnosis (dx) and procedure (sg) codes. (Note, ICD9 uses "SG" for "Procedure Codes". Typically it's PX/PRC)
HCPCS and ICD9 are types of procedure codes and we do use both, kind of confusing at first. Luckily there’s only 1 type of diagnosis code

### NDC (product & package)

Used to look up the description of the NDC codes. NDC codes exist on all drugs and is the system that pharmacies use to track your prescription. We will probably end up going with First Databank (FDB) to maintain our NDC codes since they’re updated every day but this file will get us started.

### CCS

Tells us which diagnosis (dx) and procedure (px) codes we can group together because they represent the same disease. These are the code look up values like the above ICD9 values. This is a separate company that created and maintains a system to group together the real codes into disease areas. Unless you’re a nurse/doctor this is impossible to do on your own which is why we use a something like this, so we don’t have to sit around trying to find every code for diabetes for example.

### Place of Service

Used to look up the the location of where the patient was treated.
