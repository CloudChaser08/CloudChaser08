DROP TABLE IF EXISTS cardinal_vitalpath_med;
CREATE EXTERNAL TABLE cardinal_vitalpath_med (
	unitquantity                string,
    jcode                       string,
    form_id                     string,
    tenant_id                   string,
    ndc                         string,
    clinicalorderdate           string,
    dosage_uom                  string,
    drugname                    string,
    quantity                    string,
    formname                    string,
    hvJoinKey                   string
)
ROW FORMATE SERDE 'org.apache.hive.hcatalog.data.JsonSerDe' 
STORED AS TEXTFILE
;

DROP TABLE IF EXISTS cardinal_vitalpath_patient;
CREATE EXTERNAL TABLE cardinal_vitalpath_patient (
	created						string,
	tenant_id					string,
	modified					string,
	alert						string,
	version						string,
	gender						string,
	hvJoinKey					string
)
ROW FORMATE SERDE 'org.apache.hive.hcatalog.data.JsonSerDe' 
STORED AS TEXTFILE
;

