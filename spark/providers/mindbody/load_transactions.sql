DROP TABLE IF EXISTS transactional_mindbody;

CREATE EXTERNAL TABLE transactional_mindbody(
    claim_id                string,
    first_name              string,
    last_name               string,
    address                 string,
    city                    string,
    state                   string,
    zip                     string,
    studioVertical          string,
    location_zip            string,
    gender                  string,
    birthdate               string,
    visitClassDate          string,
    sessionStart            string,
    sessionStop             string,
    visitTypeGroupName      string,
    visitTypeName           string,
    visitClassName          string,
    visitType               string,
    visitRevenue            string,
    hv_linking_id           string
)
ROW FORMAT SERDE "org.apache.hadoop.hive.serde2.OpenCSVSerde"
WITH SERDEPROPERTIES (
    "separatorChar" = "|"
)
STORED AS TEXTFILE
LOCATION {input_path}
;
