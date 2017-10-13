DROP TABLE IF EXISTS {table};
CREATE TABLE {table} {
        hvid            string, 
        patient_gender  string, 
        patient_state   string, 
        patient_age     string, 
        drug            string, 
        diagnosis       string, 
        lab             string, 
        procedure       string,
        date_service    date
        }
    {properties}
    ;