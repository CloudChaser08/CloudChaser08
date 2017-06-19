DROP TABLE IF EXISTS enrollmentrecords;
CREATE TABLE enrollmentrecords (
    record_id               bigint,
    hvid                    string,
    created                 date,
    model_version           string,
    data_set                string,
    data_feed               string,
    data_vendor             string,
    source_version          string,
    patient_age             string,
    patient_year_of_birth   string,
    patient_zip             string,
    patient_state           string,
    patient_gender          string,
    source_record_id        string,
    source_record_qual      string,
    source_record_date      date,
    date_start              date,
    date_end                date,
    benefit_type            string,
    part_provider           string,
    part_best_date          string
);

INSERT INTO enrollmentrecords VALUES
("6200","567891234","2017-03-22","1","10130X001_HV_RX_ENROLLMENT_c170104.txt.decrypted","16","17","","","1968","371","TN","F","","","2015-12-22","2015-01-01","3000-12-31","","express_scripts","2017-01"),
("14200","912345678","2017-03-22","1","10130X001_HV_RX_ENROLLMENT_c170104.txt.decrypted","16","17","","","1961","859","AZ","M","","","2013-09-23","2009-01-01","2013-09-23","","express_scripts","2017-01"),
("22200","891234567","2017-03-22","1","10130X001_HV_RX_ENROLLMENT_c170104.txt.decrypted","16","17","","","2001","314","GA","M","","","2016-12-10","2015-01-01","2015-12-31","","express_scripts","2017-01"),
("30200","789123456","2017-03-22","1","10130X001_HV_RX_ENROLLMENT_c170104.txt.decrypted","16","17","","","1942","483","MI","M","","","2013-12-31","2012-08-01","2012-12-31","","express_scripts","2017-01"),
("38200","678912345","2017-03-22","1","10130X001_HV_RX_ENROLLMENT_c170104.txt.decrypted","16","17","","","1952","023","MA","M","","","2015-02-22","2007-10-15","2014-12-31","","express_scripts","2017-01"),
("46200","123456789","2017-03-22","1","10130X001_HV_RX_ENROLLMENT_c170104.txt.decrypted","16","17","","","2005","331","FL","M","","","2014-10-29","2014-04-01","2014-09-30","","express_scripts","2017-01"),
("54200","456789123","2017-03-22","1","10130X001_HV_RX_ENROLLMENT_c170104.txt.decrypted","16","17","","","1981","656","MO","M","","","2016-08-05","2014-04-01","2015-12-31","","express_scripts","2017-01"),
("62200","345678912","2017-03-22","1","10130X001_HV_RX_ENROLLMENT_c170104.txt.decrypted","16","17","","","1971","920","CA","F","","","2011-11-08","2011-11-01","2011-11-01","","express_scripts","2017-01"),
("70200","219876543","2017-03-22","1","10130X001_HV_RX_ENROLLMENT_c170104.txt.decrypted","16","17","","","1966","468","IN","M","","","2007-01-05","2007-01-01","3000-12-31","","express_scripts","2017-01"),
("78200","312987654","2017-03-22","1","10130X001_HV_RX_ENROLLMENT_c170104.txt.decrypted","16","17","","","1979","145","NY","F","","","2011-12-05","2009-07-01","2011-12-31","","express_scripts","2017-01");
