import pytest
import spark.helpers.external_table_loader as external_table_loader

icd_diag_results = []
icd_proc_results = []
cpt_results = []
hcpcs_results = []
gen_ref_results = []

@pytest.mark.usefixtures("spark")
def test_init(spark):
    external_table_loader.load_icd_diag_codes(spark['sqlContext'])
    external_table_loader.load_icd_proc_codes(spark['sqlContext'])
    external_table_loader.load_hcpcs_codes(spark['sqlContext'])
    external_table_loader.load_cpt_codes(spark['sqlContext'])
    external_table_loader.load_ref_gen_ref(spark['sqlContext'])

    global icd_diag_results, icd_proc_results, cpt_results, hcpcs_results, gen_ref_results
    icd_diag_results = spark['sqlContext'].sql('SELECT * FROM icd_diag_codes').collect()
    icd_proc_results = spark['sqlContext'].sql('SELECT * FROM icd_proc_codes').collect()
    cpt_results = spark['sqlContext'].sql('SELECT * FROM cpt_codes').collect()
    hcpcs_results = spark['sqlContext'].sql('SELECT * FROM hcpcs_codes').collect()
    gen_ref_results = spark['sqlContext'].sql('SELECT * FROM ref_gen_ref').collect()

def test_icd_diag_codes():
    """Ensure a list of ICD diagnosis codes (9 and 10) is loaded that contains
    no blacklisted codes"""

    # Valid ICD9 code
    assert len([r for r in icd_diag_results if r.code == '40210']) == 1

    # Valid ICD10 code
    assert len([r for r in icd_diag_results if r.code == 'I2510']) == 1

    # Valid, but blacklisted ICD10 code
    assert len([r for r in icd_diag_results if r.code == 'V200XXA']) == 0

def test_icd_proc_codes():
    """Ensure a list of ICD procedure codes (9 and 10) is loaded"""

    assert len([r for r in icd_proc_results if r.code == '0449']) == 1

    assert len([r for r in icd_proc_results if r.code == '05N53ZZ']) == 1

def test_cpt_codes():
    """Ensure a list of CPT procedure codes is loaded"""

    assert len([r for r in cpt_results if r.code == '90634']) == 1

def test_hcpcs_codes():
    """Ensure a list of HCPCS procedure codes is loaded"""

    assert len([r for r in hcpcs_results if r.hcpc == 'A4282']) == 1

def test_gen_ref_data():
    """Ensure the general reference table is loaded from the analytics DB"""

    assert len([r for r in gen_ref_results if r.gen_ref_domn_nm == 'VITAL_SIGN_TYPE'
                and r.gen_ref_cd == 'WEIGHT']) == 1
