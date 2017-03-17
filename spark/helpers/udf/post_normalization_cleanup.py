#! /usr/bin/python
import datetime
import re


def uppercase_code(code):
    try:
        return code.upper()
    except:
        return None


def clean_up_alphanumeric_code(code):
    """
    Remove non-alphanumeric characters from code
    """
    try:
        return re.sub(r'[^A-Za-z0-9]', '', code)
    except:
        return None


def clean_up_numeric_code(code):
    """
    Remove non-numeric characters from code
    """
    try:
        return re.sub(r'[^0-9]', '', code)
    except:
        return None


# These codes are specific enough that along with other public fields they pose a
# re-identification risk, nullify them
# ICD9
#   764*-779* Other conditions originating In the perinatal period (including birth trauma)
#   V3* Liveborn infants according to type of birth
#   798 Unknown cause of death
#   7999 Unknown cause of death
#   E99* Operations of war
#   E97* Legal intervention
#   E96* Assault/Homicide
#   E95* Suicide
#   E9280 Prolonged stay in weightlessness
#   E910* Drowning
#   E913* Suffication
#   E80*-E84* Vehicle accident
# ICD10
#   P* Other conditions originating In the perinatal period (including birth trauma)
#   Z38* Liveborn infants according to type of birth
#   R99 Unknown cause of death
#   Y36* Operations of war
#   Y37* Military operations
#   Y35* Legal intervention
#   Y38* Terrorism
#   X92*-Y09* Assault/Homicide
#   X52* Prolonged stay in weightlessness
#   W65*-W74* Drowning
#   V* Vehicle accident

# These codes are specific enough that along with other public fields
# they pose a re-identification risk, make them more generic
# ICD9
#   V85.41 - V85.45 Body Mass Index 40 and over
# ICD10
#   Z68.41 - Z68.45 Body Mass Index 40 and over
def clean_up_diagnosis_code(
        diagnosis_code, diagnosis_code_qual, date_service
):
    diagnosis_code = uppercase_code(clean_up_alphanumeric_code(diagnosis_code))
    if diagnosis_code_qual == '01' or (
            diagnosis_code_qual is None
            and isinstance(date_service, datetime.date)
            and date_service < datetime.date(2015, 10, 1)
    ):
        if re.search(
                '^(76[4-9].*|77.*|V3.*|79[89]|7999|E9[5679].*|E9280|E910.*|E913.*|E8[0-4].*)$',
                diagnosis_code
        ):
            return None
        if re.search('^V854[1-5]$', diagnosis_code):
            return 'V854'
    if diagnosis_code_qual == '02' or (
            diagnosis_code_qual is None
            and isinstance(date_service, datetime.date)
            and date_service >= datetime.date(2015, 10, 1)
    ):
        if re.search(
                '^(P.*|Z38.*|R99|Y3[5-8].*|X9[2-9].*|Y0.*|X52.*|W6[5-9].*|W7[0-4].*|V.*)$',
                diagnosis_code
        ):
            return None
        if re.search('^Z684[1-5]$', diagnosis_code):
            return 'Z684'
    return diagnosis_code


def clean_up_procedure_code(procedure_code):
    return uppercase_code(clean_up_alphanumeric_code(procedure_code))


def clean_up_ndc_code(ndc_code):
    if isinstance(ndc_code, str) and len(ndc_code) == 11:
        return clean_up_numeric_code(ndc_code)
    else:
        return None


# These places of service pose a risk of revealing the patient's
# residence, set them to unkown, and remove data about them
# 5 Indian Health Service Free-standing Facility
# 6 Indian Health Service Provider-based Facility
# 7 Tribal 638 Free-Standing Facility
# 8 Tribal 638 Provider-based Facility
# 9 Prison/Correctional Facility
# 12 Home
# 13 Assissted Living Facility
# 14 Group Home
# 33 Custodial Care Facility
def obscure_place_of_service(place_of_service_std_id):
    if place_of_service_std_id in [
            '5', '05', '6', '06', '7', '07', '8',
            '08', '9', '09', '12', '13', '14', '33'
    ]:
        return '99'
    else:
        return place_of_service_std_id


def filter_due_to_place_of_service(prov_detail, place_of_service_std_id):
    if place_of_service_std_id in [
            '5', '05', '6', '06', '7', '07', '8',
            '08', '9', '09', '12', '13', '14', '33'
    ]:
        return None
    else:
        return prov_detail


def scrub_discharge_status(discharge_status):
    if discharge_status in ['69', '87']:
        return '0'
    else:
        return discharge_status


def nullify_drg_blacklist(drg_code):
    if drg_code in ['283', '284', '285', '789']:
        return None
    else:
        return drg_code


# Age caps
def cap_age(age):
    try:
        return '90' if int(age) > 85 else age
    except:
        return None


def cap_year_of_birth(age, date_service, year_of_birth):
    """ Cap year of birth if age or birth year over 85 """
    try:
        is_year_cap = (date_service.year - int(year_of_birth)) > 85
        is_age_cap = int(age) > 85

        if is_year_cap or is_age_cap:
            year_of_birth = date_service.year - 90

    finally:
        return year_of_birth
