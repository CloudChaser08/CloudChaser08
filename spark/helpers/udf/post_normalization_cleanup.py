#! /usr/bin/python
import datetime
import re
import logging

import spark.helpers.constants as constants


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


def clean_up_ndc_code(code):
    """
    Remove any characters other than numbers.
    Convert to NULL any remaining value that is less than 8 in length.
    """
    clean_code = clean_up_numeric_code(code)
    if clean_code is None:
        return clean_code
    return None if len(clean_code) < 8 or re.sub('0', '', clean_code) == '' else clean_code


# These codes are specific enough that along with other public fields they pose a
# re-identification risk, nullify them
# ICD9
#   V20.31 Health supervision for newborn under 8 days old
#   V20.32 Health supervision for newborn 8 to 28 days old
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
#   Z00.110 Health examination for newborn under 8 days old
#   Z00.111 Health examination for newborn 8 to 28 days old
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
    if diagnosis_code == None:
        return None

    # Removes non-alphanumeric codes including '.'
    diagnosis_code = uppercase_code(clean_up_alphanumeric_code(diagnosis_code))

    # Is this an ICD-9 code based on qualifier, or not an ICD-10 code based on date
    if diagnosis_code_qual == '01' or (
            diagnosis_code_qual is None
            and not (
                isinstance(date_service, datetime.date)
                and date_service >= datetime.date(2015, 10, 1)
            )
    ):
        if re.search(
                '^(76[4-9].*|77.*|V3.*|V203(1|2)|79[89]|7999|E9[5679].*|E9280|E910.*|E913.*|E8[0-4].*)$',
                diagnosis_code
        ):
            return None
        if re.search('^V854[1-5]$', diagnosis_code):
            return 'V854'
    # Is this an ICD-10 code based on qualifier, or not an ICD-9 code based on date
    if diagnosis_code_qual == '02' or (
            diagnosis_code_qual is None
            and not (
                isinstance(date_service, datetime.date)
                and date_service < datetime.date(2015, 10, 1)
            )
    ):
        if re.search(
                '^(P.*|Z38.*|R99|Y3[5-8].*|X9[2-9].*|Z0011(0|1)|Y0.*|X52.*|W6[5-9].*|W7[0-4].*|V.*)$',
                diagnosis_code
        ):
            return None
        if re.search('^Z684[1-5]$', diagnosis_code):
            return 'Z684'

    return diagnosis_code


def clean_up_procedure_code(procedure_code):
    if procedure_code:
        clean_code = uppercase_code(re.sub('[^a-zA-Z0-9]', ' ', procedure_code))

        if clean_code.strip():
            up_to_first_space = clean_code.split()[0]
            return up_to_first_space[:7] if up_to_first_space else None


def clean_up_loinc_code(loinc_code):
    return clean_up_numeric_code(loinc_code)


def clean_up_npi_code(npi_code):
    cleaned = clean_up_numeric_code(npi_code)
    if cleaned and len(cleaned) == 10 and re.sub('0', '', cleaned) != '':
        return cleaned
    else:
        return None


def nullify_due_to_level_of_service(target_column, level_of_service):
    if level_of_service == '6':
        return None
    else:
        return target_column


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
bad_pos_codes = [
    '5', '05', '6', '06', '7', '07', '8',
    '08', '9', '09', '12', '13', '14', '33', '99'
]


def obscure_place_of_service(place_of_service_std_id):
    if place_of_service_std_id in bad_pos_codes:
        return '99'
    else:
        return place_of_service_std_id


def filter_due_to_place_of_service(prov_detail, place_of_service_std_id):
    if place_of_service_std_id in bad_pos_codes:
        return None
    else:
        return prov_detail


def obscure_inst_type_of_bill(inst_type_of_bill):
    if str(inst_type_of_bill).startswith('3'):
        return 'X' + str(inst_type_of_bill)[1:]
    else:
        return inst_type_of_bill


def filter_due_to_inst_type_of_bill(prov_detail, inst_type_of_bill):
    if str(inst_type_of_bill).startswith('3'):
        return None
    else:
        return prov_detail


def scrub_discharge_status(discharge_status):
    if discharge_status in ['20', '21', '40', '41', '42', '69', '87']:
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
    """
    Convert age values >= 85 to 90
    """
    if age is None or age == '':
        return
    try:
        return '90' if int(age) >= 85 else \
            None if int(age) < 0 else age
    except:
        return None


def validate_age(age, date_service, year_of_birth):
    """
    Ensure that the age is within 2 years of its derivation from
    date_service and year_of_birth.

    If the age is 0 and either date_service of year_of_birth is none,
    consider '0' a null value and nullify the age.
    """
    try:
        age = int(age)
    except TypeError:
        return

    if date_service is None or year_of_birth is None:
        if age == 0:
            return None
        else:
            return age
    elif isinstance(date_service, datetime.date):
        try:
            year_of_birth = int(year_of_birth)
        except TypeError:
            return age

        if abs(age - (date_service.year - year_of_birth)) > 2:
            return None
        else:
            return age
    else:
        return age


def cap_year_of_birth(age, date_service, year_of_birth):
    """ Cap year of birth if age is 85 and over """
    try:
        if (
                isinstance(date_service, datetime.date)
                and year_of_birth is not None
                and year_of_birth != ''
        ):
            if (date_service.year - int(year_of_birth)) >= 85:
                return 1927
            return year_of_birth

        elif (
                age is not None
                and age != ''
        ):
            if int(age) >= 85 and year_of_birth is not None:
                return 1927
            return year_of_birth

        elif (
                year_of_birth is not None
                and datetime.datetime.today().year - int(year_of_birth) >= 85
        ):
            return 1927

        else:
            return year_of_birth

    except:
        return None


# Gender filtering
def clean_up_gender(gender):
    if gender and str(gender).strip().upper() in constants.genders:
        return str(gender).strip().upper()
    else:
        return 'U'


# Mask 3 digit zip codes that have very small populations
def mask_zip_code(zip_code):
    if zip_code in ["036", "102", "203", "205", "369", "556", "692", "821", \
            "823", "878", "879", "884", "893"]:
        return "000"
    else:
        return zip_code


def validate_state_code(state):
    try:
        if state and str(state).strip().upper() in constants.states:
            return str(state).strip().upper()
        else:
            return None
    except:
        return None


vital_sign_units = {
    'HEIGHT' : ['INCHES'],
    'WEIGHT' : ['POUNDS'],
    'BMI'    : ['INDEX', 'PERCENT']
}


# age,gender,min height in,max height in(at age+1),min weight lb,max weight lb(at age+1),min BMI,max BMI,min head circumference,max head circumference
# the index of each entry corresponds to the age associated with these vital
# sign values
gender_age_vital_sign_caps = {
    'M' : [
        [18.0, 31.9, 5.2, 27.3, 12.2, 18.4, 13.0, 18.6], # Birth up to, but not including, 1 year
        [27.7, 37.3, 16.5, 34.8, 15.5, 17.1, 17.6, 19.4], # 1 year up to, but not including, 2 years
        [31.8, 41.1, 20.6, 41.7, 14.5, 18.5, None, None],
        [34.1, 44.1, 24.9, 49.1, 13.9, 18.6, None, None],
        [36.5, 47.0, 28.0, 57.8, 13.8, 18.2, None, None],
        [38.6, 49.9, 31.3, 67.5, 13.7, 19.1, None, None],
        [40.9, 52.9, 34.7, 78.9, 13.5, 18.9, None, None],
        [43.1, 55.7, 38.3, 92.3, 13.7, 19.4, None, None],
        [45.3, 58.3, 42.2, 107.8, 13.7, 20.5, None, None],
        [47.2, 60.7, 46.2, 124.6, 13.8, 23.0, None, None],
        [48.8, 63.0, 50.5, 141.5, 14.4, 23.1, None, None],
        [50.4, 65.6, 55.1, 158.0, 14.4, 24.9, None, None],
        [52.2, 68.6, 60.8, 173.6, 14.9, 25.8, None, None],
        [54.4, 71.5, 67.8, 188.5, 15.5, 27.3, None, None],
        [57.0, 73.6, 76.3, 202.9, 16.2, 26.3, None, None],
        [59.3, 74.7, 85.6, 216.8, 16.5, 27.7, None, None],
        [61.1, 75.3, 95.0, 229.5, 17.4, 29.0, None, None],
        [62.2, 75.7, 102.8, 238.7, 17.5, 29.6, None, None],
        [62.8, 75.9, 108.2, 242.5, 17.8, 30.4, None, None],
        [63.0, 76.0, 111.5, 245.3, 19.0, 31.4, None, None] # 19 years up to, but not including, 20 years
    ] + \
        (10 * [[63.1, 74.8, 113.0, 270.3, 19.2, 38.9, None, None]] + #20-29
         10 * [[64.1, 74.7, 137.9, 266.4, 21, 37.7, None, None]] +    #30-39
         10 * [[65.2, 74.0, 145.4, 275.0, 22.1, 38.8, None, None]] +  #40-49
         10 * [[65.0, 74.4, 142.2, 274.3, 21.1, 39.3, None, None]] +  #50-59
         10 * [[64.2, 73.7, 140.7, 267.4, 21.5, 38.5, None, None]] +  #60-69
         10 * [[63.8, 73.1, 135.4, 257.4, 21.1, 36.5, None, None]] +  #70-79
         11 * [[62.7, 71.3, 122.1, 219.9, 19.7, 33.3, None, None]]),  #80-90
    'F' : [
        [17.7, 31.4, 5.1, 26.3, 12.1, 18.1, 12.9, 18.1],  # Birth up to, but not including, 1 year
        [26.8, 36.9, 15.0, 34.0, 15.0, 16.9, 17.2, 19.0], # 1 year up to, but not including, 2 years
        [31.1, 40.6, 19.3, 42.1, 14.2, 18.6, None, None],
        [33.5, 43.7, 24.0, 50.6, 13.7, 18.5, None, None],
        [35.9, 46.9, 26.9, 60.1, 13.6, 18.9, None, None],
        [38.3, 50.1, 30.2, 70.6, 13.3, 19.7, None, None],
        [40.8, 53.1, 33.6, 82.2, 13.2, 19.3, None, None],
        [43.2, 55.8, 37.1, 95.4, 13.3, 19.9, None, None],
        [45.2, 58.2, 40.8, 110.8, 13.5, 21.6, None, None],
        [47.0, 60.6, 44.8, 128.2, 13.8, 24.2, None, None],
        [48.5, 63.3, 49.6, 146.5, 13.9, 23.7, None, None],
        [50.2, 66.1, 55.2, 165.0, 14.0, 26.9, None, None],
        [52.7, 68.0, 61.8, 181.9, 15.0, 27.1, None, None],
        [55.5, 69.1, 69.0, 197.1, 15.1, 30.0, None, None],
        [57.2, 69.6, 76.3, 210.1, 16.3, 29.4, None, None],
        [57.9, 69.8, 82.8, 220.7, 16.8, 29.8, None, None],
        [58.2, 70.0, 88.0, 228.1, 17.2, 31.7, None, None],
        [58.3, 70.0, 91.5, 231.0, 17.5, 32.2, None, None],
        [58.4, 70.1, 93.5, 230.1, 16.7, 28.8, None, None],
        [58.4, 70.1, 94.5, 228.1, 18.3, 31.4, None, None], # 19 years up to, but not including, 20 years
    ] + \
        (10 * [[58.5, 68.0, 94.9, 244.2, 18.2, 41.3, None, None]] + #20-29
         10 * [[60.0, 68.6, 112.8, 251.8, 19.4, 41.2, None, None]] + #30-39
         10 * [[59.9, 68.5, 115.3, 257.7, 19.8, 43.5, None, None]] + #40-49
         10 * [[59.3, 67.9, 114.1, 259.7, 20.2, 44.1, None, None]] + #50-59
         10 * [[59.8, 67.5, 114.3, 248.8, 20.6, 42.0, None, None]] + #60-69
         10 * [[58.6, 66.7, 109.0, 218.0, 19.6, 39.6, None, None]] + #70-79
         11 * [[57.5, 65.4, 101.7, 190.5, 19.7, 34.3, None, None]])  #80-90
}

sign_units_cap_idxs = {
    'HEIGHT_INCHES' : [0, 1],
    'WEIGHT_POUNDS' : [2, 3],
    'BMI_INDEX'     : [4, 5]
}

def clean_up_vital_sign(sign_type, sign_measurement, sign_units, gender, age, year_of_birth, measurement_date, encounter_date):
    if sign_type not in ['HEIGHT', 'WEIGHT', 'BMI']:
        return sign_measurement

    if sign_units not in vital_sign_units.get(sign_type, []):
        logging.warning("Unknown unit of measure, {}".format(sign_units))
        return None

    if gender not in ['M', 'F']:
        return None

    m_date = measurement_date if isinstance(measurement_date, datetime.date) else encounter_date

    if age is not None and age != '':
        age = age
    elif isinstance(m_date, datetime.date) and year_of_birth is not None \
            and year_of_birth != '':
        age = str(m_date.year - int(year_of_birth))
    else:
        return None

    if cap_age(age) is None:
        return None
    else:
        age = int(cap_age(age))

    try:
        float(sign_measurement)
    except:
        return None

    if sign_units == 'PERCENT':
        if 1.0 < float(sign_measurement) < 99.0:
            return sign_measurement
        else:
            return None

    cap_idxs = sign_units_cap_idxs[sign_type + '_' + sign_units]
    caps = [
        gender_age_vital_sign_caps[gender][age][cap_idxs[0]],
        gender_age_vital_sign_caps[gender][age][cap_idxs[1]]
    ]

    try:
        float(sign_measurement)
    except TypeError:
        return None

    if caps[0] < float(sign_measurement) < caps[1]:
        return sign_measurement
    else:
        return None
