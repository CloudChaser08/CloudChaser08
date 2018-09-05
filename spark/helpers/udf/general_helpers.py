#! /usr/bin/python
from datetime import datetime
import json
import re
import hashlib


def clean_up_freetext(val, remove_periods=False):
    """
    Remove all characters that are not numbers, letters, spaces, periods, or ampersands
    Convert multiple consequtive spaces into a single space
    """
    try:
        new_val = re.sub(r'  *', ' ', re.sub(r'[^A-Za-z0-9 .&#%]', ' ', val)).strip()
        if remove_periods:
            new_val = new_val.replace('.', '')
        return new_val
    except:
        return None


def extract_number(text):
    if text is None or text == '':
        return None

    if len(text) - len(text.replace('.','')) == 1:
        try:
            return float(re.sub('[^0-9.]', '', text))
        except Exception:
            return None
    else:
        try:
            return float(re.sub('[^0-9]', '', text))
        except Exception:
            return None


def cap_date(d, min_date, max_date):
    if not d:
        return None
    elif (
        min_date is not None and d < min_date
    ) or (
        max_date is not None and d > max_date
    ):
        return None
    else:
	return d
        #return d.isoformat().split("T")[0]


def extract_date(text, pattern, min_date=None, max_date=None):
    if text is None or text == '':
        return None
    try:
        d = datetime.strptime(text, pattern).date()
    except Exception:
        return None

    return cap_date(d, min_date, max_date)


def extract_currency(text):
    try:
        # remove non-numeric characters
        text = re.sub('[^0-9.]', '', text)

        return float(text)
    except:
        return None


def convert_value(value, conversion):
    if value is not None:
        if conversion == 'KILOGRAMS_TO_POUNDS':
            return convert_kg_to_lb(value)
        elif conversion == 'CENTIMETERS_TO_INCHES':
            return convert_cm_to_in(value)
        elif conversion == 'CENTIGRADE_TO_FAHRENHEIT':
            return convert_celsius_to_fahrenheit(value)
        elif conversion == 'METERS_TO_INCHES':
            return convert_m_to_in(value)
        elif conversion == '' or conversion is None:
            return value


def convert_kg_to_lb(value):
    try:
        return round(float(value) * 2.2046, 2)
    except:
        return None


def convert_cm_to_in(value):
    try:
        return round(float(value) * 0.3937, 2)
    except:
        return None


def convert_m_to_in(value):
    try:
        return round(float(value) * 39.3701, 2)
    except:
        return None


def convert_celsius_to_fahrenheit(value):
    try:
        return round((float(value) * 9 / 5) + 32, 2)
    except:
        return None


def create_range(max):
    try:
        return ','.join(map(lambda i: str(i), range(max)))
    except:
        return None


# Takes 2 sets as colon-separated strings, and the returns the difference between
# them as a colon-separated string
def string_set_diff(s1,s2):
    if s1 is None:
        return None
    if s2 is None:
        s2 = ''

    s1s = map(lambda x : x.split('_')[0], filter(lambda x: x is not None and len(x) > 0, s1.split(':')))
    s2s = map(lambda x : x.split('_')[0], filter(lambda x: x is not None and len(x) > 0, s2.split(':')))

    return ':'.join(set(s1s).difference(set(s2s)))

# Takes a list as a colon-sparated string, and returns a unique list of values as
# a colon-separated string
def uniquify(with_dupes):
    if with_dupes is None:
        return None;
    return ':'.join(set(filter(lambda x: x is not None and len(x) > 0, with_dupes.split(':'))))


def is_int(val):
    "Test whether input value can be parsed to an integer"
    if type(val) is int:
        return True
    elif type(val) is float:
        return val.is_integer()
    else:
        try:
            int(val)
            return True
        except ValueError:
            return False
        except TypeError:
            return False


def obfuscate_hvid(hvid, salt):
    if salt is None or len(salt) == 0:
        raise ValueError("A project-specific salt must be provided to properly obfuscate the HVID")
    hvid = "" if hvid is None else hvid
    return hashlib.md5(hvid + salt).hexdigest().upper()


def slightly_obfuscate_hvid(hvid, key):
    if key is None or len(key) == 0:
        raise ValueError("A project-specific key must be provided to properly obfuscate the HVID")
    if hvid is None:
        return None
    if type(hvid) is not int:
        raise ValueError("Only integer HVIDs are expected")
    res = hvid
    # Pad cyclically so the key length is a multiple of 4
    if len(key) % 4 != 0:
        key = key + key[:4 - len(key) % 4]
    # Do multiple rounds of XORing of the id with different
    # parts of the key
    for i in xrange(len(key) / 4):
        key_p = key[i * 4 : (i + 1) * 4]
        xor = ((ord(key_p[0]) ^ (i * 4)) * (1 << 24) + \
                (ord(key_p[1]) ^ (i * 4 + 1)) * (1 << 16) + \
                (ord(key_p[2]) ^ (i * 4 + 2)) * (1 << 8) + \
                (ord(key_p[3]) ^ (i * 4 + 3)))
        res = res ^ xor
    return res


def slightly_deobfuscate_hvid(hvid, key):
    # Obfuscation and de-obfuscation are symmetric
    return slightly_obfuscate_hvid(hvid, key)


def remove_split_suffix(filename, include_parent_dirs=False):
    "Remove suffix added by the split_push_files subdag"
    # remove suffix if found
    if re.search('\.[a-z]{2}\.[^.]+$', filename):
        filename = '.'.join(filename.split('.')[:-2])

    # strip parent dirs if option is specified
    return filename if include_parent_dirs else filename.split('/')[-1]


def to_json(val):
    return json.dumps(val)

# Some of our normalizations involve exploding a sparse array, keeping only the
# non-NULL values, and numbering the rows sequentially. Removing NULL values
# and NULL structures from the array will help achieve that
#
# Expected input array of scalar values
# Output: Array of non-NULL scalar values
def densify_scalar_array(arr):
    return [v for v in arr if v is not None]

# Expected input array of arrays of scalar values
# Output: Array of arrays that contain at least one non-NULL scalar value
#           OR Array of 1 array full of NULL values if the original array did
#               not contain a single array with at least one non-NULL value
def densify_2d_array(arr):
    res = []
    for sub_arr in arr:
        if [v for v in sub_arr if v is not None]:
            res.append(sub_arr)
    if not res:
        return [arr[0]]
    return res

def obfuscate_candidate_hvids(arr, salt):
    if arr is None:
        return None

    res = []
    for i in xrange(len(arr)):
        res.append([obfuscate_hvid(str(int(arr[i][0])), salt), arr[i][1]])

    return res
