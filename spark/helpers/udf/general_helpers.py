#! /usr/bin/python
import hashlib
import json
import re
from datetime import datetime
from collections import defaultdict


def clean_up_freetext(val, remove_periods=False):
    """
    Remove all characters that are not numbers, letters, spaces, periods, or ampersands
    Convert multiple consequtive spaces into a single space
    """
    if val is None:
        return None
    try:
        new_val = re.sub(r'  *', ' ', re.sub(r'[^A-Za-z0-9 .&#%]', ' ', val)).strip()
        if remove_periods:
            new_val = new_val.replace('.', '')
        return new_val
    except ValueError:
        return None


def extract_number(text):
    if not text:
        return None

    if len(text) - len(text.replace('.', '')) == 1:
        try:
            return float(re.sub('[^0-9.]', '', text))
        except ValueError:
            return None
    else:
        try:
            return float(re.sub('[^0-9]', '', text))
        except ValueError:
            return None


def cap_date(d, min_date, max_date):
    if d is None:
        return None

    min_date = min_date or d.min
    max_date = max_date or d.max
    return d if d and max_date >= d >= min_date else None


def extract_date(text, pattern, min_date=None, max_date=None):
    try:
        return cap_date(datetime.strptime(text, pattern).date(), min_date, max_date)
    except (ValueError, TypeError):
        return None


def extract_currency(text):
    try:
        # remove non-numeric characters
        return float(re.sub('[^0-9.]', '', text))
    except (TypeError, ValueError):
        return None


def convert_value(value, conversion):
    converters = {
        'KILOGRAMS_TO_POUNDS': convert_kg_to_lb,
        'CENTIMETERS_TO_INCHES': convert_cm_to_in,
        'CENTIGRADE_TO_FAHRENHEIT': convert_celsius_to_fahrenheit,
        'METERS_TO_INCHES': convert_m_to_in
    }

    try:
        return converters[conversion](value)
    except (KeyError, TypeError):
        return None


def convert_kg_to_lb(value):
    try:
        return round(float(value) * 2.2046, 2)
    except (TypeError, ValueError):
        return None


def convert_cm_to_in(value):
    try:
        return round(float(value) * 0.3937, 2)
    except (TypeError, ValueError):
        return None


def convert_m_to_in(value):
    try:
        return round(float(value) * 39.3701, 2)
    except (TypeError, ValueError):
        return None


def convert_celsius_to_fahrenheit(value):
    try:
        return round((float(value) * 9 / 5) + 32, 2)
    except (TypeError, ValueError):
        return None


def create_range(max):
    try:
        return ','.join([str(i) for i in range(max)])
    except:
        return None


# Takes 2 sets as colon-separated strings, and the returns the difference between
# them as a colon-separated string
def string_set_diff(s1, s2):
    if s1 is None:
        return None
    if s2 is None:
        s2 = ''

    s1s = [v2.split('_')[0] for v2 in [v1 for v1 in s1.split(':') if v1 is not None and len(v1) > 0]]
    s2s = [v2.split('_')[0] for v2 in [v1 for v1 in s2.split(':') if v1 is not None and len(v1) > 0]]

    return ':'.join(set(s1s).difference(set(s2s)))


# Takes a list as a colon-sparated string, and returns a unique list of values as
# a colon-separated string
def uniquify(with_dupes):
    if with_dupes is None:
        return None
    return ':'.join({x for x in with_dupes.split(':') if x is not None and len(x) > 0})


def is_int(val):
    """Test whether input value can be parsed to an integer"""
    if type(val) is int:
        return True
    elif type(val) is float:
        return val.is_integer()
    else:
        try:
            int(val)
            return True
        except (ValueError, TypeError):
            return False


def obfuscate_hvid(hvid, salt):
    if salt is None or len(salt) == 0:
        raise ValueError("A project-specific salt must be provided to properly obfuscate the HVID")
    if hvid is None or hvid.strip() == '':
        return None
    return hashlib.md5(hvid.encode('UTF-8') + salt.encode('UTF-8')).hexdigest()


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
    for i in range(int(len(key) / 4)):
        key_p = key[i * 4: (i + 1) * 4]
        xor = ((ord(key_p[0]) ^ (i * 4)) * (1 << 24) +
               (ord(key_p[1]) ^ (i * 4 + 1)) * (1 << 16) +
               (ord(key_p[2]) ^ (i * 4 + 2)) * (1 << 8) +
               (ord(key_p[3]) ^ (i * 4 + 3)))
        res = res ^ xor
    return res


def slightly_deobfuscate_hvid(hvid, key):
    # Obfuscation and de-obfuscation are symmetric
    return slightly_obfuscate_hvid(hvid, key)


def remove_split_suffix(filename, include_parent_dirs=False):
    """Remove suffix added by the split_push_files subdag"""
    # remove suffix if found
    if re.search(r'\.[a-z]{2}\.[^.]+$', filename):
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


def densify_2d_array_by_key(arr):
    result = [subarr for subarr in arr if subarr[0] is not None]
    return result if result else [arr[0]]


def obfuscate_candidate_hvids(arr, salt):
    if arr is None:
        return None

    res = []
    for i in range(len(arr)):
        res.append([obfuscate_hvid(str(int(arr[i][0])), salt), arr[i][1]])

    return res


def find_descendants_recursively(parents, children):
    """
    Find descendants recursively. This function will primarily be used to help in the creation of
    synthetic encounters and events.
    :param parents: list of ints. ex:  [1, 1, 2, 2, 2, 2, 3, 4, 5, 5, 5, 5]
    :param children: list of ints. ex: [1, 2, 1, 2, 3, 4, 3, 4, 5, 6, 7, 8]
    :return: full paths of parent-child relationships.  {1: {1, 2, 3, 4}, 5: {5, 6, 7, 8}}
    """
    parent_child = list(zip(parents, children))
    parent_child = sorted(parent_child)
    result_dict = defaultdict(list)
    reverse_dict = defaultdict(int)

    def find_grandparent(value):
        return reverse_dict[value]

    for parent, child in parent_child:
        grandparent = find_grandparent(parent)
        # if the parent does not already exists as a child in the result_dic
        if grandparent == 0:
            result_dict[parent].append(child)
            reverse_dict[child] = parent
        else:
            reverse_dict[parent] = grandparent
            reverse_dict[child] = grandparent
            result_dict[grandparent].append(parent)
            result_dict[grandparent].append(child)

    # unique the dictionary values and convert to standard dict type for UDF
    for key in result_dict.keys():
        result_dict[key] = list(set(result_dict[key]))

    return dict(result_dict)
