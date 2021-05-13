import re

replacements = {
    "NEG" : "NEGATIVE",
    "FINAL RSLT: NEGATIVE" : "NEGATIVE",
    "NEGATIVE" : "NEGATIVE",
    "NEGATIVE CONFIRMED" : "NEGATIVE CONFIRMED",
    "NEGA CONF" : "NEGATIVE CONFIRMED",
    "POS" : "POSITIVE",
    "POSITIVE" : "POSITIVE",
    "None Detected" : "NOT DETECTED",
    "Not Detected" : "NOT DETECTED",
    "NON-REACTIVE" : "NON-REACTIVE",
    "Nonreactive" : "NON-REACTIVE",
    "Not Indicated" : "NOT INDICATED",
    "CONSISTENT" : "CONSISTENT",
    "INCONSISTENT" : "INCONSISTENT",
    "DNRTNP" : "DO NOT REPORT",
    "DNR" : "DO NOT REPORT",
    "NOT INTERPRETED~DNR" : "DO NOT REPORT",
    "TNP124" : "TEST NOT PERFORMED",
    "DETECTED" : "DETECTED",
    "REACTIVE" : "REACTIVE",
    "EQUIVOCAL" : "EQUIVOCAL",
    "INDETERMINATE" : "INDETERMINATE",
    "INCONCLUSIVE" : "INCONCLUSIVE",
    "NOT ISOLATED" : "NOT ISOLATED",
    "ISOLATED" : "ISOLATED",
    "NO CULTURE INDICATED" : "NO CULTURE INDICATED",
    "CULTURE INDICATED" : "CULTURE INDICATED",
    "INDICATED" : "INDICATED",
    "EQUIOC" : "EQUIVOCAL",
    "INDETERMINANT" : "INDETERMINATE",
    "NEG/" : "NEGATIVE",
    "POS/" : "POSITIVE",
    "DETECTED ABN" : "DETECTED",
    "DETECTED (A)" : "DETECTED",
    "NON-DETECTED" : "NOT DETECTED",
    "NO VARIANT DETECTED" : "NO VARIANT DETECTED",
    "^TNP124" : "TEST NOT PERFORMED",
    "ADD^TNP167" : "TEST NOT PERFORMED",
    "DTEL^TNP1003" : "TEST NOT PERFORMED",
    "TNP QUANTITY NOT SUFFICIENT" : "TEST NOT PERFORMED",
    "TNP124^CANC" : "TEST NOT PERFORMED",
    "TA/DNR" : "DO NOT REPORT",
    "DETECTED (A1)" : "DETECTED",
    "NEGATI" : "NEGATIVE"
}


"""
Quick regex review:
\b word boundaries - \b matches before and after an alphanumeric sequence
\s white space characters
\d digit
| logical or
+ one or more
* zero or more
() capture group
[] any of these
? lazy quantifier (match minimum possible)

re.fullmatch to match the whole string
"""


def pad(check):
    return r'\s*' + check + r'\s*'

def group(check):
    return r'(' + check + r')'

def named_group(name, check):
    n_group = group(r'?P<' + name + r'>' + check)
    return n_group

def optional(check):
    return check + r'?'

def empty(*args):
    result = []
    for arg in args:
        temp = arg
        if not arg:
            temp = ""
        result.append(temp)
    return result

def or_group(checks):
    final_check = r'|'.join(checks)
    
    return group(final_check)

def clean_numeric(value):
    if value:
        # remove commas from comma - separated numbers
        if re.match(r'\d{1,3}(,\d{3})*(\.\d+)?', value):
            value = re.sub(r',', r'', value)

        # add a zero to the front of decimals
        if re.match(r'^\.\d+', value):
            value = '0' + value

        # strip zeros from the end of a decimal
        value = re.sub(r'^(\d*\.\d*?)0*$', r'\1', value)

        # remove decimal from end of non-decimal
        value = re.sub(r'^(\d+)\.$', r'\1', value)

        # remove space around slashes
        value = re.sub(r'\s*\/\s*', r'/', value)

    return value

def parse_value(result: str):
    operator = ''
    numeric = ''
    alpha = ''
    passthru = ''
    # trim whitespace from beginning and end
    result = result.strip()

    # cleanup all weird ><= variants
    # match > or <
    # followed by OR or / or whitespace
    # followed by an =
    # >OR= goes to >=
    # </= goes to <=
    result = re.sub(r'([><])(\s*OR\s*|\/|\s+)=', r'\1=', result, count=0, flags=re.IGNORECASE)

    # replace all variants of spacing and LESS THAN with <
    result = re.sub(r'\bLESS\s*THAN\b', r'<', result)

     # replace all variants of spacing and MORE THAN with >
    result = re.sub(r'\bMORE\s*THAN\b', r'>', result)

    decimals = r'\d*\.?\d+'
    space_decimals= decimals + r'\s*'
    start_decimals = r'^' + space_decimals
    measurement = start_decimals + r'(ml|mg)\b'

    other = start_decimals + r'x\s*' + space_decimals + r'x\s*' + decimals + r'\b'


    passthru_regexes = [
        re.search(r'^[><]=?.+[><]', result), # multiple gt,lt signs
        re.search(r'\d\d?\/\d\d?\/\d\d', result), # a date
        re.search(measurement, result, flags=re.IGNORECASE), # a measurement of thing
        re.search(other, result, flags=re.IGNORECASE)
    ]

    for search in passthru_regexes:
        if search:
            passthru = result
            return operator, numeric, alpha, passthru

    if re.search(r'^TNP(\/.+)?$', result):
        alpha = 'TEST NOT PERFORMED'
        return operator, numeric, alpha, passthru

    numeric_ish = r'[\d\.:,]+'

    numeric_checks = [
        r'\d+\s*in\s*\d+',                      # digits in digits
        numeric_ish + pad(r'\/') + numeric_ish, # numeric over numeric
        r'-?' + numeric_ish                     # negative numeric
    ]

    operator_check = r'[><]=?'
    alpha_check = r'[^\d]+.*'

    full_check = optional(group(operator_check)) + pad(or_group(numeric_checks)) + optional(group(alpha_check))
    match = re.fullmatch(full_check, result, flags=re.IGNORECASE)
    
    # consume the whole string
    if match:
        operator, numeric, alpha = match.groups()

    # clean up results
    numeric = clean_numeric(numeric)

    if alpha:
        alpha = re.sub(r'(^\(|\)$)', r'', alpha)
        alpha = re.sub(r'^~', r'', alpha)

    # convert `None`s to empty strings
    operator, numeric, alpha, passthru = empty(operator, numeric, alpha, passthru)

    for key in replacements.keys():
        if result.lower() == key.lower():
            alpha = result
        alpha = re.sub(r'(\b)' + re.escape(key) + r'(\b)', r'\1' + replacements[key] + r'\2', alpha, count=0, flags=re.IGNORECASE)

    if not (operator or numeric or alpha): passthru = result

    return operator, numeric, alpha, passthru
