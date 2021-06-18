import re
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql import Column
replacements = {
    "NEG": "NEGATIVE",
    "FINAL RSLT: NEGATIVE": "NEGATIVE",
    "NEGATIVE": "NEGATIVE",
    "NEGATIVE CONFIRMED": "NEGATIVE CONFIRMED",
    "NEGA CONF": "NEGATIVE CONFIRMED",
    "POS": "POSITIVE",
    "POSITIVE": "POSITIVE",
    "None Detected": "NOT DETECTED",
    "Not Detected": "NOT DETECTED",
    "NON-REACTIVE": "NON-REACTIVE",
    "Nonreactive": "NON-REACTIVE",
    "Not Indicated": "NOT INDICATED",
    "CONSISTENT": "CONSISTENT",
    "INCONSISTENT": "INCONSISTENT",
    "DNRTNP": "DO NOT REPORT",
    "DNR": "DO NOT REPORT",
    "NOT INTERPRETED~DNR": "DO NOT REPORT",
    "TNP124": "TEST NOT PERFORMED",
    "DETECTED": "DETECTED",
    "REACTIVE": "REACTIVE",
    "EQUIVOCAL": "EQUIVOCAL",
    "INDETERMINATE": "INDETERMINATE",
    "INCONCLUSIVE": "INCONCLUSIVE",
    "NOT ISOLATED": "NOT ISOLATED",
    "ISOLATED": "ISOLATED",
    "NO CULTURE INDICATED": "NO CULTURE INDICATED",
    "CULTURE INDICATED": "CULTURE INDICATED",
    "INDICATED": "INDICATED",
    "EQUIOC": "EQUIVOCAL",
    "INDETERMINANT": "INDETERMINATE",
    "NEG/": "NEGATIVE",
    "POS/": "POSITIVE",
    "DETECTED ABN": "DETECTED",
    "DETECTED (A)": "DETECTED",
    "NON-DETECTED": "NOT DETECTED",
    "NO VARIANT DETECTED": "NO VARIANT DETECTED",
    "^TNP124": "TEST NOT PERFORMED",
    "ADD^TNP167": "TEST NOT PERFORMED",
    "DTEL^TNP1003": "TEST NOT PERFORMED",
    "TNP QUANTITY NOT SUFFICIENT": "TEST NOT PERFORMED",
    "TNP124^CANC": "TEST NOT PERFORMED",
    "TA/DNR": "DO NOT REPORT",
    "DETECTED (A1)": "DETECTED",
    "NEGATI": "NEGATIVE"
}

r"""
Quick regex review:
\b word boundaries - \b matches before and after an alphanumeric sequence
\s white space characters
\d digit
| logical or
+ one or more
* zero or more
() capture group
(?:) non-capture grouping
[] any of these
? lazy quantifier (match minimum possible)

re.fullmatch to match the whole string
"""

GENETIC_PATTERN = r'[ACGT]>[ACGT]\s*[ACGT]/[ACGT]'
SCIENTIFIC_PATTERN = r'(?:\d+(?:\.\d{1,10})?[eE]-?\d{1,3})'  # ex 1e10, 1.00023-E5
DIGIT_IN_DIGIT = r'\d+\s*in\s*\d+'  # ex. 1 in 300, 1 IN 10000


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
        if re.search(r'\d[^/]*/[^/]*\d', value):
            numerator, denominator = value.split('/')
            return '/'.join([clean_numeric(numerator).strip(), clean_numeric(denominator).strip()])

        # remove commas from comma - separated numbers
        if re.match(r'\d{1,3}(,\d{3})*(\.\d+)?', value):
            value = re.sub(r',', r'', value)

        # https://healthverity.atlassian.net/browse/DE-176?focusedCommentId=18554
        # We are no longer dropping leading/trailing zeroes
        # # drop a leading zero from integer values
        # value = re.sub(r'^(-)?0+(\d+(?!\.)[0-9])', r'\1\2', value)

        # add a zero to the front of decimals
        if re.match(r'^\.\d+', value):
            value = '0' + value

        # https://healthverity.atlassian.net/browse/DE-176?focusedCommentId=18554
        # We are no longer dropping leading/trailing zeroes
        # # strip zeros from the end of a decimal
        # value = re.sub(r'^(\d*\.\d*?)0*$', r'\1', value)

        # remove decimal from end of non-decimal
        value = re.sub(r'^(\d+)\.$', r'\1', value)

        # remove space around slashes
        value = re.sub(r'\s*\/\s*', r'/', value)

    return value


@F.udf(T.ArrayType(T.StringType()))
def parse_value(result: Column):
    operator = ''
    numeric = ''
    alpha = ''
    passthru = ''

    # Only proceed if NOT NULL
    if result:
        # trim whitespace from beginning and end
        # Save trimmed original for later passthru
        result = result.strip()

        # Don't want to passthru modified version
        original = result

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
        space_decimals = decimals + r'\s*'
        start_decimals = r'^' + space_decimals
        measurement = start_decimals + r'(ml|mg)\b'

        other = start_decimals + r'x\s*' + space_decimals + r'x\s*' + decimals + r'\b'

        passthru_regexes = [
            re.search(r'^[><]=?.+[><]', result),  # multiple gt,lt signs
            re.search(r'\d\d?\/\d\d?\/\d\d', result),  # a date
            re.search(r'0%?\s*(?:positive|negative).+0%?\s*(?:positive|negative)', result,
                      flags=re.IGNORECASE),  # references ranges
            re.search(r'negative <=?\d+:\d+', result, flags=re.IGNORECASE),
            re.search(measurement, result, flags=re.IGNORECASE),  # a measurement of thing
            re.search(other, result, flags=re.IGNORECASE),
            # Genetic markers regex.
            re.search(GENETIC_PATTERN, result),
            re.fullmatch(r'91935 >2; Verify Patient Age', original),      # Test 929
            re.fullmatch(r'19955 Patient <4', original),                  # Test 933
            re.fullmatch(r'36336 Patient <6', original),                  # Test 934
            re.fullmatch(r'PULLED NOT ENOUGH LESS THAN .1ML', original)   # Test 940
        ]

        if any(passthru_regexes):
            return operator, numeric, alpha, original

        if re.search(r'^TNP(\/.+)?$', result):
            alpha = 'TEST NOT PERFORMED'
            return operator, numeric, alpha, passthru

        numeric_ish = r'[\d\.:,]+'

        numeric_checks = [
            DIGIT_IN_DIGIT,  # digits in digits
            numeric_ish + pad(r'\/') + numeric_ish,  # numeric over numeric
            r'-?' + numeric_ish  # negative numeric,

        ]

        # Evaluate scientific notations.
        if re.search(SCIENTIFIC_PATTERN, result):
            match_result = re.search(r'(\d+(?:\.\d{1,10})?)([eE])(-?\d{1,3})', result)
            prefix, lit_e, exponent = match_result.groups()
            converted = float(prefix + lit_e + exponent)
            result = re.sub(match_result.group(0), str(converted).rstrip('0').rstrip('.'), result)

        operator_check = r'[><]=?'
        alpha_check = r'[^\d]+.*'

        result_no_parens = re.sub(r'[()]', r'', result)

        full_check = optional(group(operator_check)) + pad(or_group(numeric_checks)) + optional(group(alpha_check))
        match = re.fullmatch(full_check, result_no_parens, flags=re.IGNORECASE)

        # If no match detected Alpha may be before operator and numeric, this tries again after flipping
        if not match and len(result_no_parens.split(' ')) == 2:
            _result_list = result_no_parens.split(' ')
            reverse_match = ' '.join(_result_list[1:] + _result_list[:1])
            rematch = re.fullmatch(full_check, reverse_match, flags=re.IGNORECASE)
            if rematch:
                match = rematch

        # consume the whole string
        if match:
            operator, numeric, alpha = match.groups()

        # clean up results
        numeric = clean_numeric(numeric)

        # With DIGIT_IN_DIGIT alphas passthru when there is no operator
        if re.search(DIGIT_IN_DIGIT, numeric, flags=re.IGNORECASE) and not operator:
            return '', '', '', result_no_parens

        if alpha:
            alpha = re.sub(r'(^\(|\)$)', r'', alpha)
            alpha = re.sub(r'^~', r'', alpha)
            alpha = re.sub(r',?\s*not quantified', r'', alpha, flags=re.IGNORECASE)
            alpha = alpha.strip()

        # convert `None`s to empty strings
        operator, numeric, alpha, passthru = empty(operator, numeric, alpha, passthru)

        for key in replacements.keys():
            if result_no_parens.lower() == key.lower():
                alpha = result_no_parens
            alpha = re.sub(r'(\b)' + re.escape(key) + r'(\b)', r'\1' + replacements[key] + r'\2', alpha, count=0,
                           flags=re.IGNORECASE)

        if not (operator or numeric or alpha):
            passthru = original

    return operator, numeric, alpha, passthru
