r"""
Parse Value UDF
---------------

Operations to convert given Test Results STRING column to its relevant parts.
    - Operator: ex >, <, >= etc
    - Number: ex 1, 22.4, 1.23e23, 1 in 10000, 3:5
    - Alpha: ex Units, mg, ml
    - Passthrough: ex. "Ignore this result"

Steps:
    1. Basic whitespace cleaning
    2. Save cleaned original value for returning in Passthrough
    3. Cleanup and normalize natural language Operators
    4. Detect exempt 'passthrough' patterns and return if Any
    5. Generic Results substitutions
    6. Declare Numeric Patterns
    7. Scientific Notation Handler
    8. Main Matching Step
    9. Reverse Matching Step
    10. Process match groups
        a. Clean numerics
        b. Return as passthrough for certain numerics (ex 1 in 100 *no operator)
        c. Clean alphas
        d. Coalesce nulls as empty strings
    11. Specific string substitutions
    12. Failsafe return original string if no elements
    13. Return parsed elements

Quick regex review:
\b word boundaries - \b matches before and after an alphanumeric sequence
\s white space characters
\d digit
| logical or
+ one or more
* zero or more
() capture group
(?:foo) non-capture grouping
(?!foo) negative lookahead  See: https://www.regular-expressions.info/lookaround.html
(?=foo) positive lookahead
[] any of these
? lazy quantifier (match minimum possible chars)

re.fullmatch to match the whole string

Regex reference:
https://www.regular-expressions.info/
https://regex101.com/
"""
import re
import pyspark.sql.functions as F
import pyspark.sql.types as T

REPLACEMENT_LOOKUP = {
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


# Specialty patterns
GENETIC_PATTERN = r'[ACGT]>[ACGT]\s*[ACGT]/[ACGT]'
SCIENTIFIC_PATTERN = r'(?:\d+(?:\.\d{1,10})?[eE]-?\d{1,3})'  # ex 1e10, 1.00023-E5
DIGIT_IN_DIGIT = r'\d+\s*in\s*\d+'  # ex. 1 in 300, 1 IN 10000
NUMBERLIKE = r'[\d\.:,]+'
OPERATOR = r'(?:[><]=?|=)'
ALPHA = r'[^\d]+.*'
CFU_pattern = r'\d+\s?-\s?\d+ CFU/mL'
DECIMALS = r'\d*\.?\d+'
SPACE_DECIMALS = DECIMALS + r'\s*'
START_DECIMALS = r'^' + SPACE_DECIMALS
MEASUREMENTS = START_DECIMALS + r'(ml|mg)\b'
OTHER_DECIMALS = START_DECIMALS + r'x\s*' + SPACE_DECIMALS + r'x\s*' + DECIMALS + r'\b'


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
    value = value.strip()

    if value:
        if re.search(r'\d+/\d+/\d+/\d+/\d+/\d+', value):
            return value

        if re.search(r'\d+/\d+/\d+', value):
            return value

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
def parse_value(result):
    """
    Convert given Test Results STRING column to its relevant parts.

    :param result: Test result text field
    :type result: str
    :return: Parsed result items (operator, numeric, alpha, passthrough)
    :rtype: (str, str, str, str)
    """
    operator = ''
    numeric = ''
    alpha = ''
    passthru = ''

    # Only proceed if NOT NULL
    if result:

        # trim whitespace from beginning and end
        # Standardize whitespace to one [SPACE] char.
        result = re.sub(r'\s+', ' ', result).strip()

        # Save cleaned original for later passthru
        # Don't want to passthru modified version
        original = result

####################################################################################################
# Operator Cleanup and Normalizations
# Converts Word comparators (MORE THAN, LESS THAN etc) to symbols (>, <)
####################################################################################################
        # cleanup all weird ><= variants
        # match > or <
        # followed by OR or / or whitespace
        # followed by an =
        # >OR= goes to >=
        # </= goes to <=
        result = re.sub(r'([><])(\s*OR\s*|\/|\s+)=', r'\1=', result, count=0, flags=re.IGNORECASE)

        # replace all variants of spacing and LESS THAN with <
        result = re.sub(r'\bLESS\s*THAN\b', r'<', result, flags=re.IGNORECASE)

        # replace all variants of spacing and MORE THAN with >
        result = re.sub(r'\b(MORE|GREATER)\s*THAN\b', r'>', result, flags=re.IGNORECASE)


####################################################################################################
# Pass Through Matching Patterns
# Anything that matches one of these patterns will be returned with only minimal whitespace changes
####################################################################################################
        passthru_regexes = [
            re.search(r'^[><]=?.+[><]', result),  # multiple gt,lt signs
            re.search(r'\d{1,2}([-/])\d{1,2}\1\d{2}(?!\sunits)',
                      result, flags=re.IGNORECASE),     # test_gt_slash_numeric_units
            re.fullmatch(r'^\d{1,2}([-/])\d{1,2}\1\d{2}(\d{2})?\sNEG(ative)?$',
                         result, flags=re.IGNORECASE),  # Test test_date_neg_M_D_YY
            re.fullmatch(r'^\d{1,2}([-/])\d{4}\W+NEG(ative)?$',
                         result, flags=re.IGNORECASE),  # Test test_date_neg_M_slash_YYYY
            re.fullmatch(r'^\d{1,2}([-/])\d{1,2}\W+'
                         r'\d{1,2}([-/])\d{1,2}\sNEG(ative)?$',
                         result, flags=re.IGNORECASE),  # Test test_double_date_neg_M_slash_YYYY
            re.search(r'0%?\s*(?:positive|negative).+0%?\s*(?:positive|negative)', result,
                      flags=re.IGNORECASE),  # references ranges
            re.search(r'negative <=?\d+:\d+', result, flags=re.IGNORECASE),
            re.search(MEASUREMENTS, result, flags=re.IGNORECASE),  # a measurement of thing
            re.search(OTHER_DECIMALS, result, flags=re.IGNORECASE),
            # Genetic markers regex.
            re.search(GENETIC_PATTERN, result),
            re.fullmatch(r'91935 >2; Verify Patient Age', result),  # Test 929
            re.fullmatch(r'19955 Patient <4', result),  # Test 933
            re.fullmatch(r'36336 Patient <6', result),  # Test 934
            re.fullmatch(r'PULLED NOT ENOUGH (LESS THAN|<) .1ML', result),  # Test 940
            re.fullmatch(r'91935 >2; Verify Patient Age', result),
            re.fullmatch(r'\d?\.\d+\s?-\s?\d?\.\d+\s?equivocal', result, flags=re.IGNORECASE),
            re.search(CFU_pattern, result, re.IGNORECASE)
        ]

        if any(passthru_regexes):
            return operator, numeric, alpha, original

####################################################################################################
# Generic result substitutions
#   - TEST NOT PERFORMED
#   - DO NOT REPORT
#   - CULTURE INDICATED
#   - INDICATED
# These will match generic patterns and immediately return the value in ALPHA
####################################################################################################
        # TNP or #/TNP
        if re.search(r'^(\d+/)?TNP(/.+)?$', result, flags=re.IGNORECASE):
            alpha = 'TEST NOT PERFORMED'
            return operator, numeric, alpha, passthru

        # DNR or #/DNR
        if re.search(r'^(\d+/)?DNR$', result, flags=re.IGNORECASE):
            alpha = 'DO NOT REPORT'
            return operator, numeric, alpha, passthru

        # Culture Indicated
        if re.search(r'^\d+\s?[/-]\s?culture indicated$', result, flags=re.IGNORECASE):
            alpha = 'CULTURE INDICATED'
            return operator, numeric, alpha, passthru

        # Indicated
        if re.search(r'^\d+\s?[/-]\s?indicated$', result, flags=re.IGNORECASE):
            alpha = 'INDICATED'
            return operator, numeric, alpha, passthru

####################################################################################################
# Numeric Pattern Declarations
# These will be used to find any of these forms in the main matching step
####################################################################################################
        numerics = [
            r'(?:\d+/\d+/\d+\s)(?=units)',   # unit descriptor test_gt_slash_numeric_units
            r'(?:\d+/\d+/\d+/\d+/\d+/\d+)',
            DIGIT_IN_DIGIT,  # digits in digits
            NUMBERLIKE + pad(r'/') + NUMBERLIKE,  # numeric over numeric
            r'-?' + NUMBERLIKE  # negative numeric,
        ]

####################################################################################################
# Scientific Notation
# Detects and processes numbers in scientific notation.
####################################################################################################
        # Evaluate scientific notations.
        if re.search(SCIENTIFIC_PATTERN, result):
            match_result = re.search(r'(\d+(?:\.\d{1,10})?)([eE])(-?\d{1,3})', result)
            prefix, lit_e, exponent = match_result.groups()
            converted = float(prefix + lit_e + exponent)
            result = re.sub(match_result.group(0), str(converted).rstrip('0').rstrip('.'), result)

        # Clean parentheses
        result_no_parens = re.sub(r'[()]', r'', result)

####################################################################################################
# Main Matching Step
# Looks for
#   - Operators + Numerics + Alpha
#   - Numerics + Alpha
#   - Operators + Numerics
#   - Numerics (ONLY)
####################################################################################################
        # operator? + numerics + alpha?
        full_check = optional(group(OPERATOR)) \
            + pad(or_group(numerics)) \
            + optional(group(ALPHA))
        match = re.fullmatch(full_check, result_no_parens, flags=re.IGNORECASE)

####################################################################################################
# Reverse Matching Step
# Main matching failed so this will check if there are only two parts `.split(' ')) == 2`
# If so, it splits and swaps elements before retrying.
# ex. `units 25' -> '25 units'
####################################################################################################
        # If no match detected Alpha may be before operator and numeric,
        # this tries again after flipping the groups
        if not match and len(result_no_parens.split(' ')) == 2:
            _result_list = result_no_parens.split(' ')
            reverse_match = ' '.join(_result_list[1:] + _result_list[:1])
            rematch = re.fullmatch(full_check, reverse_match, flags=re.IGNORECASE)
            if rematch:
                match = rematch

####################################################################################################
# Process match groups
####################################################################################################
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

####################################################################################################
# Perform Specific String Substitutions
# These are predetermined patterns we want to substitute.
####################################################################################################
        for key in REPLACEMENT_LOOKUP.keys():
            if result_no_parens.lower() == key.lower():
                alpha = result_no_parens
            alpha = re.sub(r'(\b)' + re.escape(key) + r'(\b)', r'\1' + REPLACEMENT_LOOKUP[key] + r'\2',
                           alpha, count=0,
                           flags=re.IGNORECASE)

####################################################################################################
# Failsafe return original
####################################################################################################
        if not (operator or numeric or alpha):
            passthru = original

    return operator, numeric, alpha, passthru
