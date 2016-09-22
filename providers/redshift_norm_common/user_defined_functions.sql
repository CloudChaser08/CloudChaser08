/*
 * Check if a date is valid
 *
 * Expects a text object in the form 'YYYYMMDD'
 * Will return false if that text object exceeds length 8, 
 * or if it refers to an impossible date
 */
CREATE OR REPLACE FUNCTION is_date_valid(d varchar(8)) RETURNS boolean IMMUTABLE
AS $$
    if d is None:
        return False

    import datetime
    now = datetime.datetime.now()
    curYear = now.year
    curMonth = now.month
    curDay = now.day

    MONTH_DAY = {
        1: 31,
        2: 29,
        3: 31,
        4: 30,
        5: 31,
        6: 30,
        7: 31,
        8: 31,
        9: 30,
        10: 31,
        11: 30,
        12: 31
    }

    try:
        inputYear = int(d[0:4])
        inputMonth = int(d[4:6])
        inputDay = int(d[6:])
    except ValueError:
        return False

    if (curYear < inputYear
        or (curYear == inputYear and curMonth < inputMonth)
        or (curYear == inputYear and curMonth == inputMonth and curDay <= inputDay)
        or inputMonth not in MONTH_DAY.keys()
        or MONTH_DAY[inputMonth] < inputDay):
        return False
    else:
        return True

$$ LANGUAGE plpythonu;
