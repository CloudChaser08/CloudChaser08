-- Create a function for generating an serialized string of diagnosis-priority pairs
CREATE OR REPLACE FUNCTION get_diagnosis_with_priority(diags varchar(5000), priorities varchar(5000)) RETURNS varchar(5000) IMMUTABLE
AS $$
    if diags is None or priorities is None:
        return None
    ds = diags.split(':')
    ds = filter(lambda x: len(x) > 0, ds)
    ps = priorities.split(':')
    for i, p in enumerate(ps):
        if len(p) > 0:
            try:
                ps[i] = int(p)
            except:
                ps[i] = 0
        else:
            ps[i] = 0
    ps = filter(lambda x: x > 0, ps)
    res = []
    for i, p in enumerate(ps):
        if p <= len(ds):
            res.append(ds[p-1] + '_' + str(i+1))
            ds[p-1] = ''
    res.extend(filter(lambda x: x != '', ds))

    return ':'.join(res)
$$ LANGUAGE plpythonu;

CREATE OR REPLACE FUNCTION string_set_diff(s1 varchar(5000), s2 varchar(5000)) RETURNS varchar(5000) IMMUTABLE
AS $$
    if s1 is None:
        return None
    if s2 is None:
        s2 = ''

    s1s = map(lambda x : x.split('_')[0], filter(lambda x: x is not None and len(x) > 0, s1.split(':')))
    s2s = map(lambda x : x.split('_')[0], filter(lambda x: x is not None and len(x) > 0, s2.split(':')))

    return ':'.join(set(s1s).difference(set(s2s)))
$$ LANGUAGE plpythonu;

CREATE OR REPLACE FUNCTION uniquify(with_dupes text) RETURNS text IMMUTABLE
AS $$
    if with_dupes is None:
        return None;
    return ':'.join(set(filter(lambda x: x is not None and len(x) > 0, with_dupes.split(':'))))
$$ LANGUAGE plpythonu;

