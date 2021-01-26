#! /usr/bin/python
# Create a function for generating an serialized string of diagnosis-priority pairs


def get_diagnosis_with_priority(diags, pointers):
    if diags is None or pointers is None:
        return None
    import re
    ds = diags.split(':')
    ps = pointers.upper().split(':')
    ps = [x for x in ps if len(x) > 0]
    if pointers.upper().find('A') > -1:
        ps = [x for x in [ord(x[0]) - 64 for x in ps] if 0 < x < 27]
    else:
        ps = [int(x) for x in ps if re.search(r'[^\s\d]', x) is None and re.search(r'[^\s]', x) is not None]
    if 0 in ps:
        ps = [x+1 for x in ps]
    
    ps = [x for x in ps if 0 < x <= len(ds)]
    return ':'.join([ds[p-1] + '_' + str(i+1) for i, p in enumerate(ps)])


def string_set_diff(s1, s2):
    if s1 is None:
        return None
    if s2 is None:
        s2 = ''

    s1s = [x.split('_')[0] for x in [x for x in s1.split(':') if x is not None and len(x) > 0]]
    s2s = [x.split('_')[0] for x in [x for x in s2.split(':') if x is not None and len(x) > 0]]

    return ':'.join(set(s1s).difference(set(s2s)))


def uniquify(with_dupes):
    if with_dupes is None:
        return None
    return ':'.join(set([x for x in with_dupes.split(':') if x is not None and len(x) > 0]))

