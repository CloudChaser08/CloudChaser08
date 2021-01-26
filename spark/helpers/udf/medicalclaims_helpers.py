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
