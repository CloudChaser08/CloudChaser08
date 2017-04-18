#! /usr/bin/python
#Create a function for generating an serialized string of diagnosis-priority pairs
def get_diagnosis_with_priority(diags, pointers):
    if diags is None or pointers is None:
        return None
    import re
    ds = diags.split(':')
    ps = pointers.upper().split(':')
    ps = filter(lambda x: len(x) > 0, ps)
    if pointers.upper().find('A') > -1:
        ps = filter(lambda x: x > 0 and x < 27, map(lambda x: ord(x[0]) - 64, ps))
    else:
        ps = map(int, filter(lambda x: re.search('[^\s\d]', x) is None and re.search('[^\s]',x) is not None, ps))
    if 0 in ps:
        ps = map(lambda x: x+1, ps)
    
    ps = filter(lambda x: x > 0 and x <= len(ds), ps)
    return ':'.join([ds[p-1] + '_' + str(i+1) for i, p in enumerate(ps)])
