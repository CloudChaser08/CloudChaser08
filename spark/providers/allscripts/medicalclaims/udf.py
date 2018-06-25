# diags - array of diagnoses in order (diag_1, diag_2, ... diag_8)
# pointers - array of diagnosis pointers in order (ptr_1, ptr_2 .. ptr_4)
def linked_and_unlinked_diagnoses(diags, pointers):
    diag_priority = {}
    for i, p in enumerate(pointers):
        if p is not None and p.isdigit() and int(p) <= len(diags) \
                and diags[int(p) - 1] is not None \
                and diags[int(p) - 1] not in diag_priority:
            diag_priority[diags[int(p) - 1]] = str(i + 1) # priority is the pointer number (index + 1)

    res = []
    # we need to create at least one row per service line
    if len(diag_priority) == 0:
        res = [(None, None)]

    for d in diags:
        if d is not None and d not in diag_priority:
            diag_priority[d] = None # diagnoses without priority

    return res + diag_priority.items()
