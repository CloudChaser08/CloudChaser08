"""
udf.py
"""
# diags - array of diagnoses in order (diag_1, diag_2, ... diag_8)
# pointers - array of diagnosis pointers in order (ptr_1, ptr_2 .. ptr_4)


def linked_and_unlinked_diagnoses(diags, pointers):
    """Using a list of diagnoses and diagnosis_pointers from a claim/service line,
    identify all diagnoses linked to the procedure and their priority, as well as
    diagnoses unlinked to the procedure that are still part of the claim.
    Returns an array of (diagnosis, priority) pairs with at least one pair for
    diagnoses linked to the procedure"""

    diag_priority = {}
    # Identify any diagnoses linked to the procedure using the priority pointers
    # If two pointers point to the same diagnosis, the higher priority one is used
    for i, p in enumerate(pointers):
        if p is not None and p.isdigit() and int(p) <= len(diags) \
                and diags[int(p) - 1] is not None \
                and diags[int(p) - 1] not in diag_priority:
            diag_priority[diags[int(p) - 1]] = str(i + 1)
            # priority is the pointer number (index + 1)

    res = []
    # We need at least one pair in order to make sure we don't lose the procedure
    # code from this service line
    if len(diag_priority) == 0:
        res = [(None, None)]

    # Identify any unique diagnosis not linked to the procedure. Make sure they
    # are not duplicates of a linked diagnosis 
    for d in diags:
        if d is not None and d not in diag_priority:
            diag_priority[d] = None  # diagnoses without priority

    return res + list(diag_priority.items())
