"""
udf.py
"""
import re


def clean_neogenomics_diag_list(diag_list):
    if diag_list is None:
        return None
    else:
        return re.sub(
            '[ ,]*,[ ,]*', ',',
            re.sub('(^,+|,+$)', '', diag_list.strip())
        )
