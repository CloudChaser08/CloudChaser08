import re


def clean_neogenomics_diag_list(diag_list):
    return re.sub(
        '[ ,]*,[ ,]*', ',',
        re.sub('(^,+|,+$)', '', diag_list.strip())
    )
