"""
remove_last_chars
"""


def remove_last_chars(value, number_of_characters):
    if not value or number_of_characters >= len(value):
        return None
    else:
        return value[:(number_of_characters * -1)]
