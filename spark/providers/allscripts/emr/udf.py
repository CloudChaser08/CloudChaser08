def remove_last_chars(value, number_of_characters):
    if not value or number_of_characters >= len(value):
        return None
    else:
        value[:(number_of_characters * -1)]
