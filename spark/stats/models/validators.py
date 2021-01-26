"""
    Validators
"""

from datetime import datetime
from six import string_types

import attr


def is_date_str(_, __, value):
    """ Validates that the value is a string in the YYYY-MM-DD format """
    datetime.strptime(value, '%Y-%M-%d')


def is_bool(*args, **kwargs):
    """ Validates that the value is a boolean """
    return attr.validators.instance_of(bool)(*args, **kwargs)


def optional_instance_of(*types):
    """
        Generates a validator that ensures that if the value is not None,
        it must be an instance of one of the provided types
    """
    def _validator(_, __, value):
        if value is not None and not isinstance(value, types):
            raise ValueError('{} is not one of types {}'.format(value, types))
    return _validator


def is_list_of_strs(_, __, value):
    """ Validates that the value is an instance of a string """
    if not isinstance(value, list):
        raise ValueError('{} is not a list'.format(value))
    for item in value:
        if not isinstance(item, string_types):
            raise ValueError('{} is not a string'.format(item))
