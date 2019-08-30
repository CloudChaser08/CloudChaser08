"""
    Common fields for models
"""

from functools import partial
from six import string_types

import attr

from .converters import (
    model_converter,
    model_list_converter,
    model_map_converter
)
from .validators import optional_instance_of, is_bool, is_list_of_strs

def create_model_field(model_cls, optional=True):
    """
        Creates a model field that converts values to an instance of the model
    """
    converter = model_converter(model_cls, optional=optional)
    make_field = partial(attr.ib, converter=converter)
    if optional:
        return make_field(default=None)
    return make_field()


def create_model_list_field(model_cls, optional=False):
    """
        Creates a model list field that converts values in the list to an
        instance of the model
    """
    return attr.ib(
        converter=model_list_converter(model_cls),
        factory=list if optional else None
    )


def create_model_map_field(model_cls, optional=False):
    """
        Creates a model list field that converts values in the map to an
        instance of the model
    """
    return attr.ib(
        converter=model_map_converter(model_cls),
        factory=dict if optional else None
    )


def create_required_str_field():
    """ Creates a field that requires a string value """
    return attr.ib(
        validator=attr.validators.instance_of(string_types)
    )

def create_optional_str_field():
    """ Creates a field that requires a string value, but defaults to None """
    return attr.ib(
        validator=optional_instance_of(string_types),
        default=None
    )

def create_optional_bool_field():
    """ Creates a field that requires a bool value, but defaults to False """
    return attr.ib(
        validator=is_bool,
        default=False
    )


def create_str_list_field():
    """ Creates a field that expects a list of strings """
    return attr.ib(
        validator=is_list_of_strs,
        default=attr.Factory(list)
    )
