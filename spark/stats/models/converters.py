"""
    Converters for use in models
"""
def model_converter(model_cls, optional=True):
    """
        Generates a converter that casts the field value to an instance of
        model_cls class.

        Note that if the value is already an instance of the model_cls, no
        conversion will occur.

        If optional is set to True, then None values will also be allowed
    """
    def _converter(val):
        if isinstance(val, model_cls) or (optional and val is None):
            return val
        if isinstance(val, dict):
            return model_cls(**val)
        raise ValueError(
            '{} is neither an instance of {} or a dict'.format(val, model_cls)
        )
    return _converter


def model_list_converter(model_cls):
    """
        Generates a converter that casts a list of values to a list of
        model_cls instances.

        Note that if the field value is None, it will be cast to an empty list
    """
    _model_converter = model_converter(model_cls)
    def _converter(val):
        return convert_model_list(_model_converter, val)
    return _converter


def model_map_converter(model):
    """
        Generates a converter that casts a list of values to a mapping of
        {<key>: <model_cls instance>} value pairs.

        Note that if the field value is None, it will be cast to an empty dict
    """
    _model_converter = model_converter(model)
    def _converter(val):
        if not val:
            return {}
        if isinstance(val, dict):
            return {
                key: _model_converter(item) for key, item in val.items()
            }
        raise ValueError('Expecting a mapping')
    return _converter


def convert_model_list(converter, val):
    """ Runs the model list conversion """
    if not val:
        return []
    if isinstance(val, list):
        return [converter(item) for item in val]
    raise ValueError('Expecting a list of values')
