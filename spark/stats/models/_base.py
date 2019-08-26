"""
    Stats base model
"""


import attr

class BaseModel(object):
    """ Base model for stats """

    def copy_with(self, **kwargs):
        """ Copies the model instance, with additional args """
        return attr.evolve(self, **kwargs)

    def to_dict(self):
        """ Converts the model to a dictionary """
        return attr.asdict(self)
