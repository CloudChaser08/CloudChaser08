"""singleton"""
class Singleton(type):
    """This class serves as a metaclass which enforces a Singleton behavior to
    whichever class extends it.
    """
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = \
                    super(Singleton, cls).__call__(*args, **kwargs)

        return cls._instances[cls]
