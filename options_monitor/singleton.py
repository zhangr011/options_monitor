# encoding: UTF-8

class Singleton(type):
    """singleton for class:
    how to use:
    - python2
    class MyClass(object):
        __metaclass__ = Singleton

    - python3
    class MyClass(metaclass = Singleton):
        pass
    """
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]
