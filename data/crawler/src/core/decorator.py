from functools import wraps
from threading import Lock

def singleton(cls):
    orig_new = cls.__new__
    lock = Lock()
    instance = None
    
    @wraps(orig_new)
    def __new__(cls, *args, **kwargs):
        nonlocal instance
        with lock:
            if instance is None:
                instance = orig_new(cls, *args, **kwargs)
            return instance
        
    cls.__new__ = __new__
    return cls