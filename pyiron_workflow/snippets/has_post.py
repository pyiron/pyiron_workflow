from abc import ABCMeta


class HasPost(type):
    """
    A metaclass for adding a `__post__` method which has a compatible signature with
    `__init__` (and indeed receives all its input), but is guaranteed to be called
    only _after_ `__init__` is totally finished.

    Based on @jsbueno's reply in [this discussion](https://discuss.python.org/t/add-a-post-method-equivalent-to-the-new-method-but-called-after-init/5449/11)
    """

    def __call__(cls, *args, **kwargs):
        instance = super().__call__(*args, **kwargs)
        if post := getattr(cls, "__post__", False):
            post(instance, *args, **kwargs)
        return instance


class AbstractHasPost(HasPost, ABCMeta):
    # Just for resolving metaclass conflic for ABC classes that have post
    pass
