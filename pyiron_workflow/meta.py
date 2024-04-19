"""
Meta nodes are callables that create a node class instead of a node instance.
"""

from __future__ import annotations

from abc import abstractmethod
import hashlib
import re
from typing import Any, Optional

from pyiron_workflow.io_preview import StaticNode


class Meta(StaticNode):
    """
    A parent class for dynamically defined nodes who are not available for import, but
    rather require a custom `__reduce__` method that leverages the _importable_
    function that constructed them.
    """

    @property
    @abstractmethod
    def _instance_constructor(self) -> callable[[...], Meta]:
        pass

    @property
    @abstractmethod
    def _instance_constructor_args(self) -> tuple:
        pass

    def to_dict(self):
        pass  # Vestigial abstract method

    def __reduce__(self):
        return (
            self._instance_constructor,
            (
                self.label,
                self.__class__.__name__,
                *self._instance_constructor_args,
            ),
            self.__getstate__()
        )


def meta_node_class_factory(
    base_class: type[Meta],
    class_attributes: dict[str, Any],
    class_name: Optional[str] = None
):
    if class_name is None:
        suffix = "".join([f"{k}{str(v)}" for k, v in class_attributes.items()])
        class_name = f"{base_class.__name__}{_sanitize_string_for_class_name(suffix)}"

    class_attributes["__module__"] = base_class.__module__

    return type(
        class_name,
        (base_class,),
        class_attributes,
    )


def meta_node_instance_factory(
    base_class: type[Meta],
    class_attributes: dict[str, Any],
    class_name: Optional[str] = None,
    **instance_kwargs
):
    return meta_node_class_factory(
        base_class,
        class_attributes,
        class_name=class_name,
    )(**instance_kwargs)


def _sanitize_string_for_class_name(s, length_limit_to_hash=30):
    """
    Modified from
    https://stackoverflow.com/questions/3303312/how-do-i-convert-a-string-to-a-valid-variable-name-in-python
    """
    # Replace invalid characters
    s = re.sub('[^0-9a-zA-Z_]', 'X', s)

    # Replace leading characters until we find a letter or underscore
    s = re.sub('^[^a-zA-Z_]+', 'x', s)

    if len(s) > length_limit_to_hash:
        s = f"ParamHash{hashlib.sha1(s)}"

    return s
