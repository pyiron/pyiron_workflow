from __future__ import annotations

from abc import ABC
from typing import Optional

from bidict import bidict


class Semantic(ABC):
    """
    Base class for "semantic" objects.

    The motivation here is to be able to provide the object with a unique identifier
    in the context of other semantic objects. Each object may have exactly one parent
    and an arbitrary number of children, and each child's name must be unique in the
    scope of that parent. In this way, the path from the parent-most object to any
    child is completely unique. The typical filesystem on a computer is an excellent
    example and fulfills our requirements, the only reason we depart from it is so that
    we are free to have objects stored in different locations (possibly even on totally
    different drives or machines) belong to the same semantic group.
    """

    delimiter = "/"

    def __init__(
        self,
        label: str,
        *args,
        parent: Optional[Semantic] = None,
        strict_naming: bool = True,
        **kwargs
    ):
        self._label = None
        self._parent = None
        self._children = bidict()
        self.label = label
        self.parent = parent
        self.strict_naming = strict_naming
        super().__init__(self, *args, **kwargs)

    @property
    def label(self) -> str:
        return self._label

    @label.setter
    def label(self, new_label: str) -> None:
        if not isinstance(new_label, str):
            raise TypeError(f"Expected a string label but got {new_label}")
        if self.delimiter in new_label:
            raise ValueError(f"{self.delimiter} cannot be in the label")
        self._label = new_label

    @property
    def parent(self) -> Semantic | None:
        return self._parent

    @parent.setter
    def parent(self, new_parent: Semantic | None) -> None:
        if new_parent is self._parent:
            # Exit early if nothing is changing
            return

        if new_parent is not None:
            if not isinstance(new_parent, Semantic):
                raise ValueError(
                    f"Expected None or another {Semantic.__name__} for the "
                    f"semantic parent of {self.label}, but got {new_parent}"
                )

        if self._parent is not None and new_parent is not self._parent:
            self._parent.remove_child(self)
        self._parent = new_parent
        self._parent.add_child(self)

    @property
    def children(self) -> bidict[str: Semantic]:
        return self._children

    def add_child(self, child: Semantic):
        if not isinstance(child, Semantic):
            raise TypeError(
                f"{self.label} expected a new child of type {Semantic.__name__} "
                f"but got {child}"
            )

        try:
            existing_child = self.children[child.label]
            if existing_child is child:
                # Exit early if nothing is changing
                return
            else:
                raise ValueError(
                    f"{self.label} cannot add the child {child.label} "
                    f"because another child already exists with this name"
                )
        except KeyError:
            # If it's a new name, that's fine
            pass

        self.children[child.label] = child
        child.parent = self

    def remove_child(self, child: Semantic | str):
        if isinstance(child, str):
            child = self.children.pop(child)
        elif isinstance(child, Semantic):
            self.children.pop(child)
        else:
            raise TypeError(
                f"{self.label} expected to remove a child of type str or "
                f"{Semantic.__name__} but got {child}"
            )

        if child.parent is not None:
            child.parent = None

    @property
    def path(self) -> str:
        """
        The path of node labels from the graph root (parent-most node) down to this
        node.
        """
        prefix = "" if self.parent is None else self.parent.path
        return prefix + self.delimiter + self.label

    @property
    def root(self) -> Semantic:
        """The parent-most object in this semantic path; may be self."""
        return self if self.parent is None else self.root

    def __getstate__(self):
        state = dict(self.__dict__)
        state["_parent"] = None
        # Comment on moving this to semantics)
        # Basically we want to avoid recursion during (de)serialization; when the
        # parent object is deserializing itself, _it_ should know who its children are
        # and inform them of this.
        #
        # Original comment when this behaviour belonged to node)
        # I am not at all confident that removing the parent here is the _right_
        # solution.
        # In order to run composites on a parallel process, we ship off just the nodes
        # and starting nodes.
        # When the parallel process returns these, they're obviously different
        # instances, so we re-parent them back to the receiving composite.
        # At the same time, we want to make sure that the _old_ children get orphaned.
        # Of course, we could do that directly in the composite method, but it also
        # works to do it here.
        # Something I like about this, is it also means that when we ship groups of
        # nodes off to another process with cloudpickle, they're definitely not lugging
        # along their parent, its connections, etc. with them!
        # This is all working nicely as demonstrated over in the macro test suite.
        # However, I have a bit of concern that when we start thinking about
        # serialization for storage instead of serialization to another process, this
        # might introduce a hard-to-track-down bug.
        # For now, it works and I'm going to be super pragmatic and go for it, but
        # for the record I am admitting that the current shallowness of my understanding
        # may cause me/us headaches in the future.
        # -Liam
        return state

    def __setstate__(self, state):
        self.__dict__.update(**state)
