"""
Classes for "lexical" reasoning.

The motivation here is to be able to provide the object with a unique identifier
in the context of other lexical objects. Each object may have at most one parent,
while lexical parents may have an arbitrary number of children, and each child's name
must be unique in the scope of that parent. In this way, when lexical parents are also
themselves lexical, we can build a path from the parent-most object to any child that
is completely unique. The typical filesystem on a computer is an excellent
example and fulfills our requirements, the only reason we depart from it is so that
we are free to have objects stored in different locations (possibly even on totally
different drives or machines) belong to the same lexical group.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from difflib import get_close_matches
from pathlib import Path
from typing import ClassVar, Generic, TypeVar

from bidict import bidict

from pyiron_workflow.logging import logger
from pyiron_workflow.mixin.has_interface_mixins import HasLabel, UsesState

ParentType = TypeVar("ParentType", bound="LexicalParent")


class Lexical(UsesState, HasLabel, Generic[ParentType], ABC):
    """
    An object with a unique lexical path.

    The lexical parent object (if any), and the parent-most object are both easily
    accessible.
    """

    lexical_delimiter: ClassVar[str] = "/"

    def __init__(
        self,
        *args,
        label: str | None = None,
        parent: ParentType | None = None,
        **kwargs,
    ):
        self._label = ""
        self._parent = None
        self._detached_parent_path = None
        self.label = self.__class__.__name__ if label is None else label
        self.parent = parent
        super().__init__(*args, **kwargs)

    @classmethod
    @abstractmethod
    def parent_type(cls) -> type[ParentType]:
        pass

    def _check_label(self, new_label: str) -> None:
        super()._check_label(new_label)
        if self.lexical_delimiter in new_label:
            raise ValueError(
                f"Lexical delimiter {self.lexical_delimiter} cannot be in new label "
                f"{new_label}"
            )

    @property
    def parent(self) -> ParentType | None:
        return self._parent

    @parent.setter
    def parent(self, new_parent: ParentType | None) -> None:
        self._set_parent(new_parent)

    def _set_parent(self, new_parent: ParentType | None):
        """
        mypy is uncooperative with super calls for setters, so we pull the behaviour
        out.
        """
        if new_parent is self._parent:
            # Exit early if nothing is changing
            return

        if new_parent is not None and not isinstance(new_parent, self.parent_type()):
            raise ValueError(
                f"Expected None or a {self.parent_type()} for the parent of "
                f"{self.label}, but got {new_parent}"
            )

        _ensure_path_is_not_cyclic(new_parent, self)

        if (
            self._parent is not None
            and new_parent is not self._parent
            and self in self._parent.children
        ):
            self._parent.remove_child(self)
        self._parent = new_parent
        self._detached_parent_path = None
        if self._parent is not None:
            self._parent.add_child(self)

    @property
    def lexical_path(self) -> str:
        """
        The path of node labels from the graph root (parent-most node) down to this
        node.
        """
        prefix: str
        if self.parent is None and self.detached_parent_path is None:
            prefix = ""
        elif self.parent is None and self.detached_parent_path is not None:
            prefix = self.detached_parent_path
        elif self.parent is not None and self.detached_parent_path is None:
            if isinstance(self.parent, Lexical):
                prefix = self.parent.lexical_path
            else:
                prefix = self.lexical_delimiter + self.parent.label
        else:
            raise ValueError(
                f"The parent and detached path should not be able to take non-None "
                f"values simultaneously, but got {self.parent} and "
                f"{self.detached_parent_path}, respectively. Please raise an issue on "
                f"GitHub outlining how your reached this state."
            )
        return prefix + self.lexical_delimiter + self.label

    @property
    def detached_parent_path(self) -> str | None:
        """
        The get/set state cycle of :class:`Lexical` de-parents objects, but we may
        still be interested in the lexical path -- e.g. if we `pickle` dump and load
        the object we will lose parent information, but this will still hold what the
        path _was_ before the orphaning process.

        The detached path will get cleared if a new parent is set, but is otherwise
        used as the root for the purposes of finding the lexical path.
        """
        return self._detached_parent_path

    @property
    def full_label(self) -> str:
        """
        A shortcut that combines the lexical path and label into a single string.
        """
        return self.lexical_path

    @property
    def lexical_root(self) -> Lexical:
        """The parent-most object in this lexical path; may be self."""
        if isinstance(self.parent, Lexical):
            return self.parent.lexical_root
        else:
            return self

    def as_path(self, root: Path | str | None = None) -> Path:
        """
        The lexical path as a :class:`pathlib.Path`, with a filesystem :param:`root`
        (default is the current working directory).
        """
        return (Path.cwd() if root is None else Path(root)).joinpath(
            *self.lexical_path.split(self.lexical_delimiter)
        )

    def __getstate__(self):
        state = super().__getstate__()
        if self.parent is not None:
            state["_detached_parent_path"] = self.parent.lexical_path
        state["_parent"] = None
        # Regarding removing parent from state:
        # Basically we want to avoid recursion during (de)serialization; when the
        # parent object is deserializing itself, _it_ should know who its children are
        # and inform them of this.
        # In the case the object gets passed to another process using __getstate__,
        # this also avoids dragging our whole lexical parent graph along with us.
        return state


class CyclicPathError(ValueError):
    """
    To be raised when adding a child would result in a cyclic lexical path.
    """


ChildType = TypeVar("ChildType", bound=Lexical)


class LexicalParent(HasLabel, Generic[ChildType], ABC):
    """
    A labeled object with a collection of uniquely-named lexical children.

    Children should be added or removed via the :meth:`add_child` and
    :meth:`remove_child` methods and _not_ by direct manipulation of the
    :attr:`children` container.

    Children are dot-accessible and appear in :meth:`__dir__` for tab-completion.

    Iterating over the parent yields the children, and the length of the parent is
    the number of children.

    When adding children or assigning parents, a check is performed on the lexical
    path to forbid cyclic paths.
    """

    def __init__(
        self,
        *args,
        strict_naming: bool = True,
        **kwargs,
    ):
        self._children: bidict[str, ChildType] = bidict()
        self.strict_naming = strict_naming
        super().__init__(*args, **kwargs)

    @classmethod
    @abstractmethod
    def child_type(cls) -> type[ChildType]:
        # Dev note: In principle, this could be a regular attribute
        # However, in other situations this is precluded (e.g. in channels)
        # since it would result in circular references.
        # Here we favour consistency over brevity,
        # and maintain the X_type() class method pattern
        pass

    @property
    def children(self) -> bidict[str, ChildType]:
        return self._children

    @property
    def child_labels(self) -> tuple[str]:
        return tuple(child.label for child in self)

    def _check_label(self, new_label: str) -> None:
        super()._check_label(new_label)
        if self.child_type().lexical_delimiter in new_label:
            raise ValueError(
                f"Child type ({self.child_type()}) lexical delimiter "
                f"{self.child_type().lexical_delimiter} cannot be in new label "
                f"{new_label}"
            )

    def __getattr__(self, key) -> ChildType:
        try:
            return self._children[key]
        except KeyError as key_error:
            # Raise an attribute error from getattr to make sure hasattr works well!
            msg = f"Could not find attribute '{key}' on {self.label} "
            msg += f"({self.__class__.__name__}) or among its children "
            msg += f"({self._children.keys()})."
            matches = get_close_matches(key, self._children.keys(), cutoff=0.8)
            if len(matches) > 0:
                msg += f" Did you mean '{matches[0]}' and not '{key}'?"
            raise AttributeError(msg) from key_error

    def __iter__(self):
        return self.children.values().__iter__()

    def __len__(self) -> int:
        return len(self.children)

    def __dir__(self):
        return set(super().__dir__() + list(self.children.keys()))

    def add_child(
        self,
        child: ChildType,
        label: str | None = None,
        strict_naming: bool | None = None,
    ) -> ChildType:
        """
        Add a child, optionally assigning it a new label in the process.

        Args:
            child (ChildType): The child to add.
            label (str|None): A (potentially) new label to assign the child. (Default
                is None, leave the child's label alone.)
            strict_naming (bool|None): Whether to append a suffix to the label if
                another child is already held with the same label. (Default is None,
                use the class-level flag.)

        Returns:
            (ChildType): The child being added.

        Raises:
            TypeError: When the child is not of an allowed class.
            ValueError: When the child has a different parent already.
            AttributeError: When the label is already an attribute (but not a child).
            AttributeError: When the label conflicts with another child and
                `strict_naming` is true.

        """
        if not isinstance(child, self.child_type()):
            raise TypeError(
                f"{self.label} expected a new child of type {self.child_type()} "
                f"but got {child}"
            )

        _ensure_path_is_not_cyclic(self, child)

        self._ensure_child_has_no_other_parent(child)

        label = child.label if label is None else label
        strict_naming = self.strict_naming if strict_naming is None else strict_naming

        if self._this_child_is_already_at_this_label(child, label):
            pass
        else:
            label = self._get_unique_label(label, strict_naming)

            if self._this_child_is_already_at_a_different_label(child, label):
                self.children.inv.pop(child)

            # Finally, update label and reflexively form the parent-child relationship
            child.label = label
            self.children[child.label] = child
            child.parent = self
        return child

    def _ensure_child_has_no_other_parent(self, child: Lexical) -> None:
        if child.parent is not None and child.parent is not self:
            raise ValueError(
                f"The child ({child.label}) already belongs to the parent "
                f"{child.parent.label}. Please remove it there before trying to "
                f"add it to this parent ({self.label})."
            )

    def _this_child_is_already_at_this_label(self, child: Lexical, label: str) -> bool:
        return (
            label == child.label
            and label in self.child_labels
            and self.children[label] is child
        )

    def _this_child_is_already_at_a_different_label(self, child, label) -> bool:
        return child.parent is self and label != child.label

    def _get_unique_label(self, label: str, strict_naming: bool) -> str:
        if label in self.__dir__():
            if label in self.child_labels:
                if strict_naming:
                    raise AttributeError(
                        f"{label} is already the label for a child. Please remove it "
                        f"before assigning another child to this label."
                    )
                else:
                    label = self._add_suffix_to_label(label)
            else:
                raise AttributeError(
                    f"{label} is an attribute or method of the {self.__class__} class, "
                    f"and cannot be used as a child label."
                )
        return label

    def _add_suffix_to_label(self, label: str) -> str:
        i = 0
        new_label = label
        while new_label in self.__dir__():
            # We search dir and not just the child_labels for the edge case that
            # someone has a very label-like attribute
            new_label = f"{label}{i}"
            i += 1
        if new_label != label:
            logger.info(
                f"{label} is already a node; appending an index to the "
                f"node label instead: {new_label}"
            )
        return new_label

    def remove_child(self, child: ChildType | str) -> ChildType:
        if isinstance(child, str):
            child_instance = self.children.pop(child)
        elif isinstance(child, self.child_type()):
            self.children.inv.pop(child)
            child_instance = child
        else:
            raise TypeError(
                f"{self.label} expected to remove a child of type str or "
                f"{self.child_type()} but got {child}"
            )

        child_instance.parent = None

        return child_instance

    def __getstate__(self):
        state = super().__getstate__()

        # Remove the children from the state and store each element right in the state
        # -- the labels are guaranteed to not be attributes already so this is safe,
        # and it makes sure that the state path matches the lexical path
        del state["_children"]
        state["child_labels"] = self.child_labels
        for child in self:
            state[child.label] = child

        return state

    def __setstate__(self, state):
        # Reconstruct children from state
        # Remove them from the state as you go, so they don't hang around in the
        # __dict__ after we set state -- they were only there to start with to guarantee
        # that the state path and the lexical path matched (i.e. without ".children."
        # in between)
        state["_children"] = bidict(
            {label: state.pop(label) for label in state.pop("child_labels")}
        )

        super().__setstate__(state)

        self._children = bidict(self._children)

        # Children purge their parent information in their __getstate__. This avoids
        # recursion, so we don't need to ship an entire graph off to a second process,
        # but rather can send just the requested object and its scope (lexical
        # children). So, now return their parent to them:
        for child in self:
            child.parent = self


def _ensure_path_is_not_cyclic(parent, child: Lexical) -> None:
    if isinstance(parent, Lexical) and parent.lexical_path.startswith(
        child.lexical_path + child.lexical_delimiter
    ):
        raise CyclicPathError(
            f"{parent.label} cannot be the parent of {child.label}, because its "
            f"lexical path is already in {child.label}'s path and cyclic paths "
            f"are not allowed. (i.e. {child.lexical_path} is in "
            f"{parent.lexical_path})"
        )
