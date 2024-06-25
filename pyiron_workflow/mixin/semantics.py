"""
Classes for "semantic" reasoning.

The motivation here is to be able to provide the object with a unique identifier
in the context of other semantic objects. Each object may have exactly one parent
and an arbitrary number of children, and each child's name must be unique in the
scope of that parent. In this way, the path from the parent-most object to any
child is completely unique. The typical filesystem on a computer is an excellent
example and fulfills our requirements, the only reason we depart from it is so that
we are free to have objects stored in different locations (possibly even on totally
different drives or machines) belong to the same semantic group.
"""

from __future__ import annotations

from abc import ABC
from typing import Optional

from bidict import bidict

from pyiron_workflow.logging import logger
from pyiron_workflow.mixin.has_interface_mixins import HasLabel, HasParent, UsesState


class Semantic(UsesState, HasLabel, HasParent, ABC):
    """
    An object with a unique semantic path.

    The semantic parent object (if any), and the parent-most object are both easily
    accessible.
    """

    semantic_delimiter = "/"

    def __init__(
        self, label: str, *args, parent: Optional[SemanticParent] = None, **kwargs
    ):
        self._label = None
        self._parent = None
        self.label = label
        self.parent = parent
        super().__init__(*args, **kwargs)

    @property
    def label(self) -> str:
        return self._label

    @label.setter
    def label(self, new_label: str) -> None:
        if not isinstance(new_label, str):
            raise TypeError(f"Expected a string label but got {new_label}")
        if self.semantic_delimiter in new_label:
            raise ValueError(f"{self.semantic_delimiter} cannot be in the label")
        self._label = new_label

    @property
    def parent(self) -> SemanticParent | None:
        return self._parent

    @parent.setter
    def parent(self, new_parent: SemanticParent | None) -> None:
        if new_parent is self._parent:
            # Exit early if nothing is changing
            return

        if new_parent is not None:
            if not isinstance(new_parent, SemanticParent):
                raise ValueError(
                    f"Expected None or a {SemanticParent.__name__} for the parent of "
                    f"{self.label}, but got {new_parent}"
                )

        if self._parent is not None and new_parent is not self._parent:
            self._parent.remove_child(self)
        self._parent = new_parent
        if self._parent is not None:
            self._parent.add_child(self)

    @property
    def semantic_path(self) -> str:
        """
        The path of node labels from the graph root (parent-most node) down to this
        node.
        """
        prefix = self.parent.semantic_path if isinstance(self.parent, Semantic) else ""
        return prefix + self.semantic_delimiter + self.label

    @property
    def full_label(self) -> str:
        """
        A shortcut that combines the semantic path and label into a single string.
        """
        return self.semantic_path

    @property
    def semantic_root(self) -> Semantic:
        """The parent-most object in this semantic path; may be self."""
        return self.parent.semantic_root if isinstance(self.parent, Semantic) else self

    def __getstate__(self):
        state = super().__getstate__()
        state["_parent"] = None
        # Regarding removing parent from state:
        # Basically we want to avoid recursion during (de)serialization; when the
        # parent object is deserializing itself, _it_ should know who its children are
        # and inform them of this.
        # In the case the object gets passed to another process using __getstate__,
        # this also avoids dragging our whole semantic parent graph along with us.
        return state


class CyclicPathError(ValueError):
    """
    To be raised when adding a child would result in a cyclic semantic path.
    """


class SemanticParent(Semantic, ABC):
    """
    A semantic object with a collection of uniquely-named semantic children.

    Children should be added or removed via the :meth:`add_child` and
    :meth:`remove_child` methods and _not_ by direct manipulation of the
    :attr:`children` container.

    Children are dot-accessible and appear in :meth:`__dir__` for tab-completion.

    Iterating over the parent yields the children, and the length of the parent is
    the number of children.

    When adding children or assigning parents, a check is performed on the semantic
    path to forbid cyclic paths.
    """

    def __init__(
        self,
        label: str,
        *args,
        parent: Optional[SemanticParent] = None,
        strict_naming: bool = True,
        **kwargs,
    ):
        self._children = bidict()
        self.strict_naming = strict_naming
        super().__init__(*args, label=label, parent=parent, **kwargs)

    @property
    def children(self) -> bidict[str:Semantic]:
        return self._children

    @property
    def child_labels(self) -> tuple[str]:
        return tuple(child.label for child in self)

    def __getattr__(self, key):
        try:
            return self._children[key]
        except KeyError:
            # Raise an attribute error from getattr to make sure hasattr works well!
            raise AttributeError(
                f"Could not find attribute {key} on {self.label} "
                f"({self.__class__.__name__}) or among its children "
                f"({self._children.keys()})"
            )

    def __iter__(self):
        return self.children.values().__iter__()

    def __len__(self):
        return len(self.children)

    def __dir__(self):
        return set(super().__dir__() + list(self.children.keys()))

    def add_child(
        self,
        child: Semantic,
        label: Optional[str] = None,
        strict_naming: Optional[bool] = None,
    ) -> Semantic:
        """
        Add a child, optionally assigning it a new label in the process.

        Args:
            child (Semantic): The child to add.
            label (str|None): A (potentially) new label to assign the child. (Default
                is None, leave the child's label alone.)
            strict_naming (bool|None): Whether to append a suffix to the label if
                another child is already held with the same label. (Default is None,
                use the class-level flag.)

        Returns:
            (Semantic): The child being added.

        Raises:
            TypeError: When the child is not of an allowed class.
            ValueError: When the child has a different parent already.
            AttributeError: When the label is already an attribute (but not a child).
            AttributeError: When the label conflicts with another child and
                `strict_naming` is true.

        """
        if not isinstance(child, Semantic):
            raise TypeError(
                f"{self.label} expected a new child of type {Semantic.__name__} "
                f"but got {child}"
            )

        if isinstance(child, ParentMost):
            raise ParentMostError(
                f"{child.label} is {ParentMost.__name__} and may only take None as a "
                f"parent but was added as a child to {self.label}"
            )

        self._ensure_path_is_not_cyclic(self, child)

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
            child._parent = self
        return child

    @staticmethod
    def _ensure_path_is_not_cyclic(parent: SemanticParent | None, child: Semantic):
        if parent is not None and parent.semantic_path.startswith(
            child.semantic_path + child.semantic_delimiter
        ):
            raise CyclicPathError(
                f"{parent.label} cannot be the parent of {child.label}, because its "
                f"semantic path is already in {child.label}'s path and cyclic paths "
                f"are not allowed. (i.e. {child.semantic_path} is in "
                f"{parent.semantic_path})"
            )

    def _ensure_child_has_no_other_parent(self, child: Semantic):
        if child.parent is not None and child.parent is not self:
            raise ValueError(
                f"The child ({child.label}) already belongs to the parent "
                f"{child.parent.label}. Please remove it there before trying to "
                f"add it to this parent ({self.label})."
            )

    def _this_child_is_already_at_this_label(self, child: Semantic, label: str):
        return (
            label == child.label
            and label in self.child_labels
            and self.children[label] is child
        )

    def _this_child_is_already_at_a_different_label(self, child, label):
        return child.parent is self and label != child.label

    def _get_unique_label(self, label: str, strict_naming: bool):
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

    def _add_suffix_to_label(self, label):
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

    def remove_child(self, child: Semantic | str) -> Semantic:
        if isinstance(child, str):
            child = self.children.pop(child)
        elif isinstance(child, Semantic):
            self.children.inv.pop(child)
        else:
            raise TypeError(
                f"{self.label} expected to remove a child of type str or "
                f"{Semantic.__name__} but got {child}"
            )

        child._parent = None

        return child

    @property
    def parent(self) -> SemanticParent | None:
        return self._parent

    @parent.setter
    def parent(self, new_parent: SemanticParent | None) -> None:
        self._ensure_path_is_not_cyclic(new_parent, self)
        super(SemanticParent, type(self)).parent.__set__(self, new_parent)

    def __getstate__(self):
        state = super().__getstate__()

        # Remove the children from the state and store each element right in the state
        # -- the labels are guaranteed to not be attributes already so this is safe,
        # and it makes sure that the state path matches the semantic path
        del state["_children"]
        state["child_labels"] = self.child_labels
        for child in self:
            state[child.label] = child

        return state

    def __setstate__(self, state):
        # Reconstruct children from state
        # Remove them from the state as you go, so they don't hang around in the
        # __dict__ after we set state -- they were only there to start with to guarantee
        # that the state path and the semantic path matched (i.e. without ".children."
        # in between)
        state["_children"] = bidict(
            {label: state.pop(label) for label in state.pop("child_labels")}
        )

        super().__setstate__(state)

        self._children = bidict(self._children)

        # Children purge their parent information in their __getstate__ (this avoids
        # recursion, which is mainly done to accommodate h5io as most other storage
        # tools are able to store a reference to an object to overcom it), so now
        # return it to them:
        for child in self:
            child._parent = self


class ParentMostError(TypeError):
    """
    To be raised when assigning a parent to a parent-most object
    """


class ParentMost(SemanticParent, ABC):
    """
    A semantic parent that cannot have any other parent.
    """

    @property
    def parent(self) -> None:
        return None

    @parent.setter
    def parent(self, new_parent: None):
        if new_parent is not None:
            raise ParentMostError(
                f"{self.label} is {ParentMost.__name__} and may only take None as a "
                f"parent but got {type(new_parent)}"
            )
