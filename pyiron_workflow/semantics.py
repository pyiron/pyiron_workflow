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

from pyiron_workflow.snippets.logger import logger


class Semantic(ABC):
    """
    An object with a unique semantic path
    """

    semantic_delimiter = "/"

    def __init__(
        self,
        label: str,
        *args,
        parent: Optional[SemanticParent] = None,
        **kwargs
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
        self._parent.add_child(self)

    @property
    def semantic_path(self) -> str:
        """
        The path of node labels from the graph root (parent-most node) down to this
        node.
        """
        prefix = "" if self.parent is None else self.parent.path
        return prefix + self.semantic_delimiter + self.label

    @property
    def semantic_root(self) -> Semantic:
        """The parent-most object in this semantic path; may be self."""
        return self if self.parent is None else self.semantic_root

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


class _HasSemanticChildren(ABC):
    def __init__(
        self,
        *args,
        strict_naming: bool = True,
        **kwargs
    ):
        self._children = bidict()
        self.strict_naming = strict_naming
        super().__init__(*args, **kwargs)

    @property
    def children(self) -> bidict[str: Semantic]:
        return self._children

    @property
    def child_labels(self) -> tuple[str]:
        return tuple(child.label for child in self)

    def __getattr__(self, key):
        try:
            return self.children[key]
        except KeyError:
            # Raise an attribute error from getattr to make sure hasattr works well!
            raise AttributeError(
                f"Could not find attribute {key} on {self.label} "
                f"({self.__class__.__name__}) or among its children "
                f"({self.children.keys()})"
            )

    def __iter__(self):
        return self.children.values().__iter__()

    def __len__(self):
        return len(self.nodes)

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
            ValueError: When the label conflicts with another child and `strict_naming`
                is true.

        """
        if not isinstance(child, Semantic):
            raise TypeError(
                f"{self.label} expected a new child of type {Semantic.__name__} "
                f"but got {child}"
            )
        self._ensure_child_has_no_other_parent(child)

        label = child.label if label is None else label
        if label not in self.children.keys():  # Otherwise conflict may get resolved
            self._ensure_label_does_not_conflict_with_attr(label)

        strict_naming = self.strict_naming if strict_naming is None else strict_naming

        try:
            existing_child = self.children[label]
            if existing_child is child:
                if label is None or label == child.label:
                    # Exit early if nothing is changing
                    return child
                else:
                    # We're moving the child to a new label, delete the old location
                    del self.children[child.label]
            else:
                if strict_naming:
                    raise ValueError(
                        f"{self.label} cannot add the child {child.label} "
                        f"because another child already exists with this name"
                    )
                else:
                    label = self._add_suffix_to_label(child.label)
        except KeyError:
            # If it's a new name, only make sure the name is legal
            self._ensure_label_does_not_conflict_with_attr(label)

        # Finally, update label and reflexively form the parent-child relationship
        child.label = label
        self.children[child.label] = child
        child.parent = self
        return child

    def _ensure_child_has_no_other_parent(self, child: Semantic):
        if child.parent is not None and child.parent is not self:
            raise ValueError(
                f"The child ({child.label}) already belongs to the parent "
                f"{child.parent.label}. Please remove it there before trying to "
                f"add it to this parent ({self.label})."
            )

    def _ensure_label_does_not_conflict_with_attr(self, label: str):
        if label in self.__dir__():
            raise AttributeError(
                f"{label} is already in the __dir__ of {self.label}, please choose a "
                f"different label."
            )

    def _add_suffix_to_label(self, label):
        i = 0
        new_label = label
        while new_label in self.children.keys():
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
            self.children.pop(child)
        else:
            raise TypeError(
                f"{self.label} expected to remove a child of type str or "
                f"{Semantic.__name__} but got {child}"
            )

        child.parent = None

        return child

    def __getstate__(self):
        try:
            state = super().__getstate__()
        except AttributeError:
            state = dict(self.__dict__)

        # Remove the children from the state and store each element right in the state
        # -- the labels are guaranteed to not be attributes already so this is safe,
        # and it makes sure that the state path matches the semantic path
        del state["_children"]
        state["child_labels"] = self.node_labels
        for child in self:
            state[child.label] = child

        return state

    def __setstate__(self, state):
        # Reconstruct children from state
        state["_children"] = bidict(
            {label: state[label] for label in state.pop("child_labels")}
        )

        try:
            super().__setstate__(state)
        except AttributeError:
            self.__dict__.update(**state)


class SemanticParent(Semantic, _HasSemanticChildren, ABC):
    def __init__(
        self,
        label: str,
        *args,
        parent: Optional[SemanticParent] = None,
        strict_naming: bool = True,
        **kwargs
    ):
        super().__init__(
            *args, label=label, parent=parent, strict_naming=strict_naming, **kwargs
        )
