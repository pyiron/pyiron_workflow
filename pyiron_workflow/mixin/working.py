"""
A mixin for classes that know about their working directory.
"""

from __future__ import annotations

from abc import ABC

from pyiron_snippets.files import DirectoryObject

from pyiron_workflow.mixin.has_interface_mixins import HasLabel, HasParent


class HasWorkingDirectory(HasLabel, HasParent, ABC):
    def __init__(self, *args, **kwargs):
        self._working_directory = None
        super().__init__(*args, **kwargs)

    @property
    def working_directory(self) -> DirectoryObject:
        if self._working_directory is None:
            if isinstance(self.parent, HasWorkingDirectory):
                parent_dir = self.parent.working_directory
                self._working_directory = parent_dir.create_subdirectory(self.label)
            else:
                self._working_directory = DirectoryObject(self.label)
        return self._working_directory

    def tidy_working_directory(self) -> None:
        """
        If the working directory is completely empty, deletes it.
        """
        if self.working_directory.is_empty():
            self.working_directory.delete()
            self._working_directory = None
            # Touching the working directory may have created it -- if it's there and
            # empty just clean it up
