from __future__ import annotations
from pathlib import Path
import shutil


def delete_files_and_directories_recursively(path):
    if not path.exists():
        return
    for item in path.rglob("*"):
        if item.is_file():
            item.unlink()
        else:
            delete_files_and_directories_recursively(item)
    path.rmdir()


def categorize_folder_items(folder_path):
    types = [
        "dir",
        "file",
        "mount",
        "symlink",
        "block_device",
        "char_device",
        "fifo",
        "socket",
    ]
    results = {t: [] for t in types}

    for item in folder_path.iterdir():
        for tt in types:
            try:
                if getattr(item, f"is_{tt}")():
                    results[tt].append(str(item))
            except NotImplementedError:
                pass
    return results


def _resolve_directory_and_path(
    file_name: str,
    directory: DirectoryObject | str | None = None,
    default_directory: str = ".",
):
    """
    Internal routine to separate the file name and the directory in case
    file name is given in absolute path etc.
    """
    path = Path(file_name)
    file_name = path.name
    if path.is_absolute():
        if directory is not None:
            raise ValueError(
                "You cannot set `directory` when `file_name` is an absolute path"
            )
        # If absolute path, take that of new_file_name regardless of the
        # name of directory
        directory = str(path.parent)
    else:
        if directory is None:
            # If directory is not given, take default directory
            directory = default_directory
        else:
            # If the directory is given, use it as the main path and append
            # additional path if given in new_file_name
            if isinstance(directory, DirectoryObject):
                directory = directory.path
        directory = directory / path.parent
    if not isinstance(directory, DirectoryObject):
        directory = DirectoryObject(directory)
    return file_name, directory


class DirectoryObject:
    def __init__(self, directory: str | Path | DirectoryObject):
        if isinstance(directory, str):
            self.path = Path(directory)
        elif isinstance(directory, Path):
            self.path = directory
        elif isinstance(directory, DirectoryObject):
            self.path = directory.path
        self.create()

    def create(self):
        self.path.mkdir(parents=True, exist_ok=True)

    def delete(self, only_if_empty: bool = False):
        if self.is_empty() or not only_if_empty:
            delete_files_and_directories_recursively(self.path)

    def list_content(self):
        return categorize_folder_items(self.path)

    def __len__(self):
        return sum([len(cc) for cc in self.list_content().values()])

    def __repr__(self):
        return f"DirectoryObject(directory='{self.path}')\n{self.list_content()}"

    def get_path(self, file_name):
        return self.path / file_name

    def file_exists(self, file_name):
        return self.get_path(file_name).is_file()

    def write(self, file_name, content, mode="w"):
        with self.get_path(file_name).open(mode=mode) as f:
            f.write(content)

    def create_subdirectory(self, path):
        return DirectoryObject(self.path / path)

    def create_file(self, file_name):
        return FileObject(file_name, self)

    def is_empty(self) -> bool:
        return len(self) == 0

    def remove_files(self, *files: str):
        for file in files:
            path = self.get_path(file)
            if path.is_file():
                path.unlink()


class NoDestinationError(ValueError):
    """A custom error for when neither a new file name nor new location are provided"""


class FileObject:
    def __init__(self, file_name: str, directory: DirectoryObject = None):
        self._file_name, self.directory = _resolve_directory_and_path(
            file_name=file_name, directory=directory, default_directory="."
        )

    @property
    def file_name(self):
        return self._file_name

    @property
    def path(self):
        return self.directory.path / Path(self._file_name)

    def write(self, content, mode="x"):
        self.directory.write(file_name=self.file_name, content=content, mode=mode)

    def read(self, mode="r"):
        with open(self.path, mode=mode) as f:
            return f.read()

    def is_file(self):
        return self.directory.file_exists(self.file_name)

    def delete(self):
        self.path.unlink()

    def __str__(self):
        return str(self.path.absolute())

    def _resolve_directory_and_path(
        self,
        file_name: str,
        directory: DirectoryObject | str | None = None,
        default_directory: str = ".",
    ):
        """
        Internal routine to separate the file name and the directory in case
        file name is given in absolute path etc.
        """
        path = Path(file_name)
        file_name = path.name
        if path.is_absolute():
            # If absolute path, take that of new_file_name regardless of the
            # name of directory
            directory = str(path.parent)
        else:
            if directory is None:
                # If directory is not given, take default directory
                directory = default_directory
            else:
                # If the directory is given, use it as the main path and append
                # additional path if given in new_file_name
                if isinstance(directory, DirectoryObject):
                    directory = directory.path
            directory = directory / path.parent
        if not isinstance(directory, DirectoryObject):
            directory = DirectoryObject(directory)
        return file_name, directory

    def copy(
        self,
        new_file_name: str | None = None,
        directory: DirectoryObject | str | None = None,
    ):
        """
        Copy an existing file to a new location.
        Args:
            new_file_name (str): New file name. You can also set
                an absolute path (in which case `directory` will be ignored)
            directory (DirectoryObject): Directory. If None, the same
                directory is used
        Returns:
            (FileObject): file object of the new file
        """
        if new_file_name is None:
            if directory is None:
                raise NoDestinationError(
                    "Either new file name or directory must be specified"
                )
            new_file_name = self.file_name
        file_name, directory = self._resolve_directory_and_path(
            new_file_name, directory, default_directory=self.directory.path
        )
        new_file = FileObject(file_name, directory.path)
        shutil.copy(str(self.path), str(new_file.path))
        return new_file
