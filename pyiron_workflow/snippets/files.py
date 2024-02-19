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


class DirectoryObject:
    def __init__(self, directory):
        self.path = Path(directory)
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


class FileObject:
    def __init__(self, file_name: str, directory: DirectoryObject):
        self.directory = directory
        self._file_name = file_name

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

    def _clean_directory_and_path(
        self, new_file_name: str, directory: DirectoryObject | str | None=None
    ):
        """
        Internal routine to separate the file name and the directory in case
        file name is given in absolute path etc.
        """
        new_path = Path(new_file_name)
        file_name = new_path.name
        if new_path.is_absolute():
            directory = str(new_path.resolve().parent)
        elif directory is None:
            directory = self.directory
        else:
            if isinstance(directory, DirectoryObject):
                directory = directory.path
            directory = str(directory / new_path.resolve().parent)
        if isinstance(directory, str):
            directory = DirectoryObject(directory)
        return file_name, directory

    def copy(
        self, new_file_name: str, directory: DirectoryObject | str | None=None
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
        file_name, directory = self._clean_directory_and_path(new_file_name, directory)
        new_file = FileObject(file_name, directory)
        shutil.copy(str(self.path), str(new_file.path))
        return new_file
