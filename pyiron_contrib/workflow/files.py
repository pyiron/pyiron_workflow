from pathlib import Path


def delete_files_and_directories_recursively(path):
    if not path.exists():
        return
    for item in path.rglob('*'):
        if item.is_file():
            item.unlink()
        else:
            delete_files_and_directories_recursively(item)
    path.rmdir()
    
    
class DirectoryObject:
    def __init__(self, directory):
        self.path = Path(directory)
        self.create()

    def create(self):
        self.path.mkdir(parents=True, exist_ok=True)

    def delete(self):
        delete_files_and_directories_recursively(self.path)

    def list_files(self):
        return list(self.path.glob('*'))

    def __len__(self):
        return len(self.list_files())

    def __repr__(self):
        return f"DirectoryObject(directory='{self.path}' with {len(self)} files)"

    def write(self, file_name, content, mode="w"):
        path = self.path / Path(file_name)
        with path.open(mode=mode) as f:
            f.write(content)

    def create_subdirectory(self, path):
        return DirectoryObject(self.path / path)


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

    def write(self, content, mode='w'):
        self.directory.write(file_name=self.file_name, content=content, mode=mode)

    def read(self, mode='r'):
        with open(self.path, mode=mode) as f:
            return f.read()

    def delete(self):
        self.path.unlink()
