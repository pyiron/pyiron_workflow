# for development and testing only
# provide functionality, data types etc. that will be later moved to the workflow code

from pathlib import Path


class VarType:
    def __init__(
        self,
        value=None,
        dat_type=None,
        label: str = None,
        store: int = 0,
        generic: bool = None,
        doc: str = None,
    ):
        self.value = value
        self.type = dat_type
        self.label = label
        self.store = store
        self.generic = generic
        self.doc = doc


class FileObject:
    def __init__(self, path, directory=None):
        if directory is None:
            self._path = Path(path)
        else:
            self._path = Path(directory) / Path(path)

    def __repr__(self):
        return f"FileObject: {self._path} {self.is_file}"

    @property
    def path(self):
        # Note conversion to string (needed to satisfy glob which is used e.g. in dump parser)
        return str(self._path)

    @property
    def is_file(self):
        return self._path.is_file()

    @property
    def name(self):
        return self._path.name


class Replacer:
    from pyiron_workflow.node import Node

    def __init__(self, node: Node):
        self.node = node
        self.parent = node.parent
        self.node_label = node.label

    def __call__(self, other: Node, **kwargs):
        # This is just the current replacement code:
        if self.node.parent is not None:
            self.node.parent.replace_node(self.node, other)
            # print ('replacer called', self.node.label, other, kwargs) #, other.info, dir(other))
            self.node = self.parent[self.node_label]
            return self.node.set_input_values(**kwargs)

        else:
            import warnings

            warnings.warn(f"Could not replace {self.node.label}, as it has no parent.")


def set_replacer(node, node_dict):
    import functools

    replacer = Replacer(node)
    for k, v in node_dict.items():
        replacer.__setattr__(k, functools.partial(replacer, v))
    return replacer


def register_libraries(libraries, library_path="pyiron_workflow.node_library"):
    import importlib
    from pyiron_workflow.workflow import Workflow

    wf = Workflow("lib")
    for nodes in libraries:
        module = importlib.import_module(f"{library_path}.{nodes}")
        wf.create.register(nodes, *module.nodes)


# storage tools (hdf5 etc.)
# these tools are meant only for a proof of concept, some parts may be already present in
# the existing code, others should be moved there
def extract_value(value):
    if hasattr(value, "value"):
        value = value.value
    if hasattr(value, "_convert_to_dict"):
        value = value._convert_to_dict()
    return value


def nested_dict_from_keys(dictionary):
    result = {}

    for key, value in dictionary.items():
        keys = key.split("__")
        current_dict = result

        for k in keys[:-1]:
            current_dict = current_dict.setdefault(k, {})
            if (k == "name") or (k == "label"):
                raise ValueError(
                    f"Argument name {k} is used internally. Rename function argument!"
                )
        # print ('keys: ', keys, keys[:-1], len(keys), current_dict, current_dict is None)

        if current_dict is not None:
            current_dict[keys[-1]] = extract_value(value)

    return result


def to_data_container(data):
    from pyiron_base.storage.datacontainer import DataContainer

    return DataContainer(nested_dict_from_keys(data))


def node_to_data_container(node):
    from pyiron_base.storage.datacontainer import DataContainer

    i_data = nested_dict_from_keys(node.inputs.channel_dict)
    o_data = nested_dict_from_keys(node.outputs.channel_dict)
    dc_dict = {}
    dc_dict["input"] = i_data
    dc_dict["output"] = o_data
    return DataContainer(dc_dict)


# project class with minimized functionality
# idea is to avoid all dependencies that we have in the pyiron project class
# we should later decide what features from the original project class are needed
class JobType:
    job_class_dict = {}


class MiniProject:
    def __init__(self, path):
        self.path = path
        self.job_type = JobType()

    def copy(self):
        new = MiniProject(path=self.path)
        return new


from pyiron_base.storage.hdfio import ProjectHDFio
import pathlib


class DataStore:
    def __init__(self, path="."):
        self._path = path
        self._project = MiniProject(path)
        self._ensure_storage_folder_exists()

    def _ensure_storage_folder_exists(self):
        path = pathlib.Path(self._path)
        if not path.is_dir():
            path.mkdir(parents=True, exist_ok=True)

    def get_hdf(self, path, label):
        p = pathlib.Path(path, label)
        p = p.resolve()

        return ProjectHDFio(project=self._project, file_name=p.as_posix(), mode="a")

    def remove(self, node_label, path=None):
        if path is None:
            path = self._path
        p = pathlib.Path(path, f'{node_label}.h5')
        # print (p, p.is_file())
        if p.is_file():
            p.unlink()
            print (f'node {node_label} has been removed from store')

    def store(self, node, overwrite=False):
        if overwrite:
            self.remove(node.label)
        hdf = self.get_hdf(self._path, node.label)
        dc = node_to_data_container(node)
        dc.to_hdf(hdf)
        return True

    def load(self, node_label):
        from pyiron_base.storage.datacontainer import DataContainer

        hdf = self.get_hdf(self._path, node_label)
        new_node = DataContainer()

        new_node.from_hdf(hdf)
        return new_node
