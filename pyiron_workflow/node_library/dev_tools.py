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
            self.node.parent.replace(self.node, other)
            # print ('replacer called', self.node.label, other, kwargs) #, other.info, dir(other))
            self.node = self.parent[self.node_label]
            return self.node.update_input(**kwargs)

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
