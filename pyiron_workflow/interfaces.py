"""
Container classes for giving access to various workflow objects and tools
"""

from __future__ import annotations

from importlib import import_module
import pkgutil
from sys import version_info
from types import ModuleType
from typing import Optional, TYPE_CHECKING

from bidict import bidict
from pyiron_workflow.snippets.singleton import Singleton

# Import all the supported executors
from pympipool import Executor as PyMpiPoolExecutor, PyMPIExecutor

try:
    from pympipool import PySlurmExecutor
except ImportError:
    PySlurmExecutor = None
try:
    from pympipool import PyFluxExecutor
except ImportError:
    PyFluxExecutor = None

from pyiron_workflow.executors import CloudpickleProcessPoolExecutor

# Then choose one executor to be "standard"
Executor = PyMpiPoolExecutor

from pyiron_workflow.function import (
    Function,
    SingleValue,
    function_node,
    single_value_node,
)
from pyiron_workflow.snippets.dotdict import DotDict

if TYPE_CHECKING:
    from pyiron_workflow.node_package import NodePackage


class Creator(metaclass=Singleton):
    """
    A container class for providing access to various workflow objects.
    Handles the registration of new node packages and, by virtue of being a singleton,
    makes them available to all composite nodes holding a creator.

    In addition to node objects, the creator also provides workflow-compliant executors
    for parallel processing.
    This includes a very simple in-house executor that is useful for learning, but also
    choices from the :mod:`pympipool` packages.
    Some :mod:`pympipool` executors may not be available on your machine (e.g. flux- and/or
     slurm-based executors), in which case these attributes will return `None` instead.
    """

    def __init__(self):
        self._package_access = DotDict()
        self._package_registry = bidict()

        self.Executor = Executor
        self.CloudpickleProcessPoolExecutor = CloudpickleProcessPoolExecutor
        self.PyMPIExecutor = PyMPIExecutor
        self.PyMpiPoolExecutor = PyMpiPoolExecutor

        self.Function = Function
        self.SingleValue = SingleValue

        # Avoid circular imports by delaying import for children of Composite
        self._macro = None
        self._workflow = None
        self._meta = None

        if version_info[0] == 3 and version_info[1] >= 10:
            # These modules use syntactic sugar for type hinting that is only supported
            # in python >=3.10
            # If the CI skips testing on 3.9 gets dropped, we can think about removing
            # this if-clause and just letting users of python <3.10 hit an error.
            self.register("pyiron_workflow.node_library.standard", "standard")

    @property
    def PyFluxExecutor(self):
        if PyFluxExecutor is None:
            raise ImportError(f"{PyFluxExecutor.__name__} is not available")
        return PyFluxExecutor

    @property
    def PySlurmExecutor(self):
        if PySlurmExecutor is None:
            raise ImportError(f"{PySlurmExecutor.__name__} is not available")
        return PySlurmExecutor

    @property
    def Macro(self):
        if self._macro is None:
            from pyiron_workflow.macro import Macro

            self._macro = Macro
        return self._macro

    @property
    def Workflow(self):
        if self._workflow is None:
            from pyiron_workflow.workflow import Workflow

            self._workflow = Workflow
        return self._workflow

    @property
    def meta(self):
        if self._meta is None:
            from pyiron_workflow.meta import (
                for_loop,
                input_to_list,
                list_to_output,
                while_loop,
            )
            from pyiron_workflow.snippets.dotdict import DotDict

            self._meta = DotDict(
                {
                    for_loop.__name__: for_loop,
                    input_to_list.__name__: input_to_list,
                    list_to_output.__name__: list_to_output,
                    while_loop.__name__: while_loop,
                }
            )
        return self._meta

    def __getattr__(self, item):
        try:
            return self._package_access[item]
        except KeyError as e:
            raise AttributeError(
                f"{self.__class__.__name__} could not find attribute {item} -- did you "
                f"forget to register node package to this key?"
            ) from e

    def __getitem__(self, item):
        try:
            return self._package_registry[item]
        except KeyError as e:
            raise KeyError(
                f"Could not find the package {item} -- are you sure it's registered?"
            ) from e

    def __getstate__(self):
        return dict(self.__dict__)

    def __setstate__(self, state):
        self.__dict__.update(**state)

    def register(self, package_identifier: str, domain: Optional[str] = None) -> None:
        """
        Add a new package of nodes from the provided identifier.
        The new package is available by item-access using the identifier, and, if a
        domain was provided, by attribute access under that domain path ("."-split
        strings allow for deep-registration of domains).
        Add a new package of nodes under the provided attribute, e.g. after adding
        nodes to the domain `"my_nodes"`, and instance of creator can call things like
        `creator.my_nodes.some_node_that_is_there()`.

        Currently, :param:`package_identifier` is just a python module string, and we
        allow recursive registration of multiple node packages when a module is
        provided whose sub-modules are node packages. If a :param:`domain` was
        provided, then it is extended by the same semantic path as the modules, e.g.
        if `my_python_module` is registered to the domain `"mpm"`, and
        `my_python_module.submod1` and `my_python_module.submod2.subsub` are both node
        packages, then `mpm.submod1` and `mpm.submod2.subsub` will both be available
        for attribute access.

        Note: If a macro is going to use a creator, the node registration should be
            _inside_ the macro definition to make sure the node actually has access to
            those nodes! It also needs to be _able_ to register those nodes, i.e. have
            import access to that location, but we don't for that check that.

        Args:
            package_identifier (str): An identifier for the node package. (Right now
                that's just a string version of the path to the module, e.g.
                `pyiron_workflow.node_library.standard`.)
            domain (str|None): The attribute name at which to register the new package.
                (Note: no sanitizing is done here except for splitting on "." to create
                sub-domains, so if you provide a string that won't work as an attribute
                name, that's your problem.) (Default is None, don't provide attribute
                access to this package.)

        Raises:
            KeyError: If the domain already exists, but the identifier doesn't match
                with the stored identifier.
            AttributeError: If you try to register at a domain that is already another
                method or attribute of the creator.
            ValueError: If the identifier can't be parsed.
        """
        if domain in super().__dir__():
            # We store package names in __dir__ for autocomplete, so here look only
            # at the parent-class __dir__, which stores actual properties and methods,
            # but _not_ the node packages
            raise AttributeError(f"{domain} is already an attribute of {self}")

        try:
            module = import_module(package_identifier)
            if domain is not None:
                if "." in domain:
                    domain, container = self._get_deep_container(domain)
                else:
                    container = self._package_access
                self._register_recursively_from_module(module, domain, container)
            else:
                self._register_recursively_from_module(module, None, None)
        except ModuleNotFoundError as e:
            raise ModuleNotFoundError(
                f"In the current implementation, we expect package identifiers to be "
                f"modules, but {package_identifier} couldn't be imported. If this "
                f"looks like a module, perhaps it's simply not in your path?"
            ) from e

    def _get_deep_container(self, semantic_domain: str) -> tuple[str, DotDict]:
        from pyiron_workflow.node_package import NodePackage

        container = self._package_access
        path = semantic_domain.split(".")

        while len(path) > 1:
            step = path.pop(0)
            if not hasattr(container, step):
                container[step] = DotDict()
            elif isinstance(container[step], NodePackage):
                raise ValueError(
                    f"The semantic path {semantic_domain} is invalid because it uses "
                    f"{step} as an intermediary, but this is already a "
                    f"{NodePackage.__name__}"
                )
            container = container[step]
        domain = path[0]
        return domain, container

    def _register_recursively_from_module(
        self, module: ModuleType, domain: str | None, container: DotDict | None
    ) -> None:
        if hasattr(module, "__path__"):
            if domain is not None:
                if domain not in container.keys():
                    container[domain] = DotDict()
                container = container[domain]
            else:
                container = None
            subcontainer = container

            for _, submodule_name, _ in pkgutil.walk_packages(
                module.__path__, module.__name__ + "."
            ):
                submodule = import_module(submodule_name)
                subdomain = None if domain is None else submodule_name.split(".")[-1]

                if not hasattr(submodule, "__path__"):
                    if hasattr(submodule, "nodes"):
                        # If it's a .py file with a `nodes` variable,
                        # assume that we want it
                        self._register_package_from_module(
                            submodule, subdomain, subcontainer
                        )
                else:
                    relative_path = submodule.__name__.replace(
                        module.__name__ + ".", ""
                    )
                    subcontainer = container
                    for step in relative_path.split("."):
                        if step not in subcontainer.keys():
                            subcontainer[step] = DotDict()
                        subcontainer = subcontainer[subdomain]
        else:
            self._register_package_from_module(module, domain, container)

    def _register_package_from_module(
        self, module: ModuleType, domain: str | None, container: dict | DotDict | None
    ) -> None:
        package = self._get_existing_package_or_register_a_new_one(module.__name__)
        # NOTE: Here we treat the package identifier and the module name as equivalent

        if domain is not None:
            if domain not in container.keys():
                # If the container _doesn't_ yet have anything at this domain, just add it
                container[domain] = package
            else:
                self._raise_error_unless_new_package_matches_existing(
                    container, domain, package
                )

    def _get_existing_package_or_register_a_new_one(
        self, package_identifier: str
    ) -> NodePackage:
        try:
            # If the package is already registered, grab that instance
            package = self._package_registry[package_identifier]
        except KeyError:
            # Otherwise make a new package
            from pyiron_workflow.node_package import NodePackage

            package = NodePackage(package_identifier)
            self._package_registry[package_identifier] = package
        return package

    def _raise_error_unless_new_package_matches_existing(
        self, container: DotDict, domain: str, package: NodePackage
    ) -> None:
        try:
            if container[domain].package_identifier != package.package_identifier:
                raise ValueError(
                    f"The domain {domain} already holds the package "
                    f"{container[domain].package_identifier}, and cannot store the "
                    f"package {package.package_identifier}"
                )
        except AttributeError:
            raise ValueError(
                f"The domain {domain} is already a container, and cannot be "
                f"overwritten with the package {package.package_identifier}"
            )

    def __dir__(self) -> list[str]:
        return super().__dir__() + list(self._package_access.keys())


class Wrappers(metaclass=Singleton):
    """
    A container class giving access to the decorators that transform functions to nodes.
    """

    def __init__(self):
        self.function_node = function_node
        self.single_value_node = single_value_node

        # Avoid circular imports by delaying import when wrapping children of Composite
        self._macro_node = None

    @property
    def macro_node(self):
        if self._macro_node is None:
            from pyiron_workflow.macro import macro_node

            self._macro_node = macro_node
        return self._macro_node
