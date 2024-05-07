# pyiron_workflow

[![Binder](https://mybinder.org/badge_logo.svg)](https://mybinder.org/v2/gh/pyiron/pyiron_workflow/HEAD)
[![License](https://img.shields.io/badge/License-BSD_3--Clause-blue.svg)](https://opensource.org/licenses/BSD-3-Clause)
[![Codacy Badge](https://app.codacy.com/project/badge/Grade/0b4c75adf30744a29de88b5959246882)](https://app.codacy.com/gh/pyiron/pyiron_workflow/dashboard?utm_source=gh&utm_medium=referral&utm_content=&utm_campaign=Badge_grade)
[![Coverage Status](https://coveralls.io/repos/github/pyiron/pyiron_workflow/badge.svg?branch=main)](https://coveralls.io/github/pyiron/pyiron_workflow?branch=main)
[![Documentation Status](https://readthedocs.org/projects/pyiron-workflow/badge/?version=latest)](https://pyiron-workflow.readthedocs.io/en/latest/?badge=latest)

[![Anaconda](https://anaconda.org/conda-forge/pyiron_workflow/badges/version.svg)](https://anaconda.org/conda-forge/pyiron_workflow)
[![Last Updated](https://anaconda.org/conda-forge/pyiron_workflow/badges/latest_release_date.svg
)](https://anaconda.org/conda-forge/pyiron_workflow)
[![Platform](https://anaconda.org/conda-forge/pyiron_workflow/badges/platforms.svg)](https://anaconda.org/conda-forge/pyiron_workflow)
[![Downloads](https://anaconda.org/conda-forge/pyiron_workflow/badges/downloads.svg)](https://anaconda.org/conda-forge/pyiron_workflow)

## Overview

`pyiron_workflow` is a framework for constructing workflows as computational graphs from simple python functions. Its objective is to make it as easy as possible to create reliable, reusable, and sharable workflows, with a special focus on research workflows for HPC environments.

Nodes are formed from python functions with simple decorators, and the resulting nodes can have their data inputs and outputs connected. 

By allowing (but not demanding, in the case of data DAGs) users to specify the execution flow, both cyclic and acyclic graphs are supported. 

By scraping type hints from decorated functions, both new data values and new graph connections are (optionally) required to conform to hints, making workflows strongly typed.

Individual node computations can be shipped off to parallel processes for scalability. (This is a beta-feature at time of writing; the `PyMPIExecutor` executor from [`pympipool`](https://github.com/pyiron/pympipool) is supported and tested; automated execution flows to not yet fully leverage the efficiency possible in parallel execution, and `pympipool`'s more powerful flux- and slurm- based executors have not been tested and may fail.)

Once you're happy with a workflow, it can be easily turned it into a macro for use in other workflows. This allows the clean construction of increasingly complex computation graphs by composing simpler graphs.

Nodes (including macros) can be stored in plain text as python code, and registered by future workflows for easy access. This encourages and supports an ecosystem of useful nodes, so you don't need to re-invent the wheel. (This is a beta-feature, with full support of [FAIR](https://en.wikipedia.org/wiki/FAIR_data) principles for node packages planned.)

Executed or partially-executed graphs can be stored to file, either by explicit call or automatically after running. When creating a new node(/macro/workflow), the working directory is automatically inspected for a save-file and the node will try to reload itself if one is found. (This is an alpha-feature, so it is currently only possible to save entire graphs at once and not individual nodes within a graph, all the child nodes in a saved graph must have been instantiated by `Workflow.create` (or equivalent, i.e. their code lives in a `.py` file that has been registered), and there are no safety rails to protect you from changing the node source code between saving and loading (which may cause errors/inconsistencies depending on the nature of the changes).) 

## Example

`pyiron_workflow` offers a single-point-of-entry in the form of the `Workflow` object, and uses decorators to make it easy to turn regular python functions into "nodes" that can be put in a computation graph.

Nodes can be used by themselves and -- other than being "delayed" in that their computation needs to be requested after they're instantiated -- they feel an awful lot like the regular python functions they wrap:

```python
>>> from pyiron_workflow import Workflow
>>>
>>> @Workflow.wrap.as_function_node()
... def add_one(x):
...     return x + 1
>>>
>>> add_one(add_one(add_one(x=0)))()
3

```

But the intent is to collect them together into a workflow and leverage existing nodes. We can directly perform (many but not quite all) python actions natively on output channels, can build up data graph topology by simply assigning values (to attributes or at instantiation), and can package things together into reusable macros with customizable IO interfaces:

```python
>>> from pyiron_workflow import Workflow
>>> Workflow.register("pyiron_workflow.node_library.plotting", "plotting")
>>>
>>> @Workflow.wrap.as_function_node()
... def Arange(n: int):
...     import numpy as np
...     return np.arange(n)
>>>
>>> @Workflow.wrap.as_macro_node("fig")
... def PlotShiftedSquare(self, n: int, shift: int = 0):
...     self.arange = Arange(n)
...     self.plot = self.create.plotting.Scatter(
...         x=self.arange + shift,
...         y=self.arange**2
...     )
...     return self.plot
>>> 
>>> wf = Workflow("plot_with_and_without_shift")
>>> wf.n = wf.create.standard.UserInput()
>>> wf.no_shift = PlotShiftedSquare(shift=0, n=wf.n)
>>> wf.shift = PlotShiftedSquare(shift=2, n=wf.n)
>>> 
>>> diagram = wf.draw()
>>> 
>>> out = wf(shift__shift=3, n__user_input=10)

```

Which gives the workflow `diagram`

![](_static/readme_diagram.png)

And the resulting figure (when axes are not cleared)

![](_static/readme_fig.png)

## Installation

`conda install -c conda-forge pyiron_workflow`

To unlock the associated node packages and ensure that the demo notebooks run, also make sure your conda environment has the packages listed in our [notebooks dependencies](../.ci_support/environment-notebooks.yml)

## Learning more

Check out the demo [notebooks](../notebooks), read through the docstrings, and don't be scared to raise an issue on this GitHub repo!