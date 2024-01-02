# pyiron_workflow

[![Binder](https://mybinder.org/badge_logo.svg)](https://mybinder.org/v2/gh/pyiron/pyiron_workflow/HEAD)
[![License](https://img.shields.io/badge/License-BSD_3--Clause-blue.svg)](https://opensource.org/licenses/BSD-3-Clause)
[![Codacy Badge](https://app.codacy.com/project/badge/Grade/0b4c75adf30744a29de88b5959246882)](https://app.codacy.com/gh/pyiron/pyiron_workflow/dashboard?utm_source=gh&utm_medium=referral&utm_content=&utm_campaign=Badge_grade)
[![Coverage Status](https://coveralls.io/repos/github/pyiron/pyiron_workflow/badge.svg?branch=main)](https://coveralls.io/github/pyiron/pyiron_workflow?branch=main)

[//]: # ([![Documentation Status]&#40;https://readthedocs.org/projects/pyiron-workflow/badge/?version=latest&#41;]&#40;https://pyiron-workflow.readthedocs.io/en/latest/?badge=latest&#41;)

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

Nodes (including macros) can be stored in plain text, and registered by future workflows for easy access. This encourages and supports an ecosystem of useful nodes, so you don't need to re-invent the wheel. (This is an alpha-feature, with full support of [FAIR](https://en.wikipedia.org/wiki/FAIR_data) principles for node packages planned.)

## Example

`pyiron_workflow` offers a single-point-of-entry in the form of the `Workflow` object, and uses decorators to make it easy to turn regular python functions into "nodes" that can be put in a computation graph.

Nodes can be used by themselves and -- other than being "delayed" in that their computation needs to be requested after they're instantiated -- they feel an awful lot like the regular python functions they wrap:

```python
>>> from pyiron_workflow import Workflow
>>>
>>> @Workflow.wrap_as.single_value_node()
... def add_one(x):
...     return x + 1
>>>
>>> add_one(add_one(add_one(x=0)))()
3

```

But the intent is to collect them together into a workflow and leverage existing nodes. We can directly perform (many but not quite all) python actions natively on output channels, can build up data graph topology by simply assigning values (to attributes or at instantiation), and can package things together into reusable macros with customizable IO interfaces:

```python
>>> from pyiron_workflow import Workflow
>>> Workflow.register("plotting", "pyiron_workflow.node_library.plotting")
>>>
>>> @Workflow.wrap_as.single_value_node()
... def Arange(n: int):
...     import numpy as np
...     return np.arange(n)
>>>
>>> @Workflow.wrap_as.macro_node("fig")
... def PlotShiftedSquare(macro, shift: int = 0):
...     macro.arange = Arange()
...     macro.plot = macro.create.plotting.Scatter(
...         x=macro.arange + shift,
...         y=macro.arange**2
...     )
...     macro.inputs_map = {"arange__n": "n"}  # Expose arange input
...     return macro.plot
>>> 
>>> wf = Workflow("plot_with_and_without_shift")
>>> wf.n = wf.create.standard.UserInput()
>>> wf.no_shift = PlotShiftedSquare(shift=0, n=10)
>>> wf.shift = PlotShiftedSquare(shift=2, n=10)
>>> wf.inputs_map = {
...     "n__user_input": "n",
...     "shift__shift": "shift"
... }
>>> 
>>> diagram = wf.draw()
>>> 
>>> out = wf(shift=3, n=10)

```

Which gives the workflow `diagram`

![](docs/_static/readme_diagram.png)

And the resulting figure (when axes are not cleared)

![](docs/_static/readme_fig.png)

## Installation

`conda install -c conda-forge pyiron_workflow`

To unlock the associated node packages and ensure that the demo notebooks run, also make sure your conda environment has the packages listed in our [notebooks dependencies](.ci_support/environment-notebooks.yml)

## Learning more

Check out the demo [notebooks](notebooks), read through the docstrings, and don't be scared to raise an issue on this GitHub repo!