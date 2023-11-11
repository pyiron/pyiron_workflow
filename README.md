# pyiron_workflow

[![Binder](https://mybinder.org/badge_logo.svg)](https://mybinder.org/v2/gh/pyiron/pyiron_workflow/HEAD)
[![License](https://img.shields.io/badge/License-BSD_3--Clause-blue.svg)](https://opensource.org/licenses/BSD-3-Clause)
[![Codacy Badge](https://app.codacy.com/project/badge/Grade/0b4c75adf30744a29de88b5959246882)](https://app.codacy.com/gh/pyiron/pyiron_workflow/dashboard?utm_source=gh&utm_medium=referral&utm_content=&utm_campaign=Badge_grade)
[![Coverage Status](https://coveralls.io/repos/github/pyiron/pyiron_workflow/badge.svg?branch=main)](https://coveralls.io/github/pyiron/pyiron_workflow?branch=main)

[//]: # ([![Documentation Status]&#40;https://readthedocs.org/projects/pyiron_workflow/badge/?version=latest&#41;]&#40;https://pyiron_workflow.readthedocs.io/en/latest/&#41;)

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

Individual node computations can be shipped off to parallel processes for scalability. (This is an alpha-feature at time of writing and limited to single core parallel python processes; full support of [`pympipool`](https://github.com/pyiron/pympipool) is under active development)

Once you're happy with a workflow, it can be easily turned it into a macro for use in other workflows. This allows the clean construction of increasingly complex computation graphs by composing simpler graphs.

Nodes (including macros) can be stored in plain text, and registered by future workflows for easy access. This encourages and supports an ecosystem of useful nodes, so you don't need to re-invent the wheel. (This is an alpha-feature, with full support of [FAIR](https://en.wikipedia.org/wiki/FAIR_data) principles for node packages planned.)

## The absolute basics

`pyiron_workflow` offers a single-point-of-entry in the form of the `Workflow` object, and uses decorators to make it easy to turn regular python functions into "nodes" that can be put in a computation graph.

```python
from pyiron_workflow import Workflow

@Workflow.wrap_as.function_node("sum")
def x_plus_y(x: int = 0, y: int = 0) -> int:
    return x + y

wf = Workflow("my_workflow")
wf.a1 = x_plus_y()
wf.a2 = x_plus_y()
wf.b = x_plus_y(x=wf.a1.outputs.sum, y=wf.a2.outputs.sum)

out = wf(a1__x=0, a1__y=1, a2__x=2, a2__y=3)
out.b__sum
>>> 6

wf.draw()
```

![](docs/_static/demo.png)

## Installation

`conda install -c conda-forge pyiron_workflow`

To unlock the associated node packages and ensure that the demo notebooks run, also make sure your conda environment has the packages listed in our [notebooks dependencies](.ci_support/environment-notebooks.yml)

## Learning more

Check out the demo [notebooks](notebooks), read through the docstrings, and don't be scared to raise an issue on this GitHub repo!