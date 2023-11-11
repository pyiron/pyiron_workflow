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

This repository is home to the pyiron code for structuring workflows as graph objects, with different computational elements as nodes and data and execution signals travelling along edges. It is currently in an alpha state, changing quickly, and not yet feature-complete.

## The absolute basics

`pyiron_workflow` offers a single-point-of-entry in the form of the `Workflow` object, and uses decorators to make it easy to turn regular python functions into "nodes" that can be put in a computation graph:

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