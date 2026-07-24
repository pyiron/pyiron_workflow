# pyiron_workflow

[![Binder](https://mybinder.org/badge_logo.svg)](https://mybinder.org/v2/gh/pyiron/pyiron_workflow/HEAD)
[![License](https://img.shields.io/badge/License-BSD_3--Clause-blue.svg)](https://opensource.org/licenses/BSD-3-Clause)
[![Coverage](https://codecov.io/gh/pyiron/pyiron_workflow/graph/badge.svg)](https://codecov.io/gh/pyiron/pyiron_workflow)
[![Documentation](https://readthedocs.org/projects/pyiron-workflow/badge/?version=latest)](https://pyiron-workflow.readthedocs.io/en/latest/?badge=latest)

[![DOI](https://zenodo.org/badge/DOI/10.5281/zenodo.19114207.svg)](https://doi.org/10.5281/zenodo.19114207)
[![Anaconda](https://anaconda.org/conda-forge/pyiron_workflow/badges/version.svg)](https://anaconda.org/conda-forge/pyiron_workflow)
[![Last Updated](https://anaconda.org/conda-forge/pyiron_workflow/badges/latest_release_date.svg
)](https://anaconda.org/conda-forge/pyiron_workflow)
[![Platform](https://anaconda.org/conda-forge/pyiron_workflow/badges/platforms.svg)](https://anaconda.org/conda-forge/pyiron_workflow)
[![Downloads](https://anaconda.org/conda-forge/pyiron_workflow/badges/downloads.svg)](https://anaconda.org/conda-forge/pyiron_workflow)


## pyiron_workflow

A workflow management system (WfMS) built on top of the [flowrep](https://github.com/pyiron/flowrep/) data paradigm, for transforming prospective recipes into retrospective data results.

## Features

- **_Clear provenance_** coupling `flowrep`'s recipes to resulting data, augmented with practical metadata for each run
- **_Easy, dynamic workflow construction_** with the core `Workflow` object
- **_Type and ontological validation_** of recipes using [semantikon](https://github.com/pyiron/semantikon) for ontological analysis
- **_Scale compute_** by applying python executors to any node in the workflow, including HPC SLURM-allocated resources leveraging [executorlib](https://github.com/pyiron/executorlib)
- **_Monitor progress_** with a flexible event hook system, useful for dumping checkpoints or state-at-exception for failed runs, or for GUIs to grab onto for visually monitoring what has been executed so far

For an introduction and full tour of available features, check out the [user guide notebook](../notebooks/user_guide.ipynb)

## Installation

`conda install -c conda-forge pyiron_workflow`

See the [pyproject file](../pyproject.toml) for optional dependencies to unlock extra functionality.

## Compatibility

Versions of `pyiron_workflow` <0.17.0 pre-date `flowrep`. In `pwf.compatibility`, we provide new import locations for the old `@as_function_node` and `@as_macro_node` decorators to help with the migration process. See the [compatiblity notebook](../notebooks/compatibility.ipynb) for more details.

## Citing

If you use `pyiron_workflow` in your research, please cite the [Zenodo DOI](https://doi.org/10.5281/zenodo.19114207)
