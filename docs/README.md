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

[![Decision](https://img.shields.io/badge/pyiron-25--001_v2-blue)](https://github.com/pyiron/decisions/blob/main/decisions/25-001-split-api.md)

## Overview

`pyiron_workflow` is a workflow management system (WfMS) built on top of the [`flowrep`]() data paradigm, for transforming prospective recipes into retrospective data results.
It offers tools for allocating resources to node computation, an event system for checkpointing and for GUIs to grab onto, a system for dynamically and interactively building a workflow, and more.