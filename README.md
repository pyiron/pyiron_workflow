# pyiron_module_template

## Overview

This repository is a template for new pyiron modules similar to the existing modules of the 
pyiron framework, e.g. 
[pyiron_base](https://github.com/pyiron/pyiron_base),
[pyiron_atomistics](https://github.com/pyiron/pyiron_atomistics),
and 
[pyiron_contrib](https://github.com/pyiron/pyiron_contrib).

Within this repository, the new module is called `pyiron_module_template` which should be renamed to `pyiron_IntendedModuleName`. 
This can be easily achieved by modifying and running the update_module_name.sh script. 

The licence is free to choose, but as a default the BSD3 licence packed here.

## Continuous Integration

We collect all files relevant for the continuous integration (CI) pipelines in `.ci_support`, 
while the actual CI workflows are handled by GitHub and stored in `.github`.
If you are cloning this template *inside* the pyiron GitHub organization, the full CI should work out-of-the-box by calling reusable workflows from [pyiron/actions](github.com/pyiron/actions) and inheriting organization-wide secrets.
Otherwise, you will either need to modify the CI workflow files, or give your repository the following secrets:
- `DEPENDABOT_WORKFLOW_TOKEN` (GitHub token for an account that has permissions to your repository -- needs to differ from the default `github_token` already available though! In pyiron we have a special [@pyiron_runner account](https://github.com/pyiron-runner) for this purpose.)
- `PYPI_PASSWORD` (Token generated on PyPi to give access to your account there)
- `CODACY_PROJECT_TOKEN` (Token generated on Codacy to give access to your account there)

The default CI setup from [pyiron/actions](github.com/pyiron/actions) makes some assumptions about your directory structure.
The most important one is that your environment should be specified in `.ci_support/environment.yml`.
There is a base environment there already, giving dependence on `pyiron_base`.
The CI will automatically keep environment files read by readthedocs (which will look at `.readthedocs.yml`) and MyBinder (which looks in `.binder`) up-to-date based on this environment file.

In case you need extra environment files for some setups, you can modify the workflows in `.github/workflows`, which accept input variables for the docs, tests, and notebooks environments.
For example, it's typically good to not make your project depend on the `lammps` package, since this is not available for windows.
However, you might want to give some demo notebooks that run on MyBinder (a linux environment) and use LAMMPS calculations.
In this case, you could add a new file `.ci_support/environment-notebooks.yml`, and then edit `.github/workflows/push-pull-main.yml` so that instead of reading 

```yaml
jobs:
  pyiron:
    uses: pyiron/actions/.github/workflows/push-pull-main.yml@main
    secrets: inherit
    # All the environment files variables point to .ci_support/environment.yml by default
```

It instead reads

```yaml
jobs:
  pyiron:
    uses: pyiron/actions/.github/workflows/push-pull-main.yml@main
    secrets: inherit
    with:
      notebooks-env-files: .ci_support/environment.yml .ci_support/environment-notebooks.yml
```

Where `.ci_support/environment-notebooks.yml` looks like:

```yaml
channels:
  - conda-forge
dependencies:
  - lammps
```

### Label-based CI

Some CI triggers when labels get applied to a PR. 
In a new repository, you will need to define these labels:
- `format_black`: Runs black analyis and creates a bot-generated commit to fix any format violations
- `run_CodeQL`: Runs the external CodeQL analysis (expensive, only do at the end)
- `run_coverage`: Run all the tests in `tests` and use coveralls to generate a coverage report (also expensive, only run near the end of your PR)

## Documentation

You should modify this README to reflect the purpose of your new package.
You can look at the other pyiron modules to get a hint for what sort of information to include, and how to link badges at the head of your README file.

At a minimum, we suggest creating a meaningful example notebook in the `notebooks/` directory and creating a MyBinder badge so that people can quickly and easily explore your work.

You can also edit the docs for your package by modifying `docs/index.rst`.
By default, only a simple API section is included.

## Publishing your package

If you are inside the pyiron organization or have your own `PYPI_PASSWORD` secret configured, your package will be published on PyPI automatically when you make a new "release" on GitHub -- *as long as* that tag matches the pattern specified in `setup.cfg`; by default any tag that `pyiron_module_template-`, where `pyiron_module_template` is replaced with the name of your module. We recommend using semantic versioning so that your first release looks like `pyiron_module_template-0.0.1`.

Releasing your package on Conda-Forge is slightly more involved, but not too hard (at least for pure python packages).
See [conda-forge/staged-recipes](https://github.com/conda-forge/staged-recipes) for how to publish it there.
