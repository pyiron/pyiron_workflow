"""
This is a working space to rebase the workflow management system (WfMS) onto `flowrep`
non-destructively.

It will live alongside the legacy WfMS in the remainder of the package while we
maximize syntactic agreement between the two approaches

The approach here differs fundamentally by using `flowrep` recipes as a ground truth,
maximizing the number of "workflow" functions which remain plain python functions
(in contrast to the legacy approach, where the `pyiron_workflow` decorators convert
decorated functions from plain functions to node creator callables.)
"""
