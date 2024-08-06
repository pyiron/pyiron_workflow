from __future__ import annotations

"""
Shared code for various node-creating decorators facilitating multiple dispatch (i.e.
using decorators with or without arguments, contextually.
"""


class MultipleDispatchError(ValueError):
    """
    Raise from callables using multiple dispatch when no interpretation of input
    matches an expected case.
    """


def dispatch_output_labels(single_dispatch_decorator):
    def multi_dispatch_decorator(*output_labels, **kwargs):
        if len(output_labels) > 0 and callable(output_labels[0]):
            if len(output_labels) > 1:
                raise MultipleDispatchError(
                    f"Output labels must all be strings (for decorator usage with an "
                    f"argument), or a callable must be provided alone -- got "
                    f"{output_labels}."
                )
            return single_dispatch_decorator(**kwargs)(output_labels[0])
        else:
            return single_dispatch_decorator(*output_labels, **kwargs)

    return multi_dispatch_decorator
