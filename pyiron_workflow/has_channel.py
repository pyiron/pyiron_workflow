# coding: utf-8
# Copyright (c) Max-Planck-Institut für Eisenforschung GmbH - Computational Materials Design (CM) Department
# Distributed under the terms of "New BSD License", see the LICENSE file.

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyiron_workflow.channels import Channel


class HasChannel(ABC):
    """
    A mix-in class for use with the `Channel` class.
    A `Channel` is able to (attempt to) connect to any child instance of `HasConnection`
    by looking at its `connection` attribute.

    This is useful for letting channels attempt to connect to non-channel objects
    directly by pointing them to some channel that object holds.
    """

    @property
    @abstractmethod
    def channel(self) -> Channel:
        pass
