##########################################################################################
# Copyright (c) MemSQL. All rights reserved.
# Licensed under the MIT License. See LICENSE in the project root for license information.
##########################################################################################

"""
nemesis.py

fault injector interface

"""

from abc import ABC, abstractmethod
from typing import List

import coloredlogs  # type: ignore
import logging
import random
import shlex
import subprocess
import time


class Nemesis(ABC):
    """
    Interface for nemesis, a plug-in fault injector for frodo

    A user can specify one nemesis instance to disturb the setting (eg: introducing clock skew, etc.)

    It needs a function to introduce faults and another to heal
      - the fault injection function is ran in a loop by the test generator, so it internally should have no unbounded loops
      - the healing function should make the system ready to be correctly inspected
    """

    def __init__(self) -> None:
        pass

    @abstractmethod
    def inject(self) -> None:
        pass

    @abstractmethod
    def heal(self) -> None:
        pass
