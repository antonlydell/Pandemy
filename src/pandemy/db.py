"""
Contains the classes that represent the Database interface. 
"""

# ===============================================================
# Imports
# ===============================================================

# Standard Library
import logging
from abc import ABC, abstractmethod
from typing import Tuple, List, Optional, Union

# Third Party
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.sql import text
from sqlalchemy.exc import ArgumentError

# ===============================================================
# Set Logger
# ===============================================================

# Initiate the module logger
# Handlers and formatters will be inherited from the root logger
logger = logging.getLogger(__name__)

# ===============================================================
# Classes
# ===============================================================

class DatabaseManager(ABC):
    """
    Base class that the specific database types will inherit from.
    """

    @abstractmethod
    def __init__(self, **kwargs) -> None:
        """
        The initialize should support **kwargs because different types of databases
        will need different input parameters.
        """
    @abstractmethod
    def __repr__(self) -> str:
        """ Debug output."""

    @abstractmethod
    def __str__(self) -> str:
        """ String representation of the object."""