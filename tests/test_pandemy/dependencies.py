"""This module contains information about the dependencies of pandemy.

E.g. versions of packages, which can be used to skip or run certain tests.
"""

# =================================================
# Imports
# =================================================

# Standard Library
from typing import Tuple

# Third Party
import pandas as pd
import sqlalchemy

# =================================================
# Constants
# =================================================

# Versions of used packages in a comparable form
PANDAS_VERSION: Tuple[int, int, int] = tuple([int(x) for x in pd.__version__.split('.')])
SQLALCHEMY_VERSION: Tuple[int, int, int] = tuple([int(x) for x in sqlalchemy.__version__.split('.')])
