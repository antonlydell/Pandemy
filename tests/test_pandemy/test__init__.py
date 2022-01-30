r"""Tests for the __init__ module of Pandemy."""

# =================================================
# Imports
# =================================================

# Standard Library
from datetime import date
import re

# Local
import pandemy

# =================================================
# Tests
# =================================================


class TestVersion:
    r"""Tests for the `__versiontuple__` and `__version__` attributes of Pandemy.

    Attributes
    ----------
    __versiontuple__ : Tuple[Union[int, str], ...]
        Describes the version of Pandemy in semantic versioning (MAJOR.MINOR.PATCH).

    __version__ : str
        The version of Pandemy represented as a string.
        `__version__` is derived from `__versiontuple__`.
    """

    def test__versiontuple__exists(self):
        r"""Check that the `__versiontuple__` attribute exists.

        If `__versiontuple__` does not exist AttributeError is raised.
        """

        # Setup - None
        # ===========================================================

        # Exercise & Verify
        # ===========================================================
        getattr(pandemy, '__versiontuple__')

        # Clean up - None
        # ===========================================================

    def test__versiontuple__is_int_or_str(self):
        r"""Check that the values of `__versiontuple__` are of type int or str."""

        # Setup - None
        # ===========================================================

        # Exercise & Verify
        # ===========================================================
        assert all(
            isinstance(x, int) or isinstance(x, str)
            for x in pandemy.__versiontuple__
        ), 'Not all values of __versiontuple__ are of type int or str'

        # Clean up - None
        # ===========================================================

    def test__version__exists(self):
        r"""Check that the `__version__` attribute exists.

        If `__version__` does not exist AttributeError is raised.
        """

        # Setup - None
        # ===========================================================

        # Exercise & Verify
        # ===========================================================
        getattr(pandemy, '__version__')

        # Clean up - None
        # ===========================================================

    def test__version__is_str(self):
        r"""Check that the `__version__` attribute is of type string."""

        # Setup - None
        # ===========================================================

        # Exercise & Verify
        # ===========================================================
        assert isinstance(pandemy.__version__, str)

        # Clean up - None
        # ===========================================================

    def test__version__format(self):
        r"""Check that the `__version__` attribute has the correct format.

        The version specifiers (MAJOR.MINOR.PATCH) are separated by a ".".
        The number of "." in the `__version__` string should be one less
        than the length of `__versiontuple__`.
        """

        # Setup - None
        # ===========================================================

        # Exercise
        # ===========================================================
        count = len(re.findall(r'\.', pandemy.__version__))

        # Verify
        # ===========================================================
        assert count == len(pandemy.__versiontuple__) - 1

        # Clean up - None
        # ===========================================================


class TestReleaseDate:
    r"""Tests for the `__releasedate__` attribute of Pandemy.

    Attributes
    ----------
    __releasedate__ : datetime.date
        The date when the version specified in `__versiontuple__` was released.
    """

    def test_is_date(self):
        r"""Check that the `__releasedate__` attribute is of type datetime.date"""

        # Setup - None
        # ===========================================================

        # Exercise & Verify
        # ===========================================================
        assert isinstance(pandemy.__releasedate__, date)

        # Clean up - None
        # ===========================================================

    def test_has_valid_release_year(self):
        r"""The first release of Pandemy (version 0.0.1) took place in 2021.

        Test that the release year >= 2021.
        """

        # Setup - None
        # ===========================================================

        # Exercise & Verify
        # ===========================================================
        assert pandemy.__releasedate__.year >= 2021

        # Clean up - None
        # ===========================================================
