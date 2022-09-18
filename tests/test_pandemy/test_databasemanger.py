r"""Tests for the DatabaseManager base class."""

# =================================================
# Imports
# =================================================

# Standard Library

# Third Party
import pytest

# Local
import pandemy


# =================================================
# Tests
# =================================================


class TestValidateChunksize:
    r"""Test the static method `_validate_chunksize`.

    The method validates that the input to the `chunksize` parameter is
    is an integer > 0 or None.

    The `chunksize` parameter is used in several methods of the DatabaseManager.
    """

    @pytest.mark.parametrize(
        'chunksize',
        (
            pytest.param(1, id='chunksize=1'),
            pytest.param(10, id='chunksize=10')
        )
    )
    def test_valid_chunksizes(self, chunksize: int):
        r"""Test valid values of `chunksize`. 

        The method should return None and no exceptions should be raised.
        """

        # Setup - None
        # ===========================================================

        # Exercise
        # ===========================================================
        result = pandemy.DatabaseManager._validate_chunksize(chunksize=chunksize)

        # Verify
        # ===========================================================
        assert result is None

        # Clean up - None
        # ===========================================================

    @pytest.mark.raises
    @pytest.mark.parametrize(
        'chunksize',
        (
            pytest.param('2', id='chunksize=str'),
            pytest.param([3], id='chunksize=[3]'),
            pytest.param(0, id='chunksize=0'),
            pytest.param(-3, id='chunksize=-3')
        )
    )
    def test_invalidchunksizes(self, chunksize: int):
        r"""Test invalid values of `chunksize`.

        pandemy.InvalidInputError is expected to be raised.
        """

        # Setup - None
        # ===========================================================

        # Exercise
        # ===========================================================
        with pytest.raises(pandemy.InvalidInputError) as exc_info:
            pandemy.DatabaseManager._validate_chunksize(chunksize=chunksize)

        # Verify
        # ===========================================================
        exc_info.value.data == chunksize

        # Clean up - None
        # ===========================================================
