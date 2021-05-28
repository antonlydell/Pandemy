"""
Tests for the db module of the pandemy package.
"""

# =================================================
# Imports
# =================================================

# Standard Library
from typing import Any, Callable, Iterable, List, Optional, Union

# Third Party
import pytest
import pandas as pd
from pandas.testing import assert_frame_equal, assert_index_equal

# Local
import pandemy

# =================================================
# Test setup
# =================================================


class DbStatement(pandemy.DbStatement):
    """
    Container of database statements.

    Each SQL-dialect will subclass from DbStatement and implement
    the statements annotated here. If a subclass has not implemented
    the statement correctly the an error will be triggerd by the validation in
    `__init_subclass__`.
    """

    select_from_storesupply: str
    nr_queries: int

    def __init_subclass__(cls):
        """
        Check that all required SQL-queries are implemented
        in subclasses created from this class.
        """

        # Validate that subclasses of DbStatement have implemented the annotated class variables 
        pandemy.DbStatement.validate_class_variables(cls=cls, parent_cls=DbStatement,
                                                     type_validation='isinstance')


# =================================================
# Tests
# =================================================

# Input to test_Placeholder
input_test_Placeholder = [
    pytest.param(pandemy.Placeholder(':key', 'value'), ':key', 'value', True, 
                 id='new_key default True'),
    pytest.param(pandemy.Placeholder(':key', ['value0', 'value1'], False), ':key', ['value0', 'value1'], False,
                 id='new_key=False')
        ]

@pytest.mark.parametrize('placeholder, ex_key, ex_values, ex_new_key', input_test_Placeholder) 
def test_Placeholder(placeholder: pandemy.Placeholder, ex_key: str, 
                     ex_values: Union[str, List], ex_new_key: str) -> None:
    """
    Test the creation of the namedtuple pandemy.Placeholder.

    Parameters
    ----------

    placeholder: pandemy.Placeholder
        The placeholder instance.

    ex_key: str
        The expected value of the key attribute of placeholder.

    ex_values: str or list of str
        The expected value of the values attribute of placeholder.

    ex_new_key: bool
        The expected value of the new_key attribute of placeholder.
    """

    # Setup - None
    # =========================================

    # Exercise & Verify
    # =========================================
    assert placeholder.key == ex_key
    assert placeholder.values == ex_values
    assert placeholder.new_key is ex_new_key

    # Clean up - None
    # =========================================


class TestDbStatement:
    """
    Test the class `pandemy.DbStatement`.
    """

    # Statement to in which to replace palceholders
    stmt1 = """ SELECT * FROM StoreSupply
        WHERE ItemId IN (:ItemIds)
        ORDER BY :ItemName;
    """

    # The expected result after replacing placedholders in stmt1
    stmt1_exp = """ SELECT * FROM StoreSupply
        WHERE ItemId IN (:v0, :v1)
        ORDER BY ItemName DESC;
    """

    # The expected result after replacing placedholders in stmt1
    stmt1b_exp = """ SELECT * FROM StoreSupply
        WHERE ItemId IN (:v0, :v1)
        ORDER BY :v2;
    """

    # Statement to in which to replace palceholders
    stmt2 = """ SELECT * FROM StoreSupply
        WHERE ItemId IN (:ItemIds) AND
              ItemName LIKE :ItemNameLike AND
              Price > :price
        ORDER BY :ItemNameOrderBy
        LIMIT :limit;
    """

    # The expected result after replacing placedholders in stmt1
    stmt2_exp = """ SELECT * FROM StoreSupply
        WHERE ItemId IN (:v0, :v1, :v2) AND
              ItemName LIKE :v3 AND
              Price > :v4
        ORDER BY ItemName ASC, Price DESC
        LIMIT :v5;
    """

    # stmt, stmt_exp, placeholders, params_exp
    input_replace_placeholders = [
        pytest.param(stmt1, stmt1_exp,
                     [pandemy.Placeholder(key=':ItemIds', values=['100', '101'], new_key=True),
                      pandemy.Placeholder(key=':ItemName', values='ItemName DESC', new_key=False)
                      ],
                     {'v0': '100', 'v1': '101'},
                     id='Replace with iterable and string. new_key=[True, False]'),

        pytest.param(stmt1, stmt1b_exp,
                     [pandemy.Placeholder(key=':ItemIds', values=['100', '101'], new_key=True),
                      pandemy.Placeholder(key=':ItemName', values='ItemName DESC', new_key=True)
                      ],
                     {'v0': '100', 'v1': '101', 'v2': 'ItemName DESC'},
                     id='Replace with iterable and string. new_key=[True, True]'),

        pytest.param(stmt2, stmt2_exp,
                     [pandemy.Placeholder(key=':ItemIds', values=['100', '101', '102'], new_key=True),
                      pandemy.Placeholder(key=':ItemNameLike', values='Sta%', new_key=True),
                      pandemy.Placeholder(key=':price', values=18.74, new_key=True),
                      pandemy.Placeholder(key=':ItemNameOrderBy', values='ItemName ASC, Price DESC', new_key=False),
                      pandemy.Placeholder(key=':limit', values=10, new_key=True),
                      ],
                     {'v0': '100', 'v1': '101', 'v2': '102', 'v3': 'Sta%', 'v4': 18.74, 'v5': 10},
                     id='Replace in stmt2'),
    ]
    @pytest.mark.parametrize('stmt, stmt_exp, placeholders, params_exp', input_replace_placeholders)
    def test_replace_placeholders(self, stmt: str, stmt_exp: str, placeholders: dict, params_exp: dict) -> None:
        """
        Given an SQL statement replace sepecified placeholders
        with specified values.

        If a tuple is passed as the replacement value the first value is the
        replacement value and the second is an integer with the number of times
        the replacement is repeated.

        Parameters
        ----------
        """

        # Setup - None
        # =========================================

        # Exercise
        # =========================================
        stmt_result, params = pandemy.DbStatement.replace_placeholders(stmt=stmt, placeholders=placeholders)

        # Verify
        # =========================================
        assert stmt_result == stmt_exp
        assert params == params_exp

        # Clean up - None
        # =========================================


    # Input to test_create_DbQuery_subclass
    input_test_create_DbStatement_subclass = [
        pytest.param(None, id='No Error'),
        pytest.param(NotImplementedError, id='NotImplementedError'),
        pytest.param(TypeError, id='TypeError')
    ]

    @pytest.mark.parametrize('exception', input_test_create_DbStatement_subclass)
    def test_create_DbStatement_subclass(self, exception: Optional[Exception]):
        """
        Create a class that inherits from DbStatement and fails to implement
        the required statements correctly.

        Dbstatement inherits from `pandemy.DbStatement`

        Parameters
        ----------

        exception: Exception or None
            The exception to check for.
            If None no exception is expected to be raised.
        """

        # Setup - None
        # =========================================

        # Exercise & Verify
        # =========================================

        # No errors expected
        if exception is None:
            class SQLiteDbStatement(DbStatement):
                """All required statements are implemented."""

                select_from_storesupply = 'SELECT * FROM StoreSupply;'
                nr_queries = 1

        # Missing implementation of nr_queries
        elif issubclass(exception, NotImplementedError):
            with pytest.raises(exception, match='Class SQLiteDbStatement has not implemented the requried variable: nr_queries'):
                class SQLiteDbStatement(DbStatement):
                    """Not implemented nr_queries"""

                    select_from_storesupply = 'SELECT * FROM StoreSupply;'

        # nr_queries is of type string and not int
        elif issubclass(exception, TypeError):
            with pytest.raises(exception):
                class SQLiteDbStatement(DbStatement):
                    """nr_queries is a string and not int"""

                    select_from_storesupply = 'SELECT * FROM StoreSupply;'
                    nr_queries = '1'

        # Clean up - None
        # =========================================
