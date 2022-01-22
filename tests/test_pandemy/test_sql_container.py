"""Tests for the sql_container module of the pandemy package."""

# =================================================
# Imports
# =================================================

# Standard Library
from typing import List, Optional, Union

# Third Party
import pytest

# Local
import pandemy

# =================================================
# Test setup
# =================================================


class SQLContainerIsinstance(pandemy.SQLContainer):
    r"""Container of database statements.

    Each SQL-dialect will subclass from SQLContainerIsinstance and implement
    the statements annotated here. If a subclass has not implemented
    the statements correctly an error will be triggered by the validation in
    __init_subclass__ special method.
    """

    select_from_storesupply: str
    nr_queries: int
    select_with_params: dict

    def __init_subclass__(cls):
        r"""
        Check that all required SQL statements are implemented
        in subclasses created from this class.
        """

        # Validate that subclasses of SQLContainer have implemented the annotated class variables
        pandemy.SQLContainer.validate_class_variables(cls=cls, parent_cls=SQLContainerIsinstance,
                                                      type_validation='isinstance')


class NotASQLContainer:
    r"""Not a SQLContainer of type pandemy.SQLContainer.

    Used for testing the validate_class_variables method of pandemy.SQLContainer
    when `type_validation` parameter is set to 'type'.
    """


class SQLContainerType(pandemy.SQLContainer):
    r"""Container of database statements.

    Each SQL-dialect will subclass from SQLContainerType and implement
    the statements annotated here. If a subclass has not implemented
    the statements correctly an error will be triggered by the validation in
    __init_subclass__ special method.
    """

    nr_queries: NotASQLContainer

    def __init_subclass__(cls):
        r"""
        Check that all required SQL statements are implemented
        in subclasses created from this class.
        """

        # Validate that subclasses of SQLContainer have implemented the annotated class variables
        pandemy.SQLContainer.validate_class_variables(cls=cls, parent_cls=SQLContainerType,
                                                      type_validation='type')


class SQLContainerTypeValidationError(pandemy.SQLContainer):
    r"""Container of database statements.

    Each SQL-dialect will subclass from SQLContainerTypeValidationError and implement
    the statements annotated here. If a subclass has not implemented
    the statements correctly an error will be triggered by the validation in
    __init_subclass__ special method.

    This class supplies an invalid value to the `type_validation` parameter
    in the validate_class_variables method. It should raise pandemy.InvalidInputError
    when a subclass is created from this class.
    """

    select_from_storesupply: str
    nr_queries: int

    def __init_subclass__(cls):
        r"""
        Check that all required SQL statements are implemented
        in subclasses created from this class.
        """

        # Validate that subclasses of SQLContainer have implemented the annotated class variables
        pandemy.SQLContainer.validate_class_variables(cls=cls, parent_cls=SQLContainerIsinstance,
                                                      type_validation='invalid')


# =================================================
# Tests
# =================================================

class TestPlaceholder:
    r"""Test the pandemy.Placeholder namedtuple.

    The pandemy.Placeholder namedtuple contains placeholders and their replacement values
    to use when building parametrized SQL statements. pandemy.Placeholder is used as input
    to the pandemy.SQLContainer.replace_placeholders method.
    """

    @pytest.mark.parametrize(
        'placeholder, ex_placeholder, ex_replacements, ex_return_new_placeholders',
        (
            pytest.param(
                pandemy.Placeholder(':placeholder', 'value'),
                ':placeholder', 'value', True,
                id='return_new_placeholders default True'
            ),

            pytest.param(
                pandemy.Placeholder(':placeholder', ['value0', 'value1'], False),
                ':placeholder', ['value0', 'value1'], False,
                id='return_new_placeholders=False'
            )
        )
    )
    def test_create_placeholder(self,
                                placeholder: pandemy.Placeholder,
                                ex_placeholder: str,
                                ex_replacements: Union[str, List],
                                ex_return_new_placeholders: str) -> None:
        r"""Test the creation of the namedtuple pandemy.Placeholder.

        Parameters
        ----------
        placeholder : pandemy.Placeholder
            The placeholder instance.

        ex_placeholder : str
            The expected value of the `placeholder` attribute of `placeholder`.

        ex_replacements : str or list of str
            The expected value of the `replacements` attribute of `placeholder`.

        ex_return_new_placeholders : bool
            The expected value of the `return_new_placeholders` attribute of `placeholder`.
        """

        # Setup - None
        # =========================================

        # Exercise & Verify
        # =========================================
        assert placeholder.placeholder == ex_placeholder
        assert placeholder.replacements == ex_replacements
        assert placeholder.return_new_placeholders is ex_return_new_placeholders

        # Clean up - None
        # =========================================


class TestSQLContainerReplacePlaceholders:
    r"""Test the method replace_placeholders of the class pandemy.SQLContainer."""

    # Statement to in which to replace placeholders
    stmt_1 = """SELECT * FROM StoreSupply
        WHERE ItemId = :ItemId;
    """

    # The expected result after replacing placeholders in stmt_1
    stmt_1_exp = """SELECT * FROM StoreSupply
        WHERE ItemId = 2;
    """

    # The expected result after replacing placeholders in stmt_1
    stmt_1b_exp = """SELECT * FROM StoreSupply
        WHERE ItemId = :v0;
    """

    # The expected result after replacing placeholders in stmt_1
    stmt_1c_exp = """SELECT * FROM StoreSupply
        WHERE ItemId = 1, 2;
    """

    # Statement in which to replace placeholders
    stmt_2 = """SELECT * FROM StoreSupply
        WHERE ItemId IN (:ItemIds)
        ORDER BY :ItemName;
    """

    # The expected result after replacing placeholders in stmt_2
    stmt_2_exp = """SELECT * FROM StoreSupply
        WHERE ItemId IN (:v0, :v1)
        ORDER BY ItemName DESC;
    """

    # The expected result after replacing placeholders in stmt_2
    stmt_2b_exp = """SELECT * FROM StoreSupply
        WHERE ItemId IN (:v0, :v1)
        ORDER BY :v2;
    """

    # Statement in which to replace placeholders
    stmt_3 = """SELECT * FROM StoreSupply
        WHERE ItemId IN (:ItemIds) AND
              ItemName LIKE :ItemNameLike AND
              Price > :price
        ORDER BY :ItemNameOrderBy
        LIMIT :limit;
    """

    # The expected result after replacing placeholders in stmt_3
    stmt_3_exp = """SELECT * FROM StoreSupply
        WHERE ItemId IN (:v0, :v1, :v2) AND
              ItemName LIKE :v3 AND
              Price > :v4
        ORDER BY ItemName ASC, Price DESC
        LIMIT :v5;
    """

    @pytest.mark.parametrize(
        'stmt, stmt_exp, placeholders, params_exp',
        (
            pytest.param(
                stmt_1, stmt_1_exp,
                pandemy.Placeholder(placeholder=':ItemId',
                                    replacements=2,
                                    return_new_placeholders=False),
                {},
                id='Single Placeholder instance. return_new_placeholders=False'
            ),

            pytest.param(
                stmt_1, stmt_1b_exp,
                pandemy.Placeholder(placeholder=':ItemId',
                                    replacements=2,
                                    return_new_placeholders=True),
                {'v0': 2},
                id='Single Placeholder instance. return_new_placeholders=True'
            ),

            pytest.param(
                stmt_1, stmt_1c_exp,
                pandemy.Placeholder(placeholder=':ItemId',
                                    replacements=[1, '2'],
                                    return_new_placeholders=False),
                {},
                id='Single Placeholder instance, sequence. return_new_placeholders=False'
            ),

            pytest.param(
                stmt_2, stmt_2_exp,
                [
                    pandemy.Placeholder(placeholder=':ItemIds',
                                        replacements=['100', '101'],
                                        return_new_placeholders=True),

                    pandemy.Placeholder(placeholder=':ItemName',
                                        replacements='ItemName DESC',
                                        return_new_placeholders=False)
                ],
                {'v0': '100', 'v1': '101'},
                id='Replace with sequence and string. return_new_placeholders=[True, False]'
            ),

            pytest.param(
                stmt_2, stmt_2b_exp,
                [
                    pandemy.Placeholder(placeholder=':ItemIds',
                                        replacements=['100', '101'],
                                        return_new_placeholders=True),

                    pandemy.Placeholder(placeholder=':ItemName',
                                        replacements='ItemName DESC',
                                        return_new_placeholders=True)
                ],
                {'v0': '100', 'v1': '101', 'v2': 'ItemName DESC'},
                id='Replace with sequence and string. return_new_placeholders=[True, True]'
            ),

            pytest.param(
                stmt_3, stmt_3_exp,
                [
                    pandemy.Placeholder(placeholder=':ItemIds',
                                        replacements=['100', '101', '102'],
                                        return_new_placeholders=True),

                    pandemy.Placeholder(placeholder=':ItemNameLike',
                                        replacements='Sta%',
                                        return_new_placeholders=True),

                    pandemy.Placeholder(placeholder=':price',
                                        replacements=18.74,
                                        return_new_placeholders=True),

                    pandemy.Placeholder(placeholder=':ItemNameOrderBy',
                                        replacements='ItemName ASC, Price DESC',
                                        return_new_placeholders=False),

                    pandemy.Placeholder(placeholder=':limit',
                                        replacements=10,
                                        return_new_placeholders=True)
                ],
                {'v0': '100', 'v1': '101', 'v2': '102', 'v3': 'Sta%', 'v4': 18.74, 'v5': 10},
                id='Replace many placeholders in stmt_3'
            )
        )
    )
    def test_replace_placeholders(self, stmt: str, stmt_exp: str, placeholders: dict, params_exp: dict) -> None:
        r"""Given a SQL statement replace specified placeholders with specified replacement values.

        Parameters
        ----------
        stmt : str
            The SQL statement in which to replace placeholders.

        stmt_exp : str
            The expected result of the SQL statement after placeholders have been replaced.

        placeholders : pandemy.Placeholder or sequence of pandemy.Placeholder
            The placeholders and their replacement values.

        params_exp : dict
            The expected new placeholders in the SQL query and their mapped values.
        """

        # Setup - None
        # =========================================

        # Exercise
        # =========================================
        stmt_result, params = pandemy.SQLContainer.replace_placeholders(stmt=stmt, placeholders=placeholders)

        # Verify
        # =========================================
        assert stmt_result == stmt_exp
        assert params == params_exp

        # Clean up - None
        # =========================================

    @pytest.mark.raises
    @pytest.mark.parametrize(
        'placeholders',
        (
            pytest.param(
                pandemy.Placeholder(placeholder=':ItemId',
                                    replacements=pandemy.SQLContainer,
                                    return_new_placeholders=False),
                id='replacements=pandemy.SQLContainer'
            ),

            pytest.param(
                pandemy.Placeholder(placeholder=':ItemId',
                                    replacements=[pandemy.SQLContainer],
                                    return_new_placeholders=False),
                id='replacements=[pandemy.SQLContainer]'
            )

        )
    )
    def test_replace_placeholders_invalid_input(self, placeholders: dict) -> None:
        r"""Given a SQL statement replace specified placeholders with specified replacement values.

        The Placeholder instance contains invalid replacement values.
        pandemy.InvalidInputError is expected to be raised.

        Parameters
        ----------
        placeholders : pandemy.Placeholder or sequence of pandemy.Placeholder
            The placeholders and their replacement values.
        """

        # Setup
        # =========================================
        stmt = """SELECT * FROM StoreSupply WHERE ItemId = :ItemId;"""

        # Exercise & Verify
        # =========================================
        with pytest.raises(pandemy.InvalidInputError):
            pandemy.SQLContainer.replace_placeholders(stmt=stmt, placeholders=placeholders)

        # Clean up - None
        # =========================================


class TestSQLContainerInitSubclass:
    r"""Test the creation of subclasses from a class inheriting from pandemy.SQLContainer.

    Tests the use of the validate_class_variables method that is used within the
    special method __init_subclass__.
    """

    @pytest.mark.parametrize('exception', [pytest.param(None, id='No Error'),
                                           pytest.param(NotImplementedError, id='NotImplementedError'),
                                           pytest.param(TypeError, id='TypeError')])
    def test_create_SQLContainer_subclass_isinstance(self, exception: Optional[Exception]):
        r"""
        Create a class that inherits from SQLContainerIsinstance and fails to implement
        the required statements correctly.

        SQLContainerIsinstance inherits from pandemy.SQLContainer and
        uses the parameter `type_validation` = 'isinstance' in the method validate_class_variables.

        Parameters
        ----------
        exception : Exception or None
            The exception to check for.
            If None no exception is expected to be raised.
        """

        # Setup - None
        # =========================================

        # Exercise & Verify
        # =========================================

        # No errors expected
        if exception is None:
            class SQLiteSQLContainer(SQLContainerIsinstance):
                r"""All required statements are implemented."""

                select_from_storesupply = 'SELECT * FROM StoreSupply;'

                nr_queries = 1

                select_with_params = {'sql': 'SELECT * FROM StoreSupply WHERE StoreId = :StoreId;',
                                      'params': {'StoreId': 2}
                                      }

        # Missing implementation of select_with_params
        elif issubclass(exception, NotImplementedError):
            with pytest.raises(exception, match='select_with_params'):
                class SQLiteSQLContainer(SQLContainerIsinstance):
                    r"""Not implemented select_with_params"""

                    select_from_storesupply = 'SELECT * FROM StoreSupply;'

                    nr_queries = 1

        # nr_queries is of type string and not int
        elif issubclass(exception, TypeError):
            with pytest.raises(exception, match='nr_queries'):
                class SQLiteSQLContainer(SQLContainerIsinstance):
                    r"""nr_queries is a string and not int"""

                    select_from_storesupply = 'SELECT * FROM StoreSupply;'

                    nr_queries = '1'

                    select_with_params = {'sql': 'SELECT * FROM StoreSupply WHERE StoreId = :StoreId;',
                                          'params': {'StoreId': 2}
                                          }

        # Clean up - None
        # =========================================

    @pytest.mark.parametrize('exception', [pytest.param(None, id='No Error'),
                                           pytest.param(TypeError, id='TypeError')])
    def test_create_SQLContainer_subclass_type(self, exception: Optional[Exception]):
        r"""
        Create a class that inherits from SQLContainer and fails to implement
        the required statements correctly.

        SQLContainerType inherits from pandemy.SQLContainer and
        uses the parameter `type_validation` = 'type' in the method validate_class_variables.

        Parameters
        ----------
        exception : Exception or None
            The exception to check for.
            If None no exception is expected to be raised.
        """

        # Setup - None
        # =========================================

        # Exercise & Verify
        # =========================================

        # No errors expected
        if exception is None:
            class SQLiteSQLContainer(SQLContainerType):
                """All required statements are implemented."""

                nr_queries = NotASQLContainer

        # Incorrect type of nr_queries
        elif issubclass(exception, TypeError):
            with pytest.raises(exception, match='NotASQLContainer'):
                class SQLiteSQLContainer(SQLContainerType):
                    """Incorrect type of nr_queries"""

                    nr_queries = 1

        # Clean up - None
        # =========================================

    @pytest.mark.raises
    def test_create_SQLContainer_subclass_invalid_type_validation(self):
        r"""Create a SQLContainer sublcass that inherits from SQLContainerTypeValidationError.

        pandemy.InvalidInputError is expected to be raised when the subclass is created
        due to invalid input to the `type_validation` parameter of the validate_class_variables method
        of SQLContainerTypeValidationError.
        """

        # Setup - None
        # =========================================

        # Exercise & Verify
        # =========================================

        with pytest.raises(pandemy.InvalidInputError, match='invalid'):
            class SQLiteSQLContainer(SQLContainerTypeValidationError):
                r"""type_validation = 'invalid'"""

                select_from_storesupply = 'SELECT * FROM StoreSupply;'

                nr_queries = 1

        # Clean up - None
        # =========================================
