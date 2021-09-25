"""Module with the SQLContainer class.

The SQLContainer handles storing text based SQL statements
that can be executed by the DatabaseManager. It can also load
SQL statements from text files (not implemented yet).

namedtuple
----------

Placeholder: namedtuple('Placeholder', ['key', values', 'new_key'], defaults=(True,))
    The `Placeholder` contains placeholder keys and their replacement values
    to use when building parametrized SQL statements. An SQL placeholder is
    always prefixed by a colon e.g. ":placeholder".
    The `Placeholder` is used as input to the `replace_placeholders` method
    of the `SQLContainer` class.

    key : str
        The placeholder to replace in the substring.

    values : Union[str, int, float, Sequence[Union[str, int, float]]]
        The value(s) to replace the placeholder with.

    new_key : bool, default True
        If the values should be added as new placeholders.
        This is useful when parametrizing an IN statement and
        the number of values for the IN statement is unknown.
        A single placeholder can be placed in the IN statement and
        later be replaced by new placeholders that match the length
        of values in `values`.
"""

# ===============================================================
# Imports
# ===============================================================

# Standard Library
from abc import ABC
from collections import namedtuple
import logging
from typing import Sequence, Tuple, Union

# Third Party

# Local
import pandemy

# ===============================================================
# Set Logger
# ===============================================================

# Initiate the module logger
# Handlers and formatters will be inherited from the root logger
logger = logging.getLogger(__name__)

# A representation of a placeholder in a parametrized SQL statement.
Placeholder = namedtuple('Placeholder', ['key', 'values', 'new_key'], defaults=(True,))

# ===============================================================
# Classes
# ===============================================================


class SQLContainer(ABC):
    r"""Base class of a container of SQL statements.

    Each SQL-dialect will inherit from this class.
    Each statement is implemented as a class variable.
    """

    @staticmethod
    def validate_class_variables(cls: object, parent_cls: object, type_validation: str) -> None:
        r"""
        Validate that a subclass has implmeneted the class variables
        specified on its parent class.

        Intended to be used in special method `__init_subclass__`.

        Parameters
        ----------
        cls : object
            The class being validated.

        parent_cls : object
            The parent class that `cls` inherits from.

        type_validation : {'isinstance', 'type'}
            How to validate the type of the class variables.
            'type' should be used if a class is assigned to class variables.

        Raises
        ------
        AttributeError
            If the parent class is missing annotated class variables.

        NotImplementedError
            If a class variable is not implemented.

        TypeError
            If a class variable is not of the type specified in the parent class.

        ValueError
            If a value other than 'isinstance' or 'type' is given to the `type_validation` parameter.
        """

        # Get the annotated class variables of the parent class
        class_vars = parent_cls.__annotations__

        for var, dtype in class_vars.items():
            logger.debug(f'var = {var}, dtype = {dtype}')

            # Check that the class variable exists
            if (value := getattr(cls, var, None)) is None:
                raise NotImplementedError(f'Class {cls.__name__} has not implemented the requried variable: {var}')

            # Check for correct data type
            if type_validation == 'isinstance':
                is_valid = isinstance(value, dtype)
            elif type_validation == 'type':
                is_valid = type(value) == type(dtype)
            else:
                raise ValueError(f"type_validation = {type_validation}. Expected 'isinstance', or 'type'")

            if not is_valid:
                raise TypeError(f'Class variable "{var}"" of class {cls.__name__} '
                                f'is of type {type(value)} ({value}). Expected {dtype}.')

    @staticmethod
    def replace_placeholders(stmt: str, placeholders: Union[Placeholder, Sequence[Placeholder]]) -> Tuple[str, dict]:
        r"""Replace placeholders in an SQL statement.

        Replace the placeholders in the SQL statement `stmt` that are specified
        by the `key` parameter of a Placeholder instance in `placeholders` with their respective replacement value in
        the `values` parameter of a Placeholder. A placeholder in an SQL statement is always prefixed
        with a colon symbol, e.g. :placeholder1.

        Parameters
        ----------
        stmt : str
            The SQL statement in which to replace placeholders.

        placeholders : Placeholder or sequence of Placeholder
            The replacement values for each placeholder.
            Placeholder = namedtuple('Placeholder', ['key', 'values', 'new_key'], defaults=(True,))

        Returns
        -------
        stmt : str
            The SQL statement after placeholders have been replaced.

        params : dict
            The new placeholders and their replacement values from the `values` parameter of a Placeholder namedtuple.
            Entries to `params` are only written if the parameter `new_key` in a Placeholder is set to True.
            E.g. `{'new_placeholder1': 'value1', 'new_placeholder2': 'value2'}`.

        Raises
        ------
        pandemy.InvalidInputError
            If the replacement values in a Placeholder are not valid.

        See Also
        --------
        Placeholder : namedtuple with placeholders and replacement values.
        """

        def is_valid_replacement_value(value: Union[str, int, float, bool, None], raises: bool = False) -> bool:
            r"""Helper function to validate values of the replacements.

            Parameters
            ----------
            value : str, int or float
                The value to validate.

            raises : bool, default False
                If True pandemy.InvalidInputError will be raised if `value` is not valid.
                If False the function will return False instead of raising an exception.

            Returns
            -------
            bool
                True if the value is valid and False otherwise.

            Raises
            ------
            pandemy.InvalidInputError
                If the replacement values in a Placeholder are not valid and `raises` is True.
            """

            if (isinstance(value, str) or
                isinstance(value, int) or
                isinstance(value, float) or
                isinstance(value, bool) or
                value is None):
                return True
            else:
                if raises:
                    raise pandemy.InvalidInputError('values in replacements must be: str, int, float, bool or None.'
                                                    f'Got {value} ({type(value)})', data=value)
                else:
                    return False

        # Stores new placeholders and their mapped values
        params = dict()

        # Counter of number of new placeholders added. Makes sure each new placeholder is unique
        counter = 0

        # Convert to list if a single Placeholder object is passed
        if isinstance(placeholders, Placeholder):
            placeholders = [placeholders]

        for placeholder in placeholders:
            if is_valid_replacement_value(placeholder.values, raises=False):

                # Build replacement string of new placeholder
                if placeholder.new_key:
                    new_placeholder = f'v{counter}'
                    counter += 1
                    repl_str = f':{new_placeholder}'
                    params[new_placeholder] = placeholder.values
                else:
                    repl_str = str(placeholder.values)

            elif hasattr(placeholder.values, '__iter__'):  # list like

                repl_str = ''
                for value in placeholder.values:

                    # Check that we have a valid replacement value
                    is_valid_replacement_value(value, raises=True)

                    # Build replacement string of new placeholders
                    if placeholder.new_key:
                        new_placeholder = f'v{counter}'
                        counter += 1
                        repl_str += f':{new_placeholder}, '
                        params[new_placeholder] = value
                    else:
                        repl_str += f'{value}, '

                # Remove last unwanted ', '
                repl_str = repl_str[:-2]

            else:
                raise pandemy.InvalidInputError(f'placeholder replacement values must be of type str, int, float, bool '
                                                'or a sequence of those. '
                                                f'Got {placeholder.values} ({type(placeholder.values)})')

            # Replace the placeholder with the replacement string
            stmt = stmt.replace(placeholder.key, repl_str)
            logger.debug(f'stmt = {stmt}')
            logger.debug(f'params = {params}')

        return stmt, params
