# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).


## [Unreleased]

- A `DatabaseManager` for [Microsoft SQL Server](https://www.microsoft.com/en-us/sql-server/sql-server-downloads).


## [1.2.0] - 2023-02-06

New methods for `DatabaseManager` and improved performance!

### Added

  - `DatabaseManager.upsert_table()` : Update a table with data from a `pandas.DataFrame` and insert new rows if any.

  - `DatabaseManager.merge_table()` : Merge data from a `pandas.DataFrame` into a table.

  - `DatabaseManager.save_df()` :

    - Added the 'drop-replace' option to the `if_exists` parameter, which drops the table, recreates it, and then writes the
      `pandas.DataFrame` to the table. 

    - Added the parameters `datetime_cols_dtype`, `datetime_format`, `localize_tz` and `target_tz`, which allows adjusting the timezone
      and data type of datetime columns before saving the `pandas.DataFrame` to the database.

  - `DatabaseManager.load_table()` : Added the parameters `datetime_cols_dtype` and `datetime_format`, which allows adjusting the data type
    of datetime columns from the loaded `pandas.DataFrame`.

  - `InvalidColumnNameError` : Exception raised when supplying an invalid column name to a database operation.

  - `SQLStatementNotSupportedError` : Exception raised when executing a method that triggers a SQL statement not supported by the
    database dialect. E.g. a *merge statement*.

### Changed

- The `DatabaseManager` base class is now a proper class and not an ABC. It can now be initialized from a SQLAlchemy URL or Engine.
  This is useful if you want to use Pandemy, but there is no subclass of `DatabaseManager` implemented for the desired SQL dialect.
  It should only be used directly if there is no dedicated subclass since it has limited functionality.

- `DatabaseManager` and its subclasses now use `__slots__`, which improves performance of attribute access and lowers memory usage.

- `SQLiteDb` and `OracleDb` now support to be initialized with a SQLAlchemy connection URL or Engine.

- `Placeholder` has been refactored from a `namedtuple` into a proper class that is using `__slots__`.

### Fixed 

- `DatabaseManager.save_df()` :

  - The option 'replace' of the `if_exists` parameter now correctly deletes the data of the existing table before writing the `pandas.DataFrame`
    instead of dropping the table, recreating it, and finally writing the `pandas.DataFrame` to the table. The old behavior has been moved to the
    'drop-replace' option (see section [Added](#added) above).

  - The method now correctly raises `TableExistsError` and `SaveDataFrameError`. A `ValueError` raised by the `pandas.DataFrame.to_sql()` method,
    that is not related to "table exists and if_exists='fail'", is now translated into a `SaveDataFrameError` and not a `TableExistsError` as before.

### Deprecated

- The methods `OracleDb.from_url()` and `OracleDb.from_engine()` are now deprecated since their functionality is now fulfilled
  by the `url` and `engine` parameters added to the original constructor.

- The `SQLiteDb.conn_str` attribute is deprecated and replaced by the `url` attribute.


## [1.1.0] - 2022-04-09

Support for Oracle databases!

### Added

- `OracleDb` : The [Oracle](https://www.oracle.com/database/) DatabaseManager. `OracleDb` also introduces the alternative constructor methods `from_url()` and `from_engine()`, which create an instance of `OracleDb` from an existing SQLAlchemy connection URL or engine respectively.

- `CreateConnectionURLError` : An exception raised if there are errors when creating a `DatabaseManager` from a SQLAlchemy connection URL.


## [1.0.0] - 2022-02-12

The first major release of Pandemy!

### Added

- `DatabaseManager` : The base class with functionality for managing a database. Each database type will subclass from `DatabaseManager` and implement the initializer which is specific to each database type. `DatabaseManager` is never used on its own, but merely provides the methods to interact with the database to its subclasses. The following methods are available:

  - `delete_all_records_from_table()` : Delete all records from an existing table in the database. 

  - `execute()` : Execute a SQL statement on the database.

  - `load_table()` : Load a table by name or SQL query into a `pandas.DataFrame`.
   
  - `manage_foreign_keys()` : Manage how the database handles foreign key constraints. Implementation in each subclass.

  - `save_df()` : Save a `pandas.DataFrame` to a table in the database.


-  `SQLiteDb` : The [SQLite](https://sqlite.org/index.html) `DatabaseManager`.

   - `manage_foreign_keys()` : In SQLite the check of foreign key constraints is not enabled by default.


- `SQLContainer` : A storage container for the SQL statements used by a `DatabaseManager`. Each SQL-dialect will subclass from `SQLContainer` and `SQLContainer` is never used on its own, but merely provides methods to work with SQL statements. Each SQL statement is implemented as a class variable.

  - `replace_placeholders()` : Replace placeholders in a SQL statement. The main purpose of the method is to handle parametrized IN statements with a variable number of values.


- `Placeholder` : Container of placeholders and their replacement values for parametrized SQL statements. Used as input to `SQLContainer.replace_placeholders()`.


- `PandemyError` : The base Exception of Pandemy.

  - `InvalidInputError` : Invalid input to a function or method.

  - `DatabaseManagerError` : Base Exception for errors related to the `DatabaseManager` class.

    - `CreateEngineError` : Error when creating the database engine.

    - `DatabaseFileNotFoundError` : Error when the file of a SQLite database cannot be found.

    - `DataTypeConversionError` : Errors when converting data types of columns in a `pandas.DataFrame`.

    - `DeleteFromTableError` : Errors when deleting data from a table in the database.

    - `ExecuteStatementError` : Errors when executing a SQL statement with a `DatabaseManager`.

    - `InvalidTableNameError` : Errors when supplying an invalid table name to a database operation.

    - `LoadTableError` : Errors when loading tables from the database.

    - `SaveDataFrameError` : Errors when saving a `pandas.DataFrame` to a table in the database.

    - `SetIndexError` : Errors when setting an index of a `pandas.DataFrame` after loading a table from the database.

    - `TableExistsError` : Errors when saving a `pandas.DataFrame` to a table and the table already exists.


## [0.0.1] - 2021-05-18

### Added

- The initial structure of the project.

- Registration on [PyPI](https://pypi.org/project/Pandemy/0.0.1/).


[Unreleased]: https://github.com/antonlydell/Pandemy/compare/v1.2.0...HEAD
[1.2.0]: https://github.com/antonlydell/Pandemy/compare/v1.1.0...v1.2.0
[1.1.0]: https://github.com/antonlydell/Pandemy/compare/v1.0.0...v1.1.0
[1.0.0]: https://github.com/antonlydell/Pandemy/compare/v0.0.1...v1.0.0
[0.0.1]: https://github.com/antonlydell/Pandemy/releases/tag/v0.0.1