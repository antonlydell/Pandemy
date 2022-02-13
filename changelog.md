# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).


## [Unreleased]

- Support for [Oracle](https://www.oracle.com/database/).

- Support for [Microsoft SQL Server](https://www.microsoft.com/en-us/sql-server/sql-server-downloads).

- *DatabaseManager.update_table()*: Update the records of a table from a [*pandas.DataFrame*](https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.html#pandas.DataFrame).

- *SQLContainer.from_file()* : Load SQL statements from a file and create an instance of *SQLContainer*.


## [1.0.0] - 2022-02-12

The first major release of Pandemy!

### Added

- **DatabaseManager**: The base class with functionality for managing a database. Each database type will subclass from *DatabaseManager* and implement the initializer which is specific to each database type. *DatabaseManager* is never used on its own, but merely provides the methods to interact with the database to its subclasses. The following methods are available:

  - *delete_all_records_from_table()* : Delete all records from an existing table in the database. 

  - *execute()* : Execute a SQL statement on the database.

  - *load_table()* : Load a table by name or SQL query into a *pandas.DataFrame*.
   
  - *manage_foreign_keys()* : Manage how the database handles foreign key constraints. Implementation in each subclass.

  - *save_df()* : Save a *pandas.DataFrame* to a table in the database.


-  **SQLiteDb** : A [SQLite](https://sqlite.org/index.html) *DatabaseManager*.

   - *manage_foreign_keys()* : In SQLite the check of foreign key constraints is not enabled by default.


- **SQLContainer** : A storage container for the SQL statements used by a *DatabaseManager*. Each SQL-dialect will subclass from *SQLContainer* and *SQLContainer* is never used on its own, but merely provides methods to work with SQL statements. Each SQL statement is implemented as a class variable.

  - *replace_placeholders()* : Replace placeholders in a SQL statement. The main purpose of the method is to handle parametrized IN statements with a variable number of values.


- **Placeholder** : Container of placeholders and their replacement values for parametrized SQL statements. Used as input to *SQLContainer.replace_placeholders()*.


## [0.0.1] - 2021-05-18

### Added

- The initial structure of the project.

- Registration on [PyPI](https://pypi.org/).


[Unreleased]: https://github.com/antonlydell/Pandemy/compare/v1.0.0...HEAD
[1.0.0]: https://github.com/antonlydell/Pandemy/compare/v0.0.1...v1.0.0
[0.0.1]: https://github.com/antonlydell/Pandemy/releases/tag/v0.0.1