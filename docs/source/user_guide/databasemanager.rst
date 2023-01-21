The DatabaseManager
===================

:class:`DatabaseManager <pandemy.DatabaseManager>` is the base class that defines the interface of how to interact with
the database and provides the methods to do so. Each SQL dialect will inherit from the :class:`DatabaseManager <pandemy.DatabaseManager>`
and define the specific details of how to connect to the database and create the database :class:`engine <sqlalchemy.engine.Engine>`.
The database :class:`engine <sqlalchemy.engine.Engine>` is the core component that allows for connection and interaction with the database.
The :class:`engine <sqlalchemy.engine.Engine>` is created through the :func:`sqlalchemy.create_engine` function. The creation of the
connection URL needed to create the :class:`engine <sqlalchemy.engine.Engine>` is handled during the initialization of
:class:`DatabaseManager <pandemy.DatabaseManager>`. In cases where a subclass of :class:`~pandemy.DatabaseManager` for the desired SQL dialect
does not exist this class can be used on its own (starting in version 1.2.0) but with limited functionality. Some methods that require dialect
specific SQL statements such as :meth:`~pandemy.DatabaseManager.merge_df` will not be available. Using :class:`~pandemy.DatabaseManager` on its
own also requires initialization through a SQLAlchemy :class:`URL <sqlalchemy.engine.URL>` or :class:`Engine <sqlalchemy.engine.Engine>`,
which require some knowledge about `SQLAlchemy`_.

.. _SQLAlchemy: https://docs.sqlalchemy.org/en/14/core/engines.html#engine-configuration


SQL dialects
------------

This section describes the available SQL dialects in Pandemy and the dialects planned for future releases.

- **SQLite**: :class:`SQLiteDb <pandemy.SQLiteDb>`.

- **Oracle**: :class:`OracleDb <pandemy.OracleDb>` (*New in version 1.1.0*).

- **Microsoft SQL Server**: Planned.


Core functionality
------------------

All SQL dialects inherit these methods from :class:`DatabaseManager <pandemy.DatabaseManager>`:

- :meth:`~pandemy.DatabaseManager.delete_all_records_from_table`: Delete all records from an existing table in the database.

- :meth:`~pandemy.DatabaseManager.execute`: Execute arbitrary SQL statements on the database.

- :meth:`~pandemy.DatabaseManager.load_table`: Load a table by name or SQL query into a :class:`pandas.DataFrame`.

- :meth:`~pandemy.DatabaseManager.manage_foreign_keys`: Manage how the database handles foreign key constraints.

- :meth:`~pandemy.DatabaseManager.merge_df`: Merge data from a :class:`pandas.DataFrame` into a table (:class:`~pandemy.OracleDb` only).

- :meth:`~pandemy.DatabaseManager.save_df`: Save a :class:`pandas.DataFrame` to a table in the database.

- :meth:`~pandemy.DatabaseManager.upsert_table`: Update a table with data from a :class:`pandas.DataFrame` and insert new rows if any.


.. versionadded:: 1.2.0
   :meth:`~pandemy.DatabaseManager.merge_df` and :meth:`~pandemy.DatabaseManager.upsert_table`


Examples of using these methods are shown in the :doc:`sqlite/index` and :doc:`oracle/index` sections, but they work the same regardless of the SQL dialect used.


The SQLContainer
----------------

When initializing a subclass of :class:`DatabaseManager <pandemy.DatabaseManager>` it can optionally be passed a :class:`SQLContainer <pandemy.SQLContainer>`
class to the ``container`` parameter. The purpose of the :class:`SQLContainer <pandemy.SQLContainer>` is to store SQL statements used by an application in one
place where they can be easily accessed by the :class:`DatabaseManager <pandemy.DatabaseManager>`. Just like the :class:`DatabaseManager <pandemy.DatabaseManager>`
the :class:`SQLContainer <pandemy.SQLContainer>` should be subclassed and not used directly. If your application supports multiple SQL databases you can 
write the SQL statements the application needs in each SQL dialect and store the statements in one :class:`SQLContainer <pandemy.SQLContainer>` per dialect.
Examples of using the :class:`SQLContainer <pandemy.SQLContainer>` with the SQLite DatabaseManager :class:`SQLiteDb <pandemy.SQLiteDb>` are shown in section
:ref:`user_guide/sqlite/sqlcontainer:Using the SQLContainer`.
