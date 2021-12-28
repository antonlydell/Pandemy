The DatabaseManager
===================

:class:`DatabaseManager <pandemy.DatabaseManager>` is the base class that defines the interface of how to interact with
the database and provides the methods to do so. Each SQL dialect will inherit from the :class:`DatabaseManager <pandemy.DatabaseManager>`
and define the specific details of how to connect to the database and create the database :class:`engine <sqlalchemy:sqlalchemy.engine.Engine>`.
The database :class:`engine <sqlalchemy:sqlalchemy.engine.Engine>` is core component  that allows for connection and interaction with the database.
The engine is created through the :func:`create_engine <sqlalchemy:sqlalchemy.create_engine>` function from SQLAlchemy.
The creation of the connection string needed to create the engine is all handled in the initialization of the :class:`DatabaseManager <pandemy.DatabaseManager>`.
This class is never used on its own instead it serves as the facilitator of functionality to each database dialect. 


Database dialects
-----------------

This section describes the available database dialects in Pandemy and the dialects planned for future releases.

- **SQLite**: :class:`SQLiteDb <pandemy.SQLiteDb>`. 

- **Oracle**: Planned 

- **Microsoft SQL Server**: Planned 


Core functionality
------------------

All database dialects inherits these functions from :class:`DatabaseManager <pandemy.DatabaseManager>`:

- :meth:`execute <pandemy.DatabaseManager.execute>`: Execute arbitrary SQL statements on the database.

- :meth:`save_df <pandemy.DatabaseManager.save_df>`: Save a :class:`pandas DataFrame <pandas:pandas.DataFrame>` to a table in the database.

- :meth:`load_table <pandemy.DatabaseManager.load_table>`: Load a table by name or SQL query into a DataFrame.

.. _pandas DataFrame: https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas-dataframe

Examples of using these methods are shown in the :doc:`sqlite` section, but they work the same regardless of the SQL dialect used.  


The SQLContainer
----------------

When initializing a subclass of :class:`DatabaseManager <pandemy.DatabaseManager>` it can optionally be passed an :class:`SQLContainer <pandemy.SQLContainer>`
class to the ``container`` parameter. The purpose of the :class:`SQLContainer <pandemy.SQLContainer>` is to store SQL statements used by an application in one
place where they can be easily accessed by the :class:`DatabaseManager <pandemy.DatabaseManager>`. Just like the :class:`DatabaseManager <pandemy.DatabaseManager>`
the :class:`SQLContainer <pandemy.SQLContainer>` should be subclassed and not used directly. If your application supports multiple SQL databases you can 
write the SQL statements the application needs in each SQL dialect and store the statements in one :class:`SQLContainer <pandemy.SQLContainer>` per dialect.
Examples of using the :class:`SQLContainer <pandemy.SQLContainer>` with the SQLite DatabaseManager :class:`SQLiteDb <pandemy.SQLiteDb>` are shown in section
:ref:`Using an SQLContainer to organize SQL statements`.
