*******
Pandemy
*******

|PyPI| |Python| |Docs| |LICENSE|

Pandemy is a wrapper around `pandas`_ and `SQLAlchemy`_ to provide an easy class based interface for working with DataFrames and databases.
This package is for those who enjoy working with pandas and SQL but do not want to learn all "bells and whistles" of SQLAlchemy.
Use your existing SQL knowledge and provide text based SQL statements to load DataFrames from and write DataFrames to databases.

.. _pandas: https://pandas.pydata.org/
.. _SQLAlchemy: https://www.sqlalchemy.org/


Installation
============

Pandemy is available for installation through `PyPI`_ using `pip`_ and the source code is hosted on GitHub at: https://github.com/antonlydell/Pandemy

.. _PyPI: https://pypi.org/project/pandemy/
.. _pip: https://pip.pypa.io/en/stable/getting-started/

.. code-block:: bash

    $ pip install Pandemy


Dependencies
------------

The core dependencies of Pandemy are:

- **pandas** : powerful Python data analysis toolkit
- **SQLAlchemy** : The Python SQL Toolkit and Object Relational Mapper

Databases except for SQLite_ require a third-party database driver package to be installed.
The table below lists database driver packages for supported databases and their corresponding `optional dependency identifier`_.

.. csv-table:: Optional dependencies of Pandemy.
   :delim: ;
   :header-rows: 1
   :align: left

   Database;Driver package;Optional dependency identifier;Version added
   Oracle_;cx_Oracle_;oracle; 1.1.0


To install `cx_Oracle`_ together with Pandemy run:

.. code-block:: bash

   $ pip install Pandemy[oracle]


.. _cx_Oracle: https://oracle.github.io/python-cx_Oracle/
.. _optional dependency identifier: https://setuptools.pypa.io/en/latest/userguide/dependency_management.html#optional-dependencies
.. _Oracle: https://www.oracle.com/database/
.. _SQLite: https://sqlite.org/index.html


A DataFrame to and from table round trip
========================================

This section shows a simple example of writing a DataFrame_ to a SQLite database and reading it back again.

.. _DataFrame: https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html


Save a DataFrame to a table
---------------------------

Let's create a new SQLite database and save a DataFrame to it.

.. code-block:: python

    import io
    import pandas as pd 
    import pandemy

    # Data to save to the database
    data = io.StringIO("""
    ItemId,ItemName,MemberOnly,Description
    1,Pot,0,This pot is empty.
    2,Jug,0,This jug is empty.
    3,Shears,0,For shearing sheep.
    4,Bucket,0,It's a wooden bucket.
    5,Bowl,0,Useful for mixing things.
    6,Amulet of glory,1,A very powerful dragonstone amulet.
    """)

    df = pd.read_csv(filepath_or_buffer=data, index_col='ItemId')  # Create a DataFrame

    # SQL statement to create the table Item in which to save the DataFrame df
    create_table_item = """
    -- The available items in General Stores
    CREATE TABLE IF NOT EXISTS Item (
        ItemId      INTEGER,
        ItemName    TEXT    NOT NULL,
        MemberOnly  INTEGER NOT NULL,
        Description TEXT,

        CONSTRAINT ItemPk PRIMARY KEY (ItemId)
    );
    """

    db = pandemy.SQLiteDb(file='Runescape.db')  # Create the SQLite DatabaseManager instance

    with db.engine.begin() as conn:
        db.execute(sql=create_table_item, conn=conn)
        db.save_df(df=df, table='Item', conn=conn)


The database is managed through the DatabaseManager_ class which in this case is the SQLiteDb_ instance.
Each SQL dialect is a subclass of ``DatabaseManager``. The creation of the ``DatabaseManager`` instance
creates the database engine_ which is used to create a connection to the database. The begin_ method of
the engine returns a context manager with an open database transaction, which commits the statements if
no errors occur or performs a rollback on error. The connection is automatically returned to the engine's
connection pool when the context manager exits. If the database file does not exist it will be created.
The execute_ method allows for execution of arbitrary SQL statements such as creating a table. The save_df_
method saves the DataFrame ``df`` to the table *Item* in the database ``db``.

.. _begin: https://docs.sqlalchemy.org/en/14/core/connections.html#sqlalchemy.engine.Engine.begin
.. _DatabaseManager: https://pandemy.readthedocs.io/en/latest/api_reference/databasemanager.html#databasemanager
.. _engine: https://docs.sqlalchemy.org/en/14/core/connections.html#sqlalchemy.engine.Engine
.. _execute: https://pandemy.readthedocs.io/en/latest/api_reference/databasemanager.html#pandemy.DatabaseManager.execute
.. _save_df: https://pandemy.readthedocs.io/en/latest/api_reference/databasemanager.html#pandemy.DatabaseManager.save_df
.. _SQliteDb: https://pandemy.readthedocs.io/en/latest/api_reference/databasemanager.html#sqlitedb


Load a table into a DataFrame
-----------------------------

The DataFrame saved to the table *Item* of the database *Runescape.db* can easily be read back into a DataFrame.

.. code-block:: python

    import pandas as pd
    from pandas.testing import assert_frame_equal
    import pandemy

    db = pandemy.SQLiteDb(file='Runescape.db', must_exist=True)

    sql = """SELECT * FROM Item ORDER BY ItemId;"""  # Query to read back table Item into a DataFrame

    with db.engine.connect() as conn:
        df_loaded = db.load_table(sql=sql, conn=conn, index_col='ItemId')
    
    assert_frame_equal(df, df_loaded, check_dtype=False)
    print(df)


.. code-block::

                   ItemName  MemberOnly                          Description
    ItemId
    1                   Pot           0                   This pot is empty.
    2                   Jug           0                   This jug is empty.
    3                Shears           0                  For shearing sheep.
    4                Bucket           0                It's a wooden bucket.
    5                  Bowl           0            Useful for mixing things.
    6       Amulet of glory           1  A very powerful dragonstone amulet.


If the ``must_exist`` parameter is set to ``True`` an exception will be raised if the database file is not found. 
This is useful if you expect the database to exist and you want to avoid creating a new database by mistake if it
does not exist. The connect_ method of the engine is similar to begin_ but without opening a transaction.
The load_table_ method supports either a table name or a sql statement for the ``sql`` parameter. 

.. _connect: https://docs.sqlalchemy.org/en/14/core/connections.html#sqlalchemy.engine.Engine.connect
.. _load_table: https://pandemy.readthedocs.io/en/latest/api_reference/databasemanager.html#pandemy.DatabaseManager.load_table


Documentation
=============

The full documentation is hosted at: https://pandemy.readthedocs.io


Tests
=====

Pandemy has a test suite that is using the `pytest`_ framework.
The test suite is located in the directory `tests`_.

.. _pytest: https://docs.pytest.org/en/latest/
.. _tests: https://github.com/antonlydell/Pandemy/tree/main/tests

Run the test suite with the ``pytest`` command from the root directory of the repository:

.. code-block:: bash

   $ pytest


License
=======

Pandemy is distributed under the `MIT-license`_.

.. _MIT-license: https://opensource.org/licenses/mit-license.php


Contributing
============

Suggestions, feature requests and feedback are welcome in *text form* on the tab `GitHub Discussions`_, but *not* as written code.
This project is meant as a source of practice for me to become a better Python developer and I prefer to write the code myself.
Please use the category `Ideas`_ for suggestions and feature request and the `General`_ category for feedback on the project and general questions.

Bug reports should be submitted at the `Github Issues`_ tab.


.. _Github Discussions: https://github.com/antonlydell/Pandemy/discussions

.. _Ideas: https://github.com/antonlydell/Pandemy/discussions/categories/ideas

.. _General: https://github.com/antonlydell/Pandemy/discussions/categories/general

.. _Github Issues: https://github.com/antonlydell/Pandemy/issues


.. |Docs| image:: https://readthedocs.org/projects/pip/badge/?version=latest&style=plastic  
    :alt: Documentation status
    :scale: 100%
    :target: https://pandemy.readthedocs.io/en/latest/?badge=latest

.. |LICENSE| image:: https://img.shields.io/pypi/l/Pandemy?style=plastic
    :alt: PyPI - License
    :scale: 100%
    :target: https://github.com/antonlydell/Pandemy/blob/main/LICENSE

.. |PyPI| image:: https://img.shields.io/pypi/v/Pandemy?style=plastic
    :alt: PyPI
    :scale: 100%
    :target: https://pypi.org/project/Pandemy/

.. |Python| image:: https://img.shields.io/pypi/pyversions/Pandemy?style=plastic
    :alt: PyPI - Python Version
    :scale: 100%
    :target: https://pypi.org/project/Pandemy/