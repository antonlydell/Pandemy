*******
Pandemy
*******

Pandemy is a wrapper around `pandas`_ and `SQLAlchemy`_ to provide an easy class based interface for working with DataFrames and databases.
This package is for those who enjoy working with pandas and SQL but do not want to learn all "bells and whistles" of SQLAlchemy.
Use your existing SQL knowledge and provide text based SQL statements to load DataFrames from and write DataFrames to databases.

.. _pandas: https://pandas.pydata.org/
.. _SQLAlchemy: https://www.sqlalchemy.org/


Installation
============

Pandemy is available for installation through `PyPI`_ using `pip`_ and the source code is hosted on GitHub at: https://github.com/antonlydell/pandemy

.. _PyPI: https://pypi.org/project/pandemy/
.. _pip: https://pip.pypa.io/en/stable/getting-started/

.. code-block:: bash

    $ pip install pandemy


Dependencies
------------

The core dependencies of Pandemy are:

- **pandas** : powerful Python data analysis toolkit
- **SQLAlchemy** : The Python SQL Toolkit and Object Relational Mapper

To work with other databases than `SQLite`_ (which is the only supported database in version 1.0.0) additional optional dependencies may need to be installed.
Support for `Oracle`_ and `Microsoft SQL Server`_ databases is planned for the future.

.. _SQLite: https://sqlite.org/index.html
.. _Oracle: https://www.oracle.com/database/
.. _Microsoft SQL Server: https://www.microsoft.com/en-us/sql-server/sql-server-downloads


A DataFrame to and from table round trip
========================================

This section shows a simple example of writing a DataFrame to a SQLite database and reading it back again.


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

    db = pandemy.SQLiteDb(file= 'Runescape.db')  # Create the SQLite DatabaseManager instance

    with db.engine.connect() as conn:
        db.execute(sql=create_table_item, conn=conn)
        db.save_df(df=df, table='Item', conn=conn)

The database is managed through the ``DatabaseManager`` class which in this case is the ``SQLiteDb`` instance.
Each SQL dialect will be a subclass of ``DatabaseManager``. The creation of the ``DatabaseManager`` instance creates the database engine 
which is used to create a connection to the database. The engine is created with the `create_engine`_ function from SQLAlchemy. 
The connection is automatically closed when the context manager exits. If the database file does not exist it will be created.

.. _create_engine: https://docs.sqlalchemy.org/en/14/core/engines.html#engine-creation-api

The ``execute`` method allows for execution of arbitrary SQL statements such as creating a table. The ``save_df`` method 
saves the DataFrame ``df`` to the table *Item* in the database ``db`` by using pandas' `to_sql`_ DataFrame method.

.. _to_sql: https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.to_sql.html


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

If the ``must_exist`` parameter is set to ``True`` an exception will be raised if the database file is not found. 
This is useful if you expect the database to exist and you want to avoid creating a new database by mistake if it does not exist.

The ``load_table`` method supports either a table name or a sql statement for the ``sql`` parameter and 
uses the `read_sql`_ DataFrame method from pandas.

.. _read_sql: https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.read_sql.html


Documentation
=============

The full documentation is under development.


Tests
=====

Pandemy has a test suite that is using the `pytest`_ framework.
The test suite is located in the directory `tests`_.

.. _pytest: https://docs.pytest.org/en/latest/
.. _tests: https://github.com/antonlydell/pandemy/tests

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


.. _Github Discussions: https://github.com/antonlydell/pandemy/discussions

.. _Ideas: https://github.com/antonlydell/pandemy/discussions/categories/ideas

.. _General: https://github.com/antonlydell/pandemy/discussions/categories/general

.. _Github Issues: https://github.com/antonlydell/pandemy/issues
