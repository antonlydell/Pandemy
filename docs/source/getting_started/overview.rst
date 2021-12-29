Overview
========

This section shows a simple example of using Pandemy to write a DataFrame 
to an `SQLite`_ database and reading it back again.

.. _SQLite: https://sqlite.org/index.html


Save a DataFrame to a table
---------------------------

Let's create a new SQLite database and save a DataFrame to it.


.. only:: builder_html

   :download:`overview_save_df.py <./examples/overview_save_df.py>`


.. testcode:: overview_save_df

   # overview_save_df.py

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

   with db.engine.connect() as conn:
      db.execute(sql=create_table_item, conn=conn)
      db.save_df(df=df, table='Item', conn=conn)


.. code-block:: bash

    $ python overview_save_df.py


The database is managed through the :class:`DatabaseManager <pandemy.DatabaseManager>` class which is this case is the 
:class:`SQLiteDb <pandemy.SQLiteDb>` instance. Each SQL dialect will be a subclass of :class:`DatabaseManager <pandemy.DatabaseManager>`.
The creation of the :class:`DatabaseManager <pandemy.DatabaseManager>` instance creates the database engine, which is used to create a
connection to the database. The :class:`engine <sqlalchemy:sqlalchemy.engine.Engine>` is created with the :func:`create_engine <sqlalchemy:sqlalchemy.create_engine>` 
function from SQLAlchemy. The connection is automatically closed when the context manager exits. If the database file does not exist it will be created.

The :meth:`execute <pandemy.DatabaseManager.execute>` method allows for execution of arbitrary SQL statements such as creating a table.
The :meth:`save_df <pandemy.DatabaseManager.save_df>` method saves the DataFrame ``df`` to the table *Item* in the database ``db`` 
by using pandas' :meth:`to_sql <pandas:pandas.DataFrame.to_sql>` DataFrame method.


Load a table into a DataFrame
-----------------------------

The DataFrame saved to the table *Item* of the database *Runescape.db* can easily be read back into a DataFrame.


.. testsetup:: getting_started_overview_load_table

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

   with db.engine.connect() as conn:
      db.execute(sql=create_table_item, conn=conn)
      db.save_df(df=df, table='Item', conn=conn)


.. only:: builder_html

   :download:`overview_load_table.py <./examples/overview_load_table.py>`


.. testcode:: getting_started_overview_load_table

   # overview_load_table.py

   import pandemy

   db = pandemy.SQLiteDb(file='Runescape.db', must_exist=True)

   sql = """SELECT * FROM Item ORDER BY ItemId;"""  # Query to read back table Item into a DataFrame

   with db.engine.connect() as conn:
      df_loaded = db.load_table(sql=sql, conn=conn, index_col='ItemId')

   print(df_loaded)


.. code-block:: bash

    $ python overview_load_table.py


.. testoutput:: getting_started_overview_load_table
   :options: +NORMALIZE_WHITESPACE

                  ItemName  MemberOnly                          Description
   ItemId
   1                   Pot           0                   This pot is empty.
   2                   Jug           0                   This jug is empty.
   3                Shears           0                  For shearing sheep.
   4                Bucket           0                It's a wooden bucket.
   5                  Bowl           0            Useful for mixing things.
   6       Amulet of glory           1  A very powerful dragonstone amulet.


If the ``must_exist`` parameter is set to ``True`` :exc:`pandemy.DatabaseFileNotFoundError`
will be raised if the database file is not found. This is useful if you expect the database to exist 
and you want to avoid creating a new database by mistake if it does not exist.

The :meth:`load_table <pandemy.DatabaseManger.load_table>` method takes either a table name or an sql statement
for the ``sql`` parameter and uses the :func:`read_sql <pandas:pandas.read_sql>` function from pandas.
