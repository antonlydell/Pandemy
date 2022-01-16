SQLite
======

This section describes the SQLite DatabseManager :class:`SQLiteDb <pandemy.SQLiteDb>`.

Initialization
--------------

Initializing a SQLite :class:`DatabaseManager <pandemy.DatabaseManager>` with no arguments creates
a database that lives in memory.


.. doctest::
   
   >>> import pandemy
   >>> db = pandemy.SQLiteDb()
   >>> print(db)
   SQLiteDb(file=:memory:, must_exist=False)
   

The ``file`` parameter is used to connect to a database file and if the file does not exist it will be created,
which is the standard behavior of SQLite. The ``file`` parameter can be a string or :class:`python:pathlib.Path`
with the full path to the file or the filename relative to the current working directory. Specifying ``file=':memory:'`` will
create an in memory database which is the default.


.. doctest:: init_SQLiteDb
   
   >>> import pandemy
   >>> db = pandemy.SQLiteDb(file='my_db.db')
   >>> print(db)
   SQLiteDb(file=my_db.db, must_exist=False)


Require the database to exist
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Sometimes creating a new database if the database file does not exist is not a desired outcome. Your application may
expect a database to already exist and be ready for use and if it does not exist the application cannot function correctly. 
For these circumstances you can set the  ``must_exist`` parameter to ``True`` which will raise :exc:`pandemy.DatabaseFileNotFoundError`
if the file is not found.


.. doctest:: SQLiteDb_must_exist_True
   :options: +IGNORE_EXCEPTION_DETAIL

   >>> import pandemy
   >>> db = pandemy.SQLiteDb('my_db_does_not_exist.db', must_exist=True)
   Traceback (most recent call last):
   ...
   pandemy.exceptions.DatabaseFileNotFoundError: file = my_db_does_not_exist.db does not exist and and must_exist = True. Cannot instantiate the SQLite DatabaseManager.


The execute method
------------------

The :meth:`execute <pandemy.DatabaseManager.execute>` method can be used to execute arbitrary SQL statements e.g. creating a table.
Let us create a SQLite database that can be used to further demonstrate how Pandemy works.

The complete SQLite test database that is used for the test suite of Pandemy can be downloaded
below to test Pandemy on your own.

.. only:: builder_html

   :download:`Runescape.db <../../../tests/static_files/SQLite/Runescape.db>` 


.. only:: builder_html

   :download:`create_database.py </examples/SQLite/create_database.py>` 

.. testcode:: create_database

   # create_database.py

   import pandemy

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
   

.. code-block:: bash

   $ python create_database.py


Parametrized SQL statements 
^^^^^^^^^^^^^^^^^^^^^^^^^^^

A parametrized SQL statement can be created by using the ``params`` parameter.
The SQL statement should contain placeholders that will be replaced by values when the statement
is executed. The placeholders should be prefixed by a colon (e.g. *':myparameter'*) in the SQL statement.
The parameter ``params`` takes a dictionary that maps the parameter name to its value or a list of such dictionaries.
Note that parameter names in the dictionary should not be prefixed with a colon i.e. the key in the dictionary
that references the SQL placeholder *':myparameter'* should be named *'myparameter'*.

Let's insert some data into the *Item* table we just created in *Runescape.db*.

.. The database Runescape.db with an empty table Item
.. Test setup used for multiple examples
.. testsetup:: save_df, execute_insert_into, execute_insert_into_transaction

   import pandemy

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


.. only:: builder_html

   :download:`execute_insert_into.py </examples/SQLite/execute_insert_into.py>` 


.. testcode:: execute_insert_into

   # execute_insert_into.py

   import pandemy

   # SQL statement to insert values into the table Item
   insert_into_table_item = """
   INSERT INTO Item (ItemId, ItemName, MemberOnly, Description)
      VALUES (:itemid, :itemname, :memberonly, :description);
   """

   params = {'itemid': 1, 'itemname': 'Pot', 'memberonly': 0, 'description': 'This pot is empty'}

   db = pandemy.SQLiteDb(file='Runescape.db')  # Create the SQLite DatabaseManager instance

   with db.engine.connect() as conn:
      db.execute(sql=insert_into_table_item, conn=conn, params=params)
   
   # Add some more rows
   params = [
      {'itemid': 2, 'itemname': 'Jug', 'memberonly': 0, 'description': 'This jug is empty'},
      {'itemid': 3, 'itemname': 'Shears', 'memberonly': 0, 'description': 'For shearing sheep'},
      {'itemid': 4, 'itemname': 'Bucket', 'memberonly': 0, 'description': 'It''s a wooden bucket.'}
   ]
   
   with db.engine.connect() as conn:
      db.execute(sql=insert_into_table_item, conn=conn, params=params)
      
   # Retrieve the inserted rows
   query = """SELECT * FROM Item;"""

   with db.engine.connect() as conn:
      result = db.execute(sql=query, conn=conn)

      for row in result:
         print(row)


.. code-block:: bash 

   $ python execute_insert_into.py


.. testoutput:: execute_insert_into

   (1, 'Pot', 0, 'This pot is empty')
   (2, 'Jug', 0, 'This jug is empty')      
   (3, 'Shears', 0, 'For shearing sheep')  
   (4, 'Bucket', 0, 'Its a wooden bucket.')

The :meth:`execute <pandemy.DatabaseManager.execute>` method returns an object called
:class:`CursorResult <sqlalchemy:sqlalchemy.engine.CursorResult>` (the variable ``result``).
This object is an iterator that can be used to retrieve rows from the result set of a *SELECT* statement. 

.. When using a *SELECT* statement a result object is returned from the query called :class:`CursorResult <sqlalchemy:sqlalchemy.engine.CursorResult>`.
.. The result object is an iterator that can be used to retrieve rows from the result. 

.. note::

   The database connection must remain open while iterating over the rows in the
   :class:`CursorResult <sqlalchemy:sqlalchemy.engine.CursorResult>` object,
   since it is fetching one row from the database at the time.
   This means that the for loop must be placed inside the context manager.


Using transactions
^^^^^^^^^^^^^^^^^^

Database transactions can be invoked by calling the :meth:`begin <sqlalchemy:sqlalchemy.engine.Engine.begin>` method of the database engine
instead of :meth:`connect <sqlalchemy:sqlalchemy.engine.Engine.connect>`. When executing SQL statements under an open transaction 
all statements will be automatically be rolled back to the latest valid state if an error occurs in one of the statements. This differs
from using the connect method where only the statement where the error occurs will be rolled back. The example below illustrates this
difference.


.. only:: builder_html

   :download:`execute_insert_into_transaction.py </examples/SQLite/execute_insert_into_transaction.py>`


.. testcode:: execute_insert_into_transaction

   # execute_insert_into_transaction.py

   # Using the previously created database Runescape.db
   db = pandemy.SQLiteDb(file='Runescape.db')

   # Clear previous content in the table Item
   with db.engine.connect() as conn:
      db.delete_all_records_from_table(table='Item', conn=conn)

   # SQL statement to insert values into the table Item
   insert_into_table_item = """
   INSERT INTO Item (ItemId, ItemName, MemberOnly, Description)
      VALUES (:itemid, :itemname, :memberonly, :description);
   """

   row_1 = {'itemid': 1, 'itemname': 'Pot', 'memberonly': 0, 'description': 'This pot is empty'}

   with db.engine.connect() as conn:
      db.execute(sql=insert_into_table_item, conn=conn, params=row_1)

   # Add a second row
   row_2 = {'itemid': 2, 'itemname': 'Jug', 'memberonly': 0, 'description': 'This jug is empty'},

   # Add some more rows (the last row contains a typo for the itemid parameter)
   rows_3_4 = [{'itemid': 3, 'itemname': 'Shears', 'memberonly': 0, 'description': 'For shearing sheep'},
               {'itemi': 4, 'itemname': 'Bucket', 'memberonly': 0, 'description': 'It''s a wooden bucket.'}]

   # Insert with a transaction
   try:
      with db.engine.begin() as conn:
         db.execute(sql=insert_into_table_item, conn=conn, params=row_2)
         db.execute(sql=insert_into_table_item, conn=conn, params=rows_3_4)
   except pandemy.ExecuteStatementError as e:
      print(f'{e.args}\n')

   # Retrieve the inserted rows
   query = """SELECT * FROM Item;"""

   with db.engine.connect() as conn:
      result = db.execute(sql=query, conn=conn)
      result = result.fetchall()

   print(f'All statements under the transaction are rolled back when an error occurs:\n{result}\n\n')

   # Using connect instead of begin
   try:
      with db.engine.connect() as conn:
         db.execute(sql=insert_into_table_item, conn=conn, params=row_2)
         db.execute(sql=insert_into_table_item, conn=conn, params=rows_3_4)
   except pandemy.ExecuteStatementError as e:
      print(f'{e.args}\n')

   # Retrieve the inserted rows
   query = """SELECT * FROM Item;"""

   with db.engine.connect() as conn:
      result = db.execute(sql=query, conn=conn)
      result = result.fetchall()

   print(f'Only the statement with error is rolled back when using connect:\n{result}')

.. code-block:: bash

   $ python execute_insert_into_transaction.py

.. testoutput:: execute_insert_into_transaction

   ('StatementError: ("(sqlalchemy.exc.InvalidRequestError) A value is required for bind parameter \'itemid\', in parameter group 1",)',)

   All statements under the transaction are rolled back when an error occurs:
   [(1, 'Pot', 0, 'This pot is empty')]


   ('StatementError: ("(sqlalchemy.exc.InvalidRequestError) A value is required for bind parameter \'itemid\', in parameter group 1",)',)

   Only the statement with error is rolled back when using connect:
   [(1, 'Pot', 0, 'This pot is empty'), (2, 'Jug', 0, 'This jug is empty')]

.. note::

   The :meth:`fetchall <sqlalchemy:sqlalchemy.engine.CursorResult.fetchall>` method of the :class:`CursorResult <sqlalchemy:sqlalchemy.engine.CursorResult>`
   object can be used to retrieve all rows from the query into a list.

.. warning::

   The method :meth:`delete_all_records_from_table <pandemy.DatabaseManager.delete_all_records_from_table>` will delete all records from a table.
   Use this method with caution. It is mainly used to clear all content of a table before replacing it with new data. This method is used by
   the :meth:`save_df <pandemy.DatabaseManager.save_df>` method when using ``if_exists='replace'``, which is described in the next section.


.. seealso::

   The SQLAlchemy documentation provides more information about transactions: 


   * :class:`sqlalchemy:sqlalchemy.engine.Transaction`
   
   * :meth:`sqlalchemy:sqlalchemy.engine.Engine.begin`

   * :meth:`sqlalchemy:sqlalchemy.engine.Engine.connect`


   
Save a DataFrame to a table
---------------------------

Executing insert statements with the :meth:`execute <pandemy.DatabaseManager.execute>` method as shown above is not very practical if you have a lot of data
in a pandas DataFrame that you want to save to a table. In these cases it is better to use the method :meth:`save_df <pandemy.DatabaseManager.save_df>`.
It uses the power of pandas' :meth:`to_sql <pandas:pandas.DataFrame.to_sql>` DataFrame method and lets you save a DataFrame to a table in a single line of code.

The example below shows how to insert data to the table *Item* using a pandas DataFrame. 

.. only:: builder_html

   :download:`save_df.py </examples/SQLite/save_df.py>`


.. testcode:: save_df

   # save_df.py

   import io
   import pandas as pd
   import pandemy

   # The content to write to table Item
   data = io.StringIO(r"""
   ItemId;ItemName;MemberOnly;Description
   1;Pot;0;This pot is empty.
   2;Jug;0;This jug is empty.
   3;Shears;0;For shearing sheep.
   4;Bucket;0;It's a wooden bucket.
   5;Bowl;0;Useful for mixing things.
   6;Amulet of glory;1;A very powerful dragonstone amulet.
   7;Tinderbox;0;Useful for lighting a fire.
   8;Chisel;0;Good for detailed Crafting.
   9;Hammer;0;Good for hitting things.
   10;Newcomer map;0;Issued to all new citizens of Gielinor.
   11;Unstrung symbol;0;It needs a string so I can wear it.
   12;Dragon Scimitar;1;A vicious, curved sword.
   13;Amulet of glory;1;A very powerful dragonstone amulet.
   14;Ranarr seed;1;A ranarr seed - plant in a herb patch.
   15;Swordfish;0;I'd better be careful eating this!
   16;Red dragonhide Body;1;Made from 100% real dragonhide.
   """)

   df = pd.read_csv(filepath_or_buffer=data, sep=';', index_col='ItemId')  # Create the DataFrame

   db = pandemy.SQLiteDb(file='Runescape.db')  # Create the SQLite DatabaseManager instance

   with db.engine.connect() as conn:
      db.save_df(df=df, table='Item', conn=conn, if_exists='replace')


.. code-block:: bash

   $ python save_df.py 


:meth:`save_df <pandemy.DatabaseManager.save_df>` implements all parameters of :meth:`to_sql <pandas:pandas.DataFrame.to_sql>`. 
The ``if_exists`` parameter is slightly different. ``if_exists`` controls how to save a DataFrame to an existing table in the database.

``if_exists`` accepts the following values:

* *'append'*: Append the DataFrame to the existing table (default).

* *'replace'*: Delete all records from the table and then write the DataFrame to the table.

* *'fail'*: Raise :exc:`pandemy.TableExistsError` if the table exists.

in pandas' :meth:`to_sql <pandas:pandas.DataFrame.to_sql>` method *'fail'* is the default value. The option *'replace'* drops the 
existing table, recreates it with the column definitions from the DataFrame, and inserts the data. By dropping the table and recreating
it you may loose important information such as primary keys and constraints.

In :meth:`save_df <pandemy.DatabaseManager.save_df>` *'replace'* deletes all current records before inserting the new data rather than dropping the table.
This preserves the exisitng columns definitions and constraints of the table. If the target table does not exist it will be created,
which is also how :meth:`to_sql <pandas:pandas.DataFrame.to_sql>` operates by default.


Load a DataFrame from a table
-----------------------------

To load data from a table into a DataFrame the :meth:`load_table <pandemy.DatabaseManager.load_table>` method is used.
It uses the pandas' :func:`read_sql <pandas:pandas.read_sql>` function with some extra features.

Let us load the table *Item* back into a DataFrame.

.. testsetup:: load_table, sql_container, replace_placeholder, replace_multiple_placeholders

   import io
   import pandas as pd 
   import pandemy

   # SQL statement to create the table Item in which to save the DataFrame df
   create_table_item = r"""
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

   data = io.StringIO(r"""
   ItemId;ItemName;MemberOnly;Description
   1;Pot;0;This pot is empty.
   2;Jug;0;This jug is empty.
   3;Shears;0;For shearing sheep.
   4;Bucket;0;It's a wooden bucket.
   5;Bowl;0;Useful for mixing things.
   6;Amulet of glory;1;A very powerful dragonstone amulet.
   7;Tinderbox;0;Useful for lighting a fire.
   8;Chisel;0;Good for detailed Crafting.
   9;Hammer;0;Good for hitting things.
   10;Newcomer map;0;Issued to all new citizens of Gielinor.
   11;Unstrung symbol;0;It needs a string so I can wear it.
   12;Dragon Scimitar;1;A vicious, curved sword.
   13;Amulet of glory;1;A very powerful dragonstone amulet.
   14;Ranarr seed;1;A ranarr seed - plant in a herb patch.
   15;Swordfish;0;I'd better be careful eating this!
   16;Red dragonhide Body;1;Made from 100% real dragonhide.
   """)

   df = pd.read_csv(filepath_or_buffer=data, sep=';', index_col='ItemId')  # Create the DataFrame

   with db.engine.connect() as conn:
      db.execute(sql=create_table_item, conn=conn)
      db.save_df(df=df, table='Item', conn=conn, if_exists='replace')


.. only:: builder_html

   :download:`load_table.py </examples/SQLite/load_table.py>`


.. testcode:: load_table

   # load_table.py

   import pandemy

   db = pandemy.SQLiteDb(file='Runescape.db', must_exist=True)

   query = """SELECT * FROM Item ORDER BY ItemId ASC;"""

   with db.engine.connect() as conn:
      df = db.load_table(sql=query, conn=conn, index_col='ItemId')

   print(df)

.. code-block:: bash

   $ python load_table.py

.. testoutput:: load_table
   :options: +NORMALIZE_WHITESPACE

                      ItemName  MemberOnly                              Description
   ItemId
   1                       Pot           0                       This pot is empty.
   2                       Jug           0                       This jug is empty.
   3                    Shears           0                      For shearing sheep.
   4                    Bucket           0                    It's a wooden bucket.
   5                      Bowl           0                Useful for mixing things.
   6           Amulet of glory           1      A very powerful dragonstone amulet.
   7                 Tinderbox           0              Useful for lighting a fire.
   8                    Chisel           0              Good for detailed Crafting.
   9                    Hammer           0                 Good for hitting things.
   10             Newcomer map           0  Issued to all new citizens of Gielinor.
   11          Unstrung symbol           0      It needs a string so I can wear it.
   12          Dragon Scimitar           1                 A vicious, curved sword.
   13          Amulet of glory           1      A very powerful dragonstone amulet.
   14              Ranarr seed           1   A ranarr seed - plant in a herb patch.
   15                Swordfish           0       I'd better be careful eating this!
   16      Red dragonhide Body           1          Made from 100% real dragonhide.

.. note::

   The ``sql`` parameter can be either a SQL query or a table name. 
   Using a table name will not guarantee the order of the retrieved rows. 

.. Add example about using the dtypes parameter

Working with datetimes and timezones
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Columns with datetime information can be converted into datetime columns by the use of the ``parse_dates`` keyword argument,
which is a direct link to the ``parse_dates`` option of pandas' function :func:`read_sql <pandas:pandas.read_sql>`.

``parse_dates`` only returns naive datetime columns. To load datetime columns with timezone information the keyword arguments 
``localize_tz`` and ``target_tz`` can be specified. ``localize_tz`` lets you localize the the naive datetime columns to a specifed timezone and
``target_tz`` can optionally convert the localized datetime columns into a desired timezone. 

Let's use the table Customer of the Database *Runescape.db* to illustrate this.

.. only:: builder_html

   :download:`load_table_localize_tz.py </examples/SQLite/load_table_localize_tz.py>`

.. testcode:: load_table_localize_tz

   # load_table_localize_tz.py

   import io
   import pandas as pd
   import pandemy

   # SQL statement to create the table Customer in which to save the DataFrame df
   create_table_customer = """
   -- Customers that have traded in a General Store
   CREATE TABLE IF NOT EXISTS Customer (
      CustomerId         INTEGER,
      CustomerName       TEXT    NOT NULL,
      BirthDate          TEXT,
      Residence          TEXT,
      IsAdventurer       INTEGER NOT NULL, -- 1 if Adventurer and 0 if NPC

      CONSTRAINT CustomerPk PRIMARY KEY (CustomerId)
   );
   """

   db = pandemy.SQLiteDb(file='Runescape.db')  # Create the SQLite DatabaseManager instance

   data = io.StringIO("""
   CustomerId;CustomerName;BirthDate;Residence;IsAdventurer
   1;Zezima;1990-07-14;Yanille;1
   2;Dr Harlow;1970-01-14;Varrock;0
   3;Baraek;1968-12-13;Varrock;0
   4;Gypsy Aris;1996-03-24;Varrock;0
   5;Not a Bot;2006-05-31;Catherby;1
   6;Max Pure;2007-08-20;Port Sarim;1
   """)

   df = pd.read_csv(filepath_or_buffer=data, sep=';', index_col='CustomerId',
                    parse_dates=['BirthDate'])  # Create a DataFrame

   with db.engine.connect() as conn:
      db.execute(sql=create_table_customer, conn=conn)
      db.save_df(df=df, table='Customer', conn=conn, if_exists='replace')

      df_naive = db.load_table(sql='Customer', conn=conn, index_col='CustomerId',
                               parse_dates=['Birthdate'])

      df_dt_aware = db.load_table(sql='Customer', conn=conn, index_col='CustomerId',
                                  parse_dates=['Birthdate'], localize_tz='UTC', target_tz='CET')

   print(f'df:\n{df}\n')
   print(f'df_naive:\n{df_naive}\n')
   print(f'df_dt_aware:\n{df_dt_aware}')

.. code-block:: bash

   $ python load_table_localize_tz.py

.. testoutput:: load_table_localize_tz
   :options: +NORMALIZE_WHITESPACE

   df:
              CustomerName  BirthDate   Residence  IsAdventurer
   CustomerId
   1                Zezima 1990-07-14     Yanille             1
   2             Dr Harlow 1970-01-14     Varrock             0
   3                Baraek 1968-12-13     Varrock             0
   4            Gypsy Aris 1996-03-24     Varrock             0
   5             Not a Bot 2006-05-31    Catherby             1
   6              Max Pure 2007-08-20  Port Sarim             1

   df_naive:
               CustomerName  BirthDate   Residence  IsAdventurer
   CustomerId
   1                Zezima 1990-07-14     Yanille             1
   2             Dr Harlow 1970-01-14     Varrock             0
   3                Baraek 1968-12-13     Varrock             0
   4            Gypsy Aris 1996-03-24     Varrock             0
   5             Not a Bot 2006-05-31    Catherby             1
   6              Max Pure 2007-08-20  Port Sarim             1

   df_dt_aware:
               CustomerName                 BirthDate   Residence  IsAdventurer
   CustomerId
   1                Zezima 1990-07-14 02:00:00+02:00     Yanille             1
   2             Dr Harlow 1970-01-14 01:00:00+01:00     Varrock             0
   3                Baraek 1968-12-13 01:00:00+01:00     Varrock             0
   4            Gypsy Aris 1996-03-24 01:00:00+01:00     Varrock             0
   5             Not a Bot 2006-05-31 02:00:00+02:00    Catherby             1
   6              Max Pure 2007-08-20 02:00:00+02:00  Port Sarim             1

.. _Using a SQLContainer to organize SQL statements:

Using the SQLContainer
----------------------

The :class:`SQLContainer <pandemy.SQLContainer>` class is a container for the SQL statements used by an application. The database managers
can optionally be initalized with an :class:`SQLContainer <pandemy.SQLContainer>` through the keyword argument ``container``. 
:class:`SQLContainer <pandemy.SQLContainer>` is the base class and provides some useful methods. 
If you want to use an :class:`SQLContainer <pandemy.SQLContainer>` in your application you should sublcass from :class:`SQLContainer <pandemy.SQLContainer>`.
The SQL statements are stored as class variables on the :class:`SQLContainer <pandemy.SQLContainer>`. The previously used SQL statements may be stored
in an :class:`SQLContainer <pandemy.SQLContainer>` like this. 

.. only:: builder_html

   :download:`sql_container.py </examples/SQLite/sql_container.py>`


.. testcode:: sql_container

   # sql_container.py

   import pandemy


   class SQLiteSQLContainer(pandemy.SQLContainer):
      r""""A container of SQLite database statements."""

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

      insert_into_table_item = """
      INSERT INTO TABLE Item (ItemId, ItemName, MemberOnly, Description)
         VALUES (:itemid, :itemname, :memberonly, :description);
      """

      select_all_items = """SELECT * FROM Item ORDER BY ItemId ASC;"""


   db = pandemy.SQLiteDb(file='Runescape.db', container=SQLiteSQLContainer)

   with db.engine.connect() as conn:
      df = db.load_table(sql=db.container.select_all_items, conn=conn, index_col='ItemId')

   print(df)

.. code-block:: bash

   $ python sql_container.py

.. testoutput:: sql_container
   :options: +NORMALIZE_WHITESPACE

                      ItemName  MemberOnly                              Description
   ItemId                                                                          
   1                       Pot           0                       This pot is empty.
   2                       Jug           0                       This jug is empty.
   3                    Shears           0                      For shearing sheep.
   4                    Bucket           0                    It's a wooden bucket.
   5                      Bowl           0                Useful for mixing things.
   6           Amulet of glory           1      A very powerful dragonstone amulet.
   7                 Tinderbox           0              Useful for lighting a fire.
   8                    Chisel           0              Good for detailed Crafting.
   9                    Hammer           0                 Good for hitting things.
   10             Newcomer map           0  Issued to all new citizens of Gielinor.
   11          Unstrung symbol           0      It needs a string so I can wear it.
   12          Dragon Scimitar           1                 A vicious, curved sword.
   13          Amulet of glory           1      A very powerful dragonstone amulet.
   14              Ranarr seed           1   A ranarr seed - plant in a herb patch.
   15                Swordfish           0       I'd better be careful eating this!
   16      Red dragonhide Body           1          Made from 100% real dragonhide.


Replace placeholders
^^^^^^^^^^^^^^^^^^^^

The :meth:`replace_placeholders <pandemy.SQLContainer.replace_placeholders>` method of :class:`SQLContainer <pandemy.SQLContainer>`
is used to replace placeholders within a parametrized SQL statement. The purpose of this method is to handle the case of
a parametrized query using an *IN* clause with a variable number of arguments. The IN clause recieves
a single placeholder initially which can later be replaced by the correct amount of placeholders once
this is determined. The method can of course be used to replace any placeholder within a SQL statement.

The method takes the SQL statement and a single or a sequence of :data:`Placeholder <pandemy.Placeholder>` namedtuple.
It returns the SQL statement with replaced placeholders and a dictionary called ``params``. 
:data:`Placeholder <pandemy.Placeholder>` has 3 parameters:

1. ``key`` : The placeholder to replace e.g. *':myplaceholder'*.

2. ``values`` : A value or sequence of values to use for replacing the placeholder ``key``.

3. ``new_key`` : A boolean, where ``True`` indicates that :meth:`replace_placeholders <pandemy.SQLContainer.replace_placeholders>` should
return the new placeholders mapped to their respective value in ``values`` as a key value pair in the dictionary  ``params``. The dictionary  ``params``
can be passed to the ``params`` keyword argument of the :meth:`execute <pandemy.DatabaseManager.execute>` method of a DatabaseManager.
The default value is ``True``. A value of ``False`` causes the replaced placeholder to not appear in the  ``params`` dictionary.

The use of :meth:`replace_placeholders <pandemy.SQLContainer.replace_placeholders>` and :data:`Placeholder <pandemy.Placeholder>` namedtuple.
is best illustrated by some examples using the previously created database *Runescape.db*.

.. only:: builder_html

   :download:`replace_placeholder.py </examples/SQLite/replace_placeholder.py>`


.. testcode:: replace_placeholder

   # replace_placeholder.py

   import pandemy


   class SQLiteSQLContainer(pandemy.SQLContainer):
      r""""A container of SQLite database statements."""

      # Retrieve items from table Item by their ItemId
      get_items_by_id = """
      SELECT ItemId, ItemName, MemberOnly, Description
      FROM Item
      WHERE ItemId IN (:itemid)
      ORDER BY ItemId ASC;
      """
   

   items = [1, 3, 5]  # The items to retrieve from table Item

   # The placeholder with the replacement values
   placeholder = pandemy.Placeholder(key=':itemid', values=items, new_key=True)
   
   db = pandemy.SQLiteDb(file='Runescape.db', container=SQLiteSQLContainer)

   stmt, params = db.container.replace_placeholders(stmt=db.container.get_items_by_id, placeholders=placeholder)

   print(f'get_items_by_id after replacements:\n{stmt}\n')
   print(f'The new placeholders with mapped values:\n{params}\n')

   with db.engine.connect() as conn:
      df = db.load_table(sql=stmt, conn=conn, params=params, index_col='ItemId')
   
   print(f'The DataFrame from the parametrized query:\n{df}')

.. code-block:: bash

   $ python replace_placeholder.py


.. testoutput:: replace_placeholder
   :options: +NORMALIZE_WHITESPACE

   get_items_by_id after replacements:

      SELECT ItemId, ItemName, MemberOnly, Description
      FROM Item
      WHERE ItemId IN (:v0, :v1, :v2)
      ORDER BY ItemId ASC;
       
   The new placeholders with mapped values:
   {'v0': 1, 'v1': 3, 'v2': 5}

   The DataFrame from the parametrized query:
          ItemName  MemberOnly                Description
   ItemId                                                
   1           Pot           0         This pot is empty.
   3        Shears           0        For shearing sheep.
   5          Bowl           0  Useful for mixing things.


In this example the placeholder *':itemid'* of the query ``get_items_by_id`` is replaced by
three placeholders: *v1*, *v2* and *v3* (one for each of the values in the list ``items`` in the order they occur). 
Since ``new_key=True`` the returned dictionary ``params`` contains a mapping of the new placeholders to the 
values in the list  ``items``. If ``new_key=False`` then ``params`` would be an empty dictionary.
The updated version of the query ``get_items_by_id`` can then be executed with the parameters in ``params``. 

The next example shows how to replace multiple placeholders.

.. only:: builder_html

   :download:`replace_multiple_placeholders.py </examples/SQLite/replace_multiple_placeholders.py>`


.. testcode:: replace_multiple_placeholders

   # replace_multiple_placeholders.py

   import pandemy


   class SQLiteSQLContainer(pandemy.SQLContainer):
      r""""A container of SQLite database statements."""

      get_items_by_id = """
      SELECT ItemId, ItemName, MemberOnly, Description
      FROM Item
      WHERE 
         ItemId IN (:itemid)      AND
         MemberOnly = :memberonly AND
         Description LIKE :description
      ORDER BY :orderby;
      """
   

   items = [10, 12, 13, 14, 16]  # The items to retrieve from table Item

   # The placeholders with the replacement values
   placeholders = [
      pandemy.Placeholder(key=':itemid', values=items, new_key=True),
      pandemy.Placeholder(key=':memberonly', values=1, new_key=True),
      pandemy.Placeholder(key=':description', values='A%', new_key=True),
      pandemy.Placeholder(key=':orderby', values='ItemId DESC', new_key=False),
   ] 
   
   db = pandemy.SQLiteDb(file='Runescape.db', container=SQLiteSQLContainer)

   stmt, params = db.container.replace_placeholders(stmt=db.container.get_items_by_id, placeholders=placeholders)

   print(f'get_items_by_id after replacements:\n{stmt}\n')
   print(f'The new placeholders with mapped values:\n{params}\n')

   with db.engine.connect() as conn:
      df = db.load_table(sql=stmt, conn=conn, params=params, index_col='ItemId')
   
   print(f'The DataFrame from the parametrized query:\n{df}')


.. code-block:: bash

   $ python replace_multiple_placeholders.py


.. testoutput:: replace_multiple_placeholders
   :options: +NORMALIZE_WHITESPACE

   get_items_by_id after replacements:

      SELECT ItemId, ItemName, MemberOnly, Description
      FROM Item
      WHERE
         ItemId IN (:v0, :v1, :v2, :v3, :v4)      AND
         MemberOnly = :v5 AND
         Description LIKE :v6
      ORDER BY ItemId DESC;
      

   The new placeholders with mapped values:
   {'v0': 10, 'v1': 12, 'v2': 13, 'v3': 14, 'v4': 16, 'v5': 1, 'v6': 'A%'}

   The DataFrame from the parametrized query:
                  ItemName  MemberOnly                             Description
   ItemId
   14          Ranarr seed           1  A ranarr seed - plant in a herb patch.
   13      Amulet of glory           1     A very powerful dragonstone amulet.
   12      Dragon Scimitar           1                A vicious, curved sword.


.. note::

   The replacement value for the ``:orderby`` placeholder is not part of the returned ``params`` dictionary because ``new_key=False``.


.. warning::

   Replacing *':orderby'* by an arbitrary value that is not a placeholder is not safe against SQL injection attacks
   the way placeholders are and is therfore discouraged. The feature is there if it is needed,
   but be aware of its security limitations.
