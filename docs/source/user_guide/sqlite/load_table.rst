Load a DataFrame from a table
-----------------------------

To load data from a table into a :class:`pandas.DataFrame` the
:meth:`DatabaseManager.load_table() <pandemy.DatabaseManager.load_table>` method
is used. It uses the :func:`pandas.read_sql` function with some extra features.

Let us load the table *Item* back into a :class:`pandas.DataFrame`.


.. testsetup:: load_table

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

   :download:`load_table.py <examples/load_table.py>`


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

Columns with datetime information can be converted into datetime columns by using the ``parse_dates`` keyword argument,
which is a direct link to the ``parse_dates`` option of :func:`pandas.read_sql` function.

``parse_dates`` only returns naive datetime columns. To load datetime columns with timezone information the keyword arguments 
``localize_tz`` and ``target_tz`` can be specified. ``localize_tz`` lets you localize the the naive datetime columns to a specified
timezone and ``target_tz`` can optionally convert the localized datetime columns into a desired timezone. 

Let's create the table *Customer* from the database *Runescape.db* and load it into a :class:`pandas.DataFrame` to illustrate this.


.. only:: builder_html

   :download:`load_table_localize_tz.py <examples/load_table_localize_tz.py>`


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

   dtypes = {
      'CustomerId': 'int8',
      'CustomerName': 'string',
      'Residence': 'string',
      'IsAdventurer': 'boolean'
   }

   df = pd.read_csv(filepath_or_buffer=data, sep=';', index_col='CustomerId', dtype=dtypes)

   with db.engine.begin() as conn:
      db.execute(sql=create_table_customer, conn=conn)
      db.save_df(df=df, table='Customer', conn=conn, if_exists='replace')

      df_naive = db.load_table(
         sql='Customer',
         conn=conn,
         index_col='CustomerId',
         dtypes=dtypes,
         parse_dates={'BirthDate': r'%Y-%m-%d'}
      )

      df_dt_aware = db.load_table(
         sql='Customer',
         conn=conn,
         index_col='CustomerId',
         dtypes=dtypes,
         parse_dates={'BirthDate': r'%Y-%m-%d'},
         localize_tz='UTC',
         target_tz='CET'
      )

   print(f'df:\n{df}\n')

   print(f'df_naive:\n{df_naive}\n')
   print(f'df_naive.dtypes:\n{df_naive.dtypes}\n')

   print(f'df_dt_aware:\n{df_dt_aware}\n')
   print(f'df_dt_aware.dtypes:\n{df_dt_aware.dtypes}')


.. code-block:: bash

   $ python load_table_localize_tz.py


.. testoutput:: load_table_localize_tz
   :options: +NORMALIZE_WHITESPACE

   df:
              CustomerName  BirthDate   Residence  IsAdventurer
   CustomerId
   1                Zezima 1990-07-14     Yanille          True
   2             Dr Harlow 1970-01-14     Varrock         False
   3                Baraek 1968-12-13     Varrock         False
   4            Gypsy Aris 1996-03-24     Varrock         False
   5             Not a Bot 2006-05-31    Catherby          True
   6              Max Pure 2007-08-20  Port Sarim          True

   df_naive:
               CustomerName  BirthDate   Residence  IsAdventurer
   CustomerId
   1                Zezima 1990-07-14     Yanille           True
   2             Dr Harlow 1970-01-14     Varrock          False
   3                Baraek 1968-12-13     Varrock          False
   4            Gypsy Aris 1996-03-24     Varrock          False
   5             Not a Bot 2006-05-31    Catherby           True
   6              Max Pure 2007-08-20  Port Sarim           True

   df_naive.dtypes:
   CustomerName            string
   BirthDate       datetime64[ns]
   Residence               string
   IsAdventurer           boolean
   dtype: object

   df_dt_aware:
               CustomerName                 BirthDate   Residence  IsAdventurer
   CustomerId
   1                Zezima 1990-07-14 02:00:00+02:00     Yanille           True
   2             Dr Harlow 1970-01-14 01:00:00+01:00     Varrock          False
   3                Baraek 1968-12-13 01:00:00+01:00     Varrock          False
   4            Gypsy Aris 1996-03-24 01:00:00+01:00     Varrock          False
   5             Not a Bot 2006-05-31 02:00:00+02:00    Catherby           True
   6              Max Pure 2007-08-20 02:00:00+02:00  Port Sarim           True

   df_dt_aware.dtypes:
   CustomerName                 string
   BirthDate       datetime64[ns, CET]
   Residence                    string
   IsAdventurer                boolean
   dtype: object