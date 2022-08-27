Update and insert data into a table (upsert)
--------------------------------------------

Sometimes you have a :class:`pandas.DataFrame` that represents an existing table in the database that already contains data.
If you want to update the existing records of the table with data from the :class:`pandas.DataFrame` and also insert new rows
(that exist in the :class:`pandas.DataFrame` but not in the table) you should use the 
:meth:`DatabaseManager.upsert_table() <pandemy.DatabaseManager.upsert_table>` (update and insert) method. The method can update
and insert new rows or only update existing rows. It works by creating and executing an UPDATE statement followed by an optional
INSERT statement derived from the structure of the :class:`pandas.DataFrame`. Let's look at some examples of using
:meth:`DatabaseManager.upsert_table() <pandemy.DatabaseManager.upsert_table>` with the *Customer* table of *Runescape.db*.


.. only:: builder_html

   :download:`upsert_table.py <examples/upsert_table.py>`


.. testsetup:: upsert_table
   
   import io
   import pandas as pd 
   import pandemy


   # SQL statement to create the table Customer and insert data into it
   create_table_customer = """
   -- Customers that have traded in a General Store
   CREATE TABLE IF NOT EXISTS Customer (
      CustomerId         INTEGER, 
      CustomerName       TEXT    NOT NULL,
      BirthDate          TEXT,
      Residence          TEXT,
      IsAdventurer       INTEGER NOT NULL, -- 1 if Adventurer and 0 if NPC 

      CONSTRAINT CustomerPk PRIMARY KEY (CustomerId),

      CONSTRAINT IsAdventurerBool CHECK (IsAdventurer IN (0, 1))
   );
   """

   db = pandemy.SQLiteDb(file='Runescape.db')  # Create the SQLite DatabaseManager instance

   data = io.StringIO(r"""
   CustomerId;CustomerName;BirthDate;Residence;IsAdventurer
   1;Zezima;1990-07-14;Yanille;1
   2;Dr Harlow;1970-01-14;Varrock;0
   3;Baraek;1968-12-13;Varrock;0
   4;Gypsy Aris;1996-03-24;Varrock;0
   5;Not a Bot;2006-05-31;Catherby;1
   6;Max Pure;2007-08-20;Port Sarim;1
   """)

   df = pd.read_csv(filepath_or_buffer=data, sep=';', index_col='CustomerId')  # Create the DataFrame

   with db.engine.begin() as conn:
      db.execute(sql=create_table_customer, conn=conn)
      db.save_df(df=df, table='Customer', conn=conn)


.. code-block:: bash

   $ python upsert_table.py 


.. testcode:: upsert_table

   # upsert_table.py

   from datetime import datetime
   import pandas as pd
   import pandemy

   db = pandemy.SQLiteDb(file='Runescape.db', must_exist=True)

   query = """SELECT * FROM Customer ORDER BY CustomerId ASC"""

   dtypes = {
      'CustomerName': 'string',
      'Residence': 'string',
      'IsAdventurer': 'boolean'
   }

   with db.engine.connect() as conn:
      df = db.load_table(
         sql=query,
         conn=conn,
         index_col='CustomerId',
         dtypes=dtypes,
         parse_dates=['BirthDate']
      )

   print(f'Customer table original:\n\n{df}')

   # Change some data
   df.loc[1, ['BirthDate', 'Residence']] = [datetime(1891, 7, 15), 'Falador']
   df.loc[4, 'IsAdventurer'] = True

   # Add new data
   df.loc[9, :] = ['Prince Ali', datetime(1969, 6, 20), 'Al Kharid', False]
   df.loc[10, :] = ['Mosol Rei', datetime(1983, 4, 30), 'Shilo Village', False]

   with db.engine.begin() as conn:
      # Update and insert the new data
      db.upsert_table(
         df=df,
         table='Customer',
         conn=conn,
         where_cols=['CustomerName'],
         update_index_cols=False,
         update_only=False,
         datetime_cols_dtype='str',
         datetime_format=r'%Y-%m-%d'
      )

      # Load the data back
      df_upsert = db.load_table(
         sql=query,
         conn=conn,
         index_col='CustomerId',
         dtypes=dtypes,
         parse_dates=['BirthDate']
      )

   print(f'\n\nCustomer table after upsert:\n\n{df_upsert}')


.. testoutput:: upsert_table
   :options: +NORMALIZE_WHITESPACE

   Customer table original:

              CustomerName   BirthDate   Residence  IsAdventurer
   CustomerId
   1                Zezima  1990-07-14     Yanille          True 
   2             Dr Harlow  1970-01-14     Varrock         False 
   3                Baraek  1968-12-13     Varrock         False
   4            Gypsy Aris  1996-03-24     Varrock         False
   5             Not a Bot  2006-05-31    Catherby          True
   6              Max Pure  2007-08-20  Port Sarim          True


   Customer table after upsert:

              CustomerName  BirthDate      Residence  IsAdventurer
   CustomerId
   1                Zezima 1891-07-15        Falador          True
   2             Dr Harlow 1970-01-14        Varrock         False
   3                Baraek 1968-12-13        Varrock         False
   4            Gypsy Aris 1996-03-24        Varrock          True
   5             Not a Bot 2006-05-31       Catherby          True
   6              Max Pure 2007-08-20     Port Sarim          True
   7            Prince Ali 1969-06-20      Al Kharid         False
   8             Mosol Rei 1983-04-30  Shilo Village         False


The *Customer* table is loaded from the database into a :class:`pandas.DataFrame` (``df``). The data is modified and two new rows
are added to ``df``. :meth:`DatabaseManager.upsert_table() <pandemy.DatabaseManager.upsert_table>` is called with the updated
version of ``df``. The ``where_cols`` parameter is set to the *CustomerName* column which means that rows from ``df``
with a *CustomerName* that already exists in the *Customer* table will be updated. Rows that do not have a matching
*CustomerName* will be inserted instead. The *BirthDate* column is inserted as formatted strings (YYYY-MM-DD) by the
parameters ``datetime_cols_dtype='str'`` and ``datetime_format=r'%Y-%m-%d'``. Setting the parameter ``update_only=True`` would have
only updated existing rows and not inserted any new rows.

.. note::

   The Primary key column *CustomerId* is not included in database statements sent to the database in the example above.
   This is due to the parameter ``update_index_cols=False``, which is also the default behavior.
   The values of *CustomerId* in ``df`` of the two new rows (9 and 10) differ from the ones inserted into the database
   (7 and 8). The *CustomerId* column is defined as an INTEGER data type in the database and if it is not supplied
   in the INSERT statement SQLite will autoincrement the value by one from the previous row.
   It is useful to exclude the Primary key from the upsert if it is generated by the database.


Using the dry_run parameter
^^^^^^^^^^^^^^^^^^^^^^^^^^^

If you want to look at the SQL statements sent to the database you can set the parameter ``dry_run=True``.
This will return the SQL statements that **would have been** executed on the database for every row in ``df``.
Nothing gets executed on the database. This is useful to verify that you have set the parameters correct to make
the statements do what you expect. If ``update_only=True`` the returned INSERT statement will be ``None``.

The next example illustrates using the ``dry_run`` parameter with the *Customer* table from before.


.. only:: builder_html

   :download:`upsert_table_dry_run.py <examples/upsert_table_dry_run.py>`


.. code-block:: bash

   $ python upsert_table_dry_run.py


.. testcode:: upsert_table

   # upsert_table_dry_run.py

   from datetime import datetime
   import pandas as pd
   import pandemy

   db = pandemy.SQLiteDb(file='Runescape.db', must_exist=True)

   query = """SELECT * FROM Customer ORDER BY CustomerId ASC"""

   dtypes = {
      'CustomerName': 'string',
      'Residence': 'string',
      'IsAdventurer': 'boolean'
   }

   with db.engine.connect() as conn:
      df = db.load_table(
         sql=query,
         conn=conn,
         index_col='CustomerId',
         dtypes=dtypes,
         parse_dates=['BirthDate']
      )

   print(f'Customer table:\n\n{df}')

   with db.engine.begin() as conn:
      # Get the UPDATE and INSERT statements
      update_stmt, insert_stmt = db.upsert_table(
         df=df,
         table='Customer',
         conn=conn,
         where_cols=['CustomerName', 'BirthDate'],
         update_cols=['Residence', 'CustomerName'],
         update_index_cols=True,
         update_only=False,
         dry_run=True
      )

   print(f'\n\nUPDATE statement:\n\n{update_stmt}')
   print(f'INSERT statement:\n\n{insert_stmt}')


.. testoutput:: upsert_table
   :options: +NORMALIZE_WHITESPACE

   Customer table:

              CustomerName  BirthDate      Residence  IsAdventurer
   CustomerId
   1                Zezima 1891-07-15        Falador          True
   2             Dr Harlow 1970-01-14        Varrock         False
   3                Baraek 1968-12-13        Varrock         False
   4            Gypsy Aris 1996-03-24        Varrock          True
   5             Not a Bot 2006-05-31       Catherby          True
   6              Max Pure 2007-08-20     Port Sarim          True
   7            Prince Ali 1969-06-20      Al Kharid         False
   8             Mosol Rei 1983-04-30  Shilo Village         False


   UPDATE statement:

   UPDATE Customer
   SET
       Residence = :Residence,
       CustomerId = :CustomerId
   WHERE
       CustomerName = :CustomerName AND
       BirthDate = :BirthDate
   
   INSERT statement:

   INSERT INTO Customer (
       Residence,
       CustomerName,
       CustomerId
   )
       SELECT
           :Residence,
           :CustomerName,
           :CustomerId
       WHERE
           NOT EXISTS (
               SELECT
                   1
               FROM Customer
                   WHERE
                       CustomerName = :CustomerName AND
                       BirthDate = :BirthDate
           )


Here we use the columns *CustomerName* and *BirthDate* in the WHERE clause and specify that the index column (*CustomerId*) 
should also be updated. If the index is a :class:`pandas.MultiIndex` all levels are included if ``update_index_cols=True``.
A list of level names can be used to only select desired levels of the index to the upsert. By specifying the ``update_cols``
parameter a subset of the columns of ``df`` can be selected for the upsert, in this case the columns *Residence* and *CustomerName*.
Since *CustomerName* is also supplied to the ``where_cols`` parameter it is excluded from the columns to update,
because that would otherwise result in an update to the same value.
