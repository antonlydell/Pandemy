Save a DataFrame to a table
---------------------------

Executing insert statements with the :meth:`DatabaseManager.execute() <pandemy.DatabaseManager.execute>` method as shown
above is not very practical if you have a lot of data in a :class:`pandas.DataFrame` that you want to save to a table.
In these cases it is better to use the method :meth:`DatabaseManager.save_df() <pandemy.DatabaseManager.save_df>`.
It uses the power of :meth:`pandas.DataFrame.to_sql` method and lets you save a :class:`pandas.DataFrame`
to a table in a single line of code.

The example below shows how to insert data to the table *Item* using a :class:`pandas.DataFrame`. 


.. only:: builder_html

   :download:`save_df.py <examples/save_df.py>`


.. The database Runescape.db with an empty table Item
.. testsetup:: save_df

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


:meth:`DatabaseManager.save_df() <pandemy.DatabaseManager.save_df>` implements all parameters of :meth:`pandas.DataFrame.to_sql`. 
The ``if_exists`` parameter is slightly different. ``if_exists`` controls how to save a :class:`pandas.DataFrame`
to an existing table in the database.

``if_exists`` accepts the following values:

* *'append'*: Append the :class:`pandas.DataFrame` to the existing table (default).

* *'replace'*: Delete all records from the table and then write the :class:`pandas.DataFrame` to the table.

* *'drop-replace'*: Drop the table, recreate it, and then write the :class:`pandas.DataFrame` to the table.

* *'fail'*: Raise :exc:`pandemy.TableExistsError` if the table exists.


.. versionadded:: 1.2.0
   'drop-replace'


In the :meth:`pandas.DataFrame.to_sql` method *'fail'* is the default value. The option *'replace'* drops the 
existing table, recreates it with the column definitions from the :class:`pandas.DataFrame`, and inserts the data.
By dropping the table and recreating it you loose important information such as primary keys and constraints.

In :meth:`DatabaseManager.save_df() <pandemy.DatabaseManager.save_df>` *'replace'* deletes all current records before inserting
the new data rather than dropping the table. This preserves the existing columns definitions and constraints of the table.
Deleting the current records is done with the 
:meth:`DatabaseManager.delete_all_records_from_table() <pandemy.DatabaseManager.delete_all_records_from_table>` method.
The *'drop-replace'* option is the equivalent of *'replace'* in :meth:`pandas.DataFrame.to_sql`.
