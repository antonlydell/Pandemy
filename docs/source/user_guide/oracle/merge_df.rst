Merge a DataFrame into a table
------------------------------

The :meth:`DatabaseManager.merge_df() <pandemy.DatabaseManager.merge_df>` method lets you take a :class:`pandas.DataFrame`
and update existing and insert new rows into a table based on some criteria. It executes a combined UPDATE and INSERT statement
-- a MERGE statement. The MERGE statement is executed once for every row in the :class:`pandas.DataFrame`. The method and its API
is very similar to :meth:`DatabaseManager.upsert_table() <pandemy.DatabaseManager.upsert_table>`. 
See the section :ref:`user_guide/sqlite/upsert_table:Update and insert data into a table (upsert)`
in the :doc:`SQLite User guide <../sqlite/index>`.

An example of using :meth:`DatabaseManager.merge_df() <pandemy.DatabaseManager.merge_df>` is shown below with the *Item* table
from *Runescape.db*. Setting the parameter ``dry_run=True`` will return the generated MERGE statement as a string instead of
executing it on the database. To avoid having to connect to a real Oracle database server the :meth:`begin() <sqlalchemy.engine.Engine.begin>`
method of the :class:`DatabaseManager.engine <sqlalchemy.engine.Engine>` is mocked.


.. only:: builder_html

   :download:`Runescape.db <../../../../tests/static_files/SQLite/Runescape.db>` 


.. only:: builder_html

   :download:`merge_df.py <examples/merge_df.py>`


.. testsetup:: merge_df
   
   import io
   import pandas as pd 
   import pandemy

   # SQL statement to create the table Item
   create_table_item = """
   -- The available items in General Stores
   CREATE TABLE IF NOT EXISTS Item (
      ItemId      INTEGER,
      ItemName    TEXT    NOT NULL,
      MemberOnly  INTEGER NOT NULL,
      Description TEXT,

      CONSTRAINT ItemPk PRIMARY KEY (ItemId),

      -- Validation
      CONSTRAINT MemberOnlyBool CHECK (MemberOnly IN (0,1))
   );
   """

   db = pandemy.SQLiteDb(file='Runescape.db')  # Create the SQLite DatabaseManager instance

   data = io.StringIO(r"""
   ItemId;ItemName;MemberOnly;Description
   1;Pot;0;This pot is empty.
   2;Jug;0;This jug is empty.
   3;Shears;0;For shearing sheep.
   4;Bucket;0;Its a wooden bucket.
   5;Bowl;0;Useful for mixing things.
   6;Cake tin;0;Useful for baking things.
   7;Tinderbox;0;Useful for lighting a fire.
   8;Chisel;0;Good for detailed Crafting.
   9;Hammer;0;Good for hitting things.
   10;Newcomer map;0;Issued to all new citizens of Gielinor.
   11;Unstrung symbol;0;It needs a string so I can wear it.
   12;Dragon Scimitar;1;A vicious, curved sword.
   13;Amulet of glory;1;A very powerful dragonstone amulet.
   14;Ranarr seed;1;A ranarr seed - plant in a herb patch.
   15;Swordfish;0;Id better be careful eating this!
   16;Red dragonhide Body;1;Made from 100% real dragonhide.
   """)

   df = pd.read_csv(filepath_or_buffer=data, sep=';', index_col='ItemId')  # Create the DataFrame

   with db.engine.begin() as conn:
      db.execute(sql=create_table_item, conn=conn)
      db.save_df(df=df, table='Item', conn=conn)


.. testcode:: merge_df

   # merge_df.py

   from datetime import datetime
   from unittest.mock import MagicMock

   import pandas as pd
   import pandemy

   db_sqlite = pandemy.SQLiteDb(file='Runescape.db', must_exist=True)

   query = """SELECT * FROM Item ORDER BY ItemId ASC"""

   dtypes = {
      'ItemId': 'int8',
      'ItemName': 'string',
      'MemberOnly': 'boolean',
      'Description': 'string'
   }

   with db_sqlite.engine.connect() as conn:
      df = db_sqlite.load_table(
         sql=query,
         conn=conn,
         index_col=['ItemId'],
         dtypes=dtypes,
      )

   print(f'Item table:\n\n{df}')

   # Change some data
   df.loc[1, 'Description'] = 'This pot is not empty!' 
   df.loc[12, ['ItemName', 'MemberOnly']] = ['Dragon Super Scimitar', False]

   # Add new data
   df.loc[17, :] = ['Coal', False, 'Hmm a non-renewable energy source!']
   df.loc[18, :] = ['Redberry pie', False, 'Looks tasty.']

   db_oracle = pandemy.OracleDb(
      username='Fred_the_Farmer',
      password='Penguins-sheep-are-not',
      host='fred.farmer.rs',
      port=1234,
      service_name='woollysheep'
   )

   db_oracle.engine.begin = MagicMock()  # Mock the begin method

   print(f'\n\nRows to be updated or inserted:\n\n{df.loc[[1, 12, 17, 18], :]}')

   with db_oracle.engine.begin() as conn:
      merge_stmt = db_oracle.merge_df(
         df=df,
         table='Item',
         conn=conn,
         on_cols=['ItemName'],
         merge_cols='all',
         merge_index_cols=False,
         dry_run=True
      )

   print(f'\n\nMERGE statement:\n\n{merge_stmt}')


.. code-block:: bash

   $ python merge_df.py


.. testoutput:: merge_df
   :options: +NORMALIZE_WHITESPACE

   Item table:

                      ItemName  MemberOnly                              Description
   ItemId
   1                       Pot       False                       This pot is empty.
   2                       Jug       False                       This jug is empty.
   3                    Shears       False                      For shearing sheep.
   4                    Bucket       False                     Its a wooden bucket.
   5                      Bowl       False                Useful for mixing things.
   6                  Cake tin       False                Useful for baking things.
   7                 Tinderbox       False              Useful for lighting a fire.
   8                    Chisel       False              Good for detailed Crafting.
   9                    Hammer       False                 Good for hitting things.
   10             Newcomer map       False  Issued to all new citizens of Gielinor.
   11          Unstrung symbol       False      It needs a string so I can wear it.
   12          Dragon Scimitar        True                 A vicious, curved sword.
   13          Amulet of glory        True      A very powerful dragonstone amulet.
   14              Ranarr seed        True   A ranarr seed - plant in a herb patch.
   15                Swordfish       False        Id better be careful eating this!
   16      Red dragonhide Body        True          Made from 100% real dragonhide.


   Rows to be updated or inserted:

                        ItemName  MemberOnly                         Description
   ItemId
   1                         Pot       False              This pot is not empty!
   12      Dragon Super Scimitar       False            A vicious, curved sword.
   17                       Coal       False  Hmm a non-renewable energy source!
   18               Redberry pie       False                        Looks tasty.


   MERGE statement:

   MERGE INTO Item t

   USING (
      SELECT
         :ItemName AS ItemName,
         :MemberOnly AS MemberOnly,
         :Description AS Description
      FROM DUAL
   ) s

   ON (
      t.ItemName = s.ItemName
   )

   WHEN MATCHED THEN
      UPDATE
      SET
         t.MemberOnly = s.MemberOnly,
         t.Description = s.Description

   WHEN NOT MATCHED THEN
      INSERT (
         t.ItemName,
         t.MemberOnly,
         t.Description
      )
      VALUES (
         s.ItemName,
         s.MemberOnly,
         s.Description
      )


The *Item* table is loaded from the database into a :class:`pandas.DataFrame` (``df``). Two rows are modified and two new rows are
added. :meth:`DatabaseManager.merge_df() <pandemy.DatabaseManager.merge_df>` is called with ``df`` and ``dry_run=True``. The parameter
``merge_cols='all'`` includes all columns from ``df`` in the MERGE statement, which is the default. It also accepts a list of column
names to only include a subset of the columns. ``merge_index_cols=False`` excludes the index column from the statement, which is also
the default.

In the returned MERGE statement the ``t`` (target) alias refers to the *Item* table in the database and the ``s`` (source) alias
to ``df``. When a value of the *ItemName* column in the database matches a value from the *ItemName* column of  ``df`` an UPDATE
statement is executed to update the *MemberOnly* and *Description* columns. The *ItemName* column does not get updated since it is part
of the ON clause and would mean an update to the same value. When there is no match the values of the columns *ItemName*, *MemberOnly*
and *Description* are inserted into their respective column counterparts of the *Item* table.

Oracle supports adding a WHERE clause to the UPDATE clause of the WHEN MATCHED THEN part. This can be controlled with
the parameter ``omit_update_where_clause``, which defaults to ``True``. If set to ``False`` the columns to update will not
be updated if their values from ``df`` are the same as in the database. If at least one value differs the update will be executed.
The next example illustrates this and also uses two columns in the ON clause (*ItemId* and *ItemName*). This time ``df`` is loaded
with a :class:`pandas.MultiIndex`. Setting ``merge_index_cols=True`` includes all column levels of the :class:`pandas.MultiIndex`
in the MERGE statement. You can also supply a list of column level names to only include the desired index levels.


.. only:: builder_html

   :download:`merge_df_omit_update_where_clause.py <examples/merge_df_omit_update_where_clause.py>`


.. testcode:: merge_df

   # merge_df_omit_update_where_clause.py

   from datetime import datetime
   from unittest.mock import MagicMock

   import pandas as pd
   import pandemy

   db_sqlite = pandemy.SQLiteDb(file='Runescape.db', must_exist=True)

   query = """SELECT * FROM Item ORDER BY ItemId ASC"""

   dtypes = {
      'ItemId': 'int8',
      'ItemName': 'string',
      'MemberOnly': 'boolean',
      'Description': 'string'
   }

   with db_sqlite.engine.connect() as conn:
      df = db_sqlite.load_table(
         sql=query,
         conn=conn,
         index_col=['ItemId', 'ItemName'],
         dtypes=dtypes,
      )

   db_oracle = pandemy.OracleDb(
      username='Fred_the_Farmer',
      password='Penguins-sheep-are-not',
      host='fred.farmer.rs',
      port=1234,
      service_name='woollysheep'
   )

   db_oracle.engine.begin = MagicMock()  # Mock the begin method

   with db_oracle.engine.begin() as conn:
      merge_stmt = db_oracle.merge_df(
         df=df,
         table='Item',
         conn=conn,
         on_cols=['ItemId', 'ItemName'],
         merge_cols='all',
         merge_index_cols=True,
         omit_update_where_clause=False,
         dry_run=True
      )

   print(f'MERGE statement:\n\n{merge_stmt}')


.. code-block:: bash

   $ python merge_df_omit_update_where_clause.py


.. testoutput:: merge_df
   :options: +NORMALIZE_WHITESPACE

   MERGE statement:

   MERGE INTO Item t

   USING (
      SELECT
         :MemberOnly AS MemberOnly,
         :Description AS Description,
         :ItemId AS ItemId,
         :ItemName AS ItemName
      FROM DUAL
   ) s

   ON (
      t.ItemId = s.ItemId AND
      t.ItemName = s.ItemName
   )

   WHEN MATCHED THEN
      UPDATE
      SET
         t.MemberOnly = s.MemberOnly,
         t.Description = s.Description
      WHERE
         t.MemberOnly <> s.MemberOnly OR
         t.Description <> s.Description

   WHEN NOT MATCHED THEN
      INSERT (
         t.MemberOnly,
         t.Description,
         t.ItemId,
         t.ItemName
      )
      VALUES (
         s.MemberOnly,
         s.Description,
         s.ItemId,
         s.ItemName
      )


.. tip::

   If the MERGE includes a lot of columns and the statement needs to be tailored to suit a specific use case it is error prone to
   type all column names into the statement by hand. Using ``dry_run=True`` is useful to extract a template of the MERGE
   statement that can be further manually edited and parametrized. An empty :class:`pandas.DataFrame` representing the table the MERGE
   acts on is enough to get a template statement. The final statement can then be added to a :class:`SQLContainer <pandemy.SQLContainer>`
   and executed with the :meth:`DatabaseManager.execute() <pandemy.DatabaseManager.execute>` method.
   

.. seealso::

   `Diving into Oracle MERGE Statement`_ : A short and informative tutorial.

.. _Diving into Oracle MERGE Statement: https://www.oracletutorial.com/oracle-basics/oracle-merge/
