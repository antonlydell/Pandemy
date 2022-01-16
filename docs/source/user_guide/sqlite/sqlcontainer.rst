.. _Using a SQLContainer to organize SQL statements:

Using the SQLContainer
----------------------

The :class:`SQLContainer <pandemy.SQLContainer>` class is a container for the SQL statements used by an application.
The database managers can optionally be initialized with a :class:`SQLContainer <pandemy.SQLContainer>` through the
keyword argument ``container``. :class:`SQLContainer <pandemy.SQLContainer>` is the base class and provides some useful methods. 
If you want to use a :class:`SQLContainer <pandemy.SQLContainer>` in your application you should subclass from
:class:`SQLContainer <pandemy.SQLContainer>`. The SQL statements are stored as class variables on the
:class:`SQLContainer <pandemy.SQLContainer>`. The previously used SQL statements may be stored
in a :class:`SQLContainer <pandemy.SQLContainer>` like this. 


.. only:: builder_html

   :download:`sql_container.py <examples/sql_container.py>`


.. testsetup:: sql_container, replace_placeholder, replace_multiple_placeholders

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

The :meth:`SQLContainer.replace_placeholders <pandemy.SQLContainer.replace_placeholders>` method is used
to replace placeholders within a parametrized SQL statement. The purpose of this method is to handle the
case of a parametrized query using an *IN* clause with a variable number of arguments. The *IN* clause receives
a single placeholder initially which can later be replaced by the correct amount of placeholders once
this is determined. The method can of course be used to replace any placeholder within a SQL statement.

The method takes the SQL statement and a single or a sequence of :class:`Placeholder <pandemy.Placeholder>`.
It returns the SQL statement with replaced placeholders and a dictionary called ``params``. 
:class:`Placeholder <pandemy.Placeholder>` has 3 parameters:

1. ``key`` : The placeholder to replace e.g. ``':myplaceholder'``.

2. ``values`` : A value or sequence of values to use for replacing the placeholder ``key``.

3. ``new_key`` : A boolean, where ``True`` indicates that :meth:`replace_placeholders <pandemy.SQLContainer.replace_placeholders>`
should return the new placeholders mapped to their respective value(s) in ``values`` as a key value pair in the dictionary  ``params``.
The dictionary  ``params`` can be passed to the ``params`` keyword argument of the :meth:`execute <pandemy.DatabaseManager.execute>` 
or :meth:`load_table <pandemy.DatabaseManager.load_table>` methods of a :class:`DatabaseManager <pandemy.DatabaseManager>`.
The default value is ``True``. A value of ``False`` causes the replaced placeholder to not appear in the returned  ``params`` dictionary.

The use of :meth:`replace_placeholders <pandemy.SQLContainer.replace_placeholders>` and :data:`Placeholder <pandemy.Placeholder>`
is best illustrated by some examples using the previously created database *Runescape.db*.


.. only:: builder_html

   :download:`replace_placeholder.py <examples/replace_placeholder.py>`


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


In this example the placeholder *:itemid* of the query ``get_items_by_id`` is replaced by
three placeholders: *:v0*, *:v1* and *:v2* (one for each of the values in the list ``items`` in the order they occur). 
Since ``new_key=True`` the returned dictionary ``params`` contains a mapping of the new placeholders to the 
values in the list  ``items``. If ``new_key=False`` then ``params`` would be an empty dictionary.
The updated version of the query ``get_items_by_id`` can then be executed with the parameters in ``params``. 

The next example shows how to replace multiple placeholders.


.. only:: builder_html

   :download:`replace_multiple_placeholders.py <examples/replace_multiple_placeholders.py>`


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

   The replacement value for the *:orderby* placeholder is not part of the returned ``params`` dictionary because ``new_key=False``
   for the last placeholder.


.. warning::

   Replacing *:orderby* by an arbitrary value that is not a placeholder is not safe against SQL injection attacks
   the way placeholders are and is therefore discouraged. The feature is there if it is needed,
   but be aware of its security limitations.
