The execute method
------------------

The :meth:`DatabaseManager.execute <pandemy.DatabaseManager.execute>` method can be used to execute arbitrary
SQL statements e.g. creating a table. Let us create a SQLite database that can be used to further
demonstrate how Pandemy works.

The complete SQLite test database that is used for the test suite of Pandemy can be downloaded
below to test Pandemy on your own.

.. only:: builder_html

   :download:`Runescape.db <../../../../tests/static_files/SQLite/Runescape.db>` 


.. only:: builder_html

   :download:`create_database.py <examples/create_database.py>` 


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
is executed. The placeholders should be prefixed by a colon (*:*) (e.g. ``:myparameter``) in the SQL statement.
The parameter ``params`` takes a dictionary that maps the parameter name to its value(s) or a list of such dictionaries.
Note that parameter names in the dictionary should not be prefixed with a colon i.e. the key in the dictionary
that references the SQL placeholder ``:myparameter`` should be named ``'myparameter'``.

Let's insert some data into the *Item* table we just created in *Runescape.db*.

.. The database Runescape.db with an empty table Item
.. testsetup:: execute_insert_into, execute_insert_into_transaction

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

   :download:`execute_insert_into.py <examples/execute_insert_into.py>` 


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


.. note::

   The database connection must remain open while iterating over the rows in the
   :class:`CursorResult <sqlalchemy:sqlalchemy.engine.CursorResult>` object,
   since it is fetching one row from the database at the time.
   This means that the for loop must be placed inside the context manager.


Using transactions
^^^^^^^^^^^^^^^^^^

Database transactions can be invoked by calling the :meth:`begin <sqlalchemy:sqlalchemy.engine.Engine.begin>` method of the
database engine instead of :meth:`connect <sqlalchemy:sqlalchemy.engine.Engine.connect>`. When executing SQL statements under
an open transaction all statements will automatically be rolled back to the latest valid state if an error occurs in one of the
statements. This differs from using the connect method where only the statement where the error occurs will be rolled back.
The example below illustrates this difference.


.. only:: builder_html

   :download:`execute_insert_into_transaction.py <examples/execute_insert_into_transaction.py>`


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

   The :meth:`fetchall <sqlalchemy:sqlalchemy.engine.CursorResult.fetchall>` method of the
   :class:`CursorResult <sqlalchemy:sqlalchemy.engine.CursorResult>` object can be used to
   retrieve all rows from the query into a list.


.. warning::

   The method :meth:`delete_all_records_from_table <pandemy.DatabaseManager.delete_all_records_from_table>` will delete all records from a table.
   Use this method with caution. It is mainly used to clear all content of a table before replacing it with new data. This method is used by
   the :meth:`save_df <pandemy.DatabaseManager.save_df>` method when using ``if_exists='replace'``, which is described in the next section.


.. seealso::

   The SQLAlchemy documentation provides more information about transactions: 

   * :meth:`sqlalchemy:sqlalchemy.engine.Engine.begin` : Establish a database connection with a transaction.

   * :meth:`sqlalchemy:sqlalchemy.engine.Engine.connect` : Establish a database connection.

   * :class:`sqlalchemy:sqlalchemy.engine.Transaction` : A database transaction object.
