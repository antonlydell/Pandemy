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
