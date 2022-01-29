SQLContainer
============

The :class:`SQLContainer <pandemy.SQLContainer>` is a storage container for the SQL statements
used by a :class:`DatabaseManager <pandemy.DatabaseManager>` of an application.
It also provides the :meth:`replace_placeholders() <pandemy.SQLContainer.replace_placeholders>`
method for pre-processing of placeholders in a SQL statement before it is executed on the database.


.. autoclass:: pandemy.SQLContainer
   :members: replace_placeholders


Placeholder
-----------

Input to the :meth:`SQLContainer.replace_placeholders() <pandemy.SQLContainer.replace_placeholders>` method.

.. autoclass:: pandemy.Placeholder
