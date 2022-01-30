*************
API reference
*************

This chapter explains the complete API of Pandemy.

Pandemy consists of two main classes: :class:`DatabaseManager <pandemy.DatabaseManager>` and :class:`SQLContainer <pandemy.SQLContainer>`.
Each SQL dialect is implemented as a subclass of :class:`DatabaseManager <pandemy.DatabaseManager>`. The :class:`SQLContainer <pandemy.SQLContainer>`
serves as a container of the SQL statements used by the :class:`DatabaseManager <pandemy.DatabaseManager>` of an application.

.. toctree::
   :maxdepth: 3
   :caption: Table of contents 

   databasemanager
   sqlcontainer
   exceptions
   attributes
