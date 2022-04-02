Installation
============

Pandemy is available for installation through `PyPI`_ using `pip`_ and the source code is hosted on GitHub at: https://github.com/antonlydell/Pandemy

Install Pandemy by running:


.. code-block:: bash

   $ pip install Pandemy


Dependencies
------------

The core dependencies of Pandemy are:

- pandas_ : powerful Python data analysis toolkit
- SQLAlchemy_ : The Python SQL Toolkit and Object Relational Mapper


Optional dependencies
^^^^^^^^^^^^^^^^^^^^^

Databases except for SQLite_ require a third-party database driver package to be installed.
SQLAlchemy uses the database driver to communicate with the database.
The default database driver for SQLite is built-in to Python in the :mod:`python:sqlite3` module [#fn1]_. 
The optional dependencies of Pandemy can be installed by supplying an `optional dependency identifier`_
to the `pip`_ installation command. The table below lists database driver packages for supported databases
and their corresponding optional dependency identifier.


.. csv-table:: Optional dependencies of Pandemy.
   :delim: ;
   :header-rows: 1
   :align: left
   :name: table_optional_dependencies 

   Database;Driver package;Optional dependency identifier;Version added
   :ref:`Oracle <user_guide/oracle/index:Oracle>`;cx_Oracle_;oracle; 1.1.0


To install `cx_Oracle`_ together with Pandemy run:


.. code-block:: bash

   $ pip install Pandemy[oracle]


.. rubric:: Footnotes

.. [#fn1] There are other drivers for SQLite, see `SQLite drivers in the SQLAlchemy documentation`_. 


.. _cx_Oracle: https://oracle.github.io/python-cx_Oracle/
.. _Microsoft SQL Server: https://www.microsoft.com/en-us/sql-server/sql-server-downloads
.. _optional dependency identifier: https://setuptools.pypa.io/en/latest/userguide/dependency_management.html#optional-dependencies
.. _Oracle: https://www.oracle.com/database/
.. _pandas: https://pandas.pydata.org/
.. _pip: https://pip.pypa.io/en/stable/getting-started/
.. _PyPI: https://pypi.org/project/Pandemy/
.. _SQLAlchemy: https://www.sqlalchemy.org/
.. _SQLite: https://sqlite.org/index.html
.. _SQLite drivers in the SQLAlchemy documentation: https://docs.sqlalchemy.org/en/14/dialects/sqlite.html#dialect-sqlite
