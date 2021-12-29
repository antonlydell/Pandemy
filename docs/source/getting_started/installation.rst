Installation
============

Pandemy is available for installation through `PyPI`_ using `pip`_ and the source code is hosted on GitHub at: https://github.com/antonlydell/pandemy

.. _PyPI: https://pypi.org/project/pandemy/
.. _pip: https://pip.pypa.io/en/stable/getting-started/

Install Pandemy by running:

.. code-block:: bash

    $ pip install pandemy


Dependencies
------------

The core dependencies of Pandemy are:

- pandas_ : powerful Python data analysis toolkit
- SQLAlchemy_ : The Python SQL Toolkit and Object Relational Mapper

To work with other databases than `SQLite`_ (which is the only supported database in version 1.0.0) additional optional dependencies may need to be installed.
Support for `Oracle`_ and `Microsoft SQL Server`_ databases is planned for the future.

.. _pandas: https://pandas.pydata.org/
.. _SQLAlchemy: https://www.sqlalchemy.org/
.. _SQLite: https://sqlite.org/index.html
.. _Oracle: https://www.oracle.com/database/
.. _Microsoft SQL Server: https://www.microsoft.com/en-us/sql-server/sql-server-downloads
