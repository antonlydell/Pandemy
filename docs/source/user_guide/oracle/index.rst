Oracle
======

This section describes the Oracle :class:`DatabaseManager <pandemy.DatabaseManager>` :class:`OracleDb <pandemy.OracleDb>`.
:class:`OracleDb <pandemy.OracleDb>` is used in the same way as :class:`SQLiteDb <pandemy.SQLiteDb>`
except for the initialization. For information about how to work with :class:`OracleDb <pandemy.OracleDb>`
please refer to :doc:`The SQLite User guide <../sqlite/index>`. There are several different ways of
connecting to an Oracle database and :class:`OracleDb <pandemy.OracleDb>` further simplifies the
SQLAlchemy connection process for the most common methods. To use :class:`OracleDb <pandemy.OracleDb>`
the Oracle database driver package cx_Oracle_ needs to be installed. Please refer to the
:doc:`Installation <../../getting_started/installation>` section for how to install Pandemy and cx_Oracle.


Initialization
--------------

The initialization of :class:`OracleDb <pandemy.OracleDb>` is described further in the
:ref:`API reference of OracleDb <api_reference/databasemanager:OracleDb>`. Additional information about connecting
to an Oracle database with SQLAlchemy can be found in the `cx_Oracle part of the SQLAlchemy documentation`_.


.. _cx_Oracle: https://oracle.github.io/python-cx_Oracle/
.. _cx_Oracle part of the SQLAlchemy documentation: https://docs.sqlalchemy.org/en/14/dialects/oracle.html#module-sqlalchemy.dialects.oracle.cx_oracle
.. _optional dependency identifier: https://setuptools.pypa.io/en/latest/userguide/dependency_management.html#optional-dependencies
