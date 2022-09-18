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


.. toctree::
   :maxdepth: 3
   :caption: Table of contents

   initialization
   merge_df


.. _cx_Oracle: https://oracle.github.io/python-cx_Oracle/
