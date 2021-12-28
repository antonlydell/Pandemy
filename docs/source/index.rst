.. Pandemy documentation master file, created by
   sphinx-quickstart on Sat Oct 16 22:09:42 2021.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

#####################
Pandemy documentation
#####################

Pandemy is a wrapper around `pandas`_ and `SQLAlchemy`_ to provide an easy class based interface for working with DataFrames and databases.
This package is for those who enjoy working with pandas and SQL but do not want to learn all "bells and whistles" of SQLAlchemy.
Use your existing SQL knowledge and provide text based SQL statements to load DataFrames from and write DataFrames to databases.

.. _pandas: https://pandas.pydata.org/
.. _SQLAlchemy: https://www.sqlalchemy.org/


- **Release**: |release|
- **Release Date**: YYYY-MM-DD

***************
Getting started
***************

The Getting started chapter describes how to install Pandemy and showcases a brief 
roundtrip of saving a DataFrame to and reading a DataFrame from an SQLite database.
A teaser for what the User guide has to offer.

.. toctree::
   :maxdepth: 2

   getting_started/installation


**********
User guide
**********

The User guide chapter explains the main concepts and use cases of Pandemy.
It starts with a description about the core components of Pandemy: the 
:class:`DatabaseManager <pandemy.DatabaseManager>` and :class:`SQLContainer <pandemy.SQLContainer>` classes.
Thereafter the implemented SQL dialects are described.

.. toctree::
   :maxdepth: 2
   :caption: User guide

   user_guide/databasemanager
   user_guide/sqlite


*************
API reference
*************

This chapter explains the complete the API of Pandemy.

.. toctree::
   :maxdepth: 3

   api_reference/api_reference


******************
Indices and tables
******************

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
