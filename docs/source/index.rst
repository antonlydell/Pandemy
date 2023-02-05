.. Pandemy documentation master file, created by
   sphinx-quickstart on Sat Oct 16 22:09:42 2021.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

#####################
Pandemy documentation
#####################

.. meta::
   :keywords: Anton Lydell, data, database, database engine, DataFrame, data analytics, data science, GitHub, merge, Oracle, pandas, Pandemy, PyPI, Python, Python package, read_sql, SQLAlchemy, SQLite, sql statement, to_sql, update, upsert

Pandemy is a wrapper around `pandas`_ and `SQLAlchemy`_ to provide an easy class based interface for working with DataFrames and databases.
This package is for those who enjoy working with `pandas`_ and SQL but do not want to learn all "bells and whistles" of `SQLAlchemy`_.
Use your existing SQL knowledge and provide text based SQL statements to load DataFrames from and write DataFrames to databases.

.. _pandas: https://pandas.pydata.org/
.. _SQLAlchemy: https://www.sqlalchemy.org/


- **Release**: |release| \| |today|

- **License**: Pandemy is distributed under the `MIT-license`_.

.. _MIT-license: https://opensource.org/licenses/mit-license.php


***********
Disposition
***********

The documentation consists of 3 parts:

* :doc:`Getting started <getting_started/index>` : Install Pandemy and get a brief overview of the package.

* :doc:`User guide <user_guide/index>` : The structure of Pandemy and a walkthrough of how to use it.

* :doc:`API reference <api_reference/index>` : Details about the API of Pandemy. 


.. Adds a home button (to this page) to the navigation bar
.. toctree::
   :maxdepth: 2
   :hidden:

   Home <self>


.. Main Table of contents
.. toctree::
   :maxdepth: 2
   :caption: Table of contents

   getting_started/index
   user_guide/index
   api_reference/index


.. toctree::
   :hidden:
   :caption: Links

   conda-forge ↪ <https://anaconda.org/conda-forge/pandemy>
   GitHub ↪ <https://github.com/antonlydell/Pandemy>
   LinkedIn ↪ <https://www.linkedin.com/in/antonlydell/>
   PyPI ↪ <https://pypi.org/project/Pandemy/>


******************
Indices and tables
******************

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
