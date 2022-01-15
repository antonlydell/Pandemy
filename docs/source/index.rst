.. Pandemy documentation master file, created by
   sphinx-quickstart on Sat Oct 16 22:09:42 2021.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

#####################
Pandemy documentation
#####################

Pandemy is a wrapper around `pandas`_ and `SQLAlchemy`_ to provide an easy class based interface for working with DataFrames and databases.
This package is for those who enjoy working with `pandas`_ and SQL but do not want to learn all "bells and whistles" of `SQLAlchemy`_.
Use your existing SQL knowledge and provide text based SQL statements to load DataFrames from and write DataFrames to databases.

.. _pandas: https://pandas.pydata.org/
.. _SQLAlchemy: https://www.sqlalchemy.org/


- **Release**: |release| | YYYY-MM-DD


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

   GitHub ↪ <https://github.com/antonlydell/pandemy>
   LinkedIn ↪ <https://www.linkedin.com/in/antonlydell/>
   PyPI ↪ <https://pypi.org/project/pandemy/>


******************
Indices and tables
******************

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
