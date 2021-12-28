# Configuration file for the Sphinx documentation builder.
#
# This file only contains a selection of the most common options. For a full
# list see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Path setup --------------------------------------------------------------

# If extensions (or modules to document with autodoc) are in another directory,
# add these directories to sys.path here. If the directory is relative to the
# documentation root, use os.path.abspath to make it absolute, like shown here.
#
# import os
# import sys
# sys.path.insert(0, os.path.abspath('.'))


# -- Project information -----------------------------------------------------

project = 'Pandemy'
copyright = '2021, Anton Lydell'
author = 'Anton Lydell'

# The full version, including alpha/beta/rc tags
release = '1.0.0'

# The major project version, used as the replacement for |version|. 
# For example, for the Python documentation, this may be something like 2.6.
version = '1.0.0'


# -- General configuration ---------------------------------------------------

primary_domain = 'py'  # Python is the default domain

# Add any Sphinx extension module names here, as strings. They can be
# extensions coming with Sphinx (named 'sphinx.ext.*') or your custom
# ones.
extensions = [
    'sphinx.ext.autodoc',
    'sphinx.ext.autosectionlabel',
    'sphinx.ext.doctest',
    'sphinx.ext.intersphinx',
    'sphinx.ext.napoleon'
]

# -- Options for sphinx.ext.intersphinx --------------------------------------

intersphinx_mapping = {'sqlalchemy': ('https://docs.sqlalchemy.org', None),
                       'pandas': ('https://pandas.pydata.org/docs/', None),
                       'python': ('https://docs.python.org/3', None)
                       }

# -- Options for sphinx.ext.autosectionlabel ---------------------------------

# Make sure the target is unique
autosectionlabel_prefix_document = True

# -- Options for sphinx.ext.autodoc ------------------------------------------

autodoc_default_options = {
    'member-order': 'bysource',
    'special-members': '__init__',
    'undoc-members': True,
    'exclude-members': '__weakref__'
}

autoclass_content = 'init'

# -- Options for sphinx.ext.doctest ------------------------------------------

doctest_global_setup = """
try:
    import pandemy
except ImportError:
    pandemy = None
"""

doctest_global_cleanup = """
import os
try:
    os.remove('Runescape.db')
except Exception:
    pass
"""

# Add any paths that contain templates here, relative to this directory.
templates_path = ['_templates']

# List of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files.
# This pattern also affects html_static_path and html_extra_path.
exclude_patterns = []


# -- Options for HTML output -------------------------------------------------

# The theme to use for HTML and HTML Help pages.  See the documentation for
# a list of builtin themes.
#
html_theme = 'sphinx_rtd_theme'

# Add any paths that contain custom static files (such as style sheets) here,
# relative to this directory. They are copied after the builtin static files,
# so a file named "default.css" will overwrite the builtin "default.css".
html_static_path = ['_static']

# If this is not None, a ‘Last updated on:’ timestamp is inserted at every page bottom, using the given strftime() format. 
# The empty string is equivalent to '%b %d, %Y' (or a locale-dependent equivalent).
html_last_updated_fmt = r'%Y-%m-%d'
