# Configuration file for the Sphinx documentation builder.
#
# This file only contains a selection of the most common options. For a full
# list see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Imports -----------------------------------------------------------------

from datetime import date


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
copyright = f'2021-{date.today().year}, Anton Lydell'
author = 'Anton Lydell'

# The full version, including alpha/beta/rc tags
release = '1.0.0'

# The major project version, used as the replacement for |version|.
# For example, for the Python documentation, this may be something like 2.6.
version = release


# -- General configuration ---------------------------------------------------

primary_domain = 'py'  # Python is the default domain

# Add any Sphinx extension module names here, as strings. They can be
# extensions coming with Sphinx (named 'sphinx.ext.*') or your custom
# ones.
extensions = [
    'sphinx.ext.autodoc',
    'sphinx.ext.autosectionlabel',
    'sphinx.ext.coverage',  # .\make coverage
    'sphinx.ext.doctest',  # .\make doctest
    'sphinx.ext.intersphinx',
    'sphinx.ext.napoleon'
]

# Minimum version, enforced by sphinx
needs_sphinx = '4.2.0'


# -- Options for sphinx.ext.intersphinx --------------------------------------

intersphinx_mapping = {
    'pandas': ('https://pandas.pydata.org/docs/', None),
    'python': ('https://docs.python.org/3', None),
    'sqlalchemy': ('https://docs.sqlalchemy.org/en/14/', None)
}


# -- Options for sphinx.ext.autosectionlabel ---------------------------------

# Make sure the target is unique
autosectionlabel_prefix_document = True


# -- Options for sphinx.ext.autodoc ------------------------------------------

autodoc_default_options = {
    'show-inheritance': True
}

autoclass_content = 'both'
# This value selects what content will be inserted into the main body of an autoclass directive.

# The possible values are:

# "class"
#     Only the class’ docstring is inserted. This is the default. You can still document __init__ as
#     a separate method using automethod or the members option to autoclass.

# "both"
#     Both the class’ and the __init__ method’s docstring are concatenated and inserted.

# "init"
#     Only the __init__ method’s docstring is inserted.

# If the class has no __init__ method or if the __init__ method’s docstring is empty, but the class
# has a __new__ method’s docstring, it is used instead.

autodoc_member_order = 'alphabetical'
# This value selects if automatically documented members are sorted alphabetical (value 'alphabetical'),
# by member type (value 'groupwise') or by source order (value 'bysource'). The default is alphabetical.
# Note that for source order, the module must be a Python module with the source code available.


autodoc_typehints = 'none'
# This value controls how to represent typehints. The setting takes the following values:

#     'signature' – Show typehints in the signature (default)

#     'description' – Show typehints as content of the function or method The typehints of overloaded functions or
#                     methods will still be represented in the signature.

#     'none' – Do not show typehints

#     'both' – Show typehints in the signature and as content of the function or method

# Overloaded functions or methods will not have typehints included in the description because it is impossible to
# accurately represent all possible overloads as a list of parameters.


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


# -- Options for sphinx.ext.napoleon ------------------------------------------

napoleon_custom_sections = [('Attributes', 'params_style')]
# Attributes should be rendered by default, but is not for some reason.

# Add a list of custom sections to include, expanding the list of parsed sections.
# Defaults to None.

# The entries can either be strings or tuples, depending on the intention:
#     To create a custom “generic” section, just pass a string.

#     To create an alias for an existing section, pass a tuple containing the
#     alias name and the original, in that order.

#     To create a custom section that displays like the parameters or returns section
#     pass a tuple containing the custom section name and a string value,
#     “params_style” or “returns_style”.


# -- Options for templates and patterns ----------------------------------------

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

# If this is not None, a ‘Last updated on:’ timestamp is inserted at every page bottom, using the given strftime()
# format. The empty string is equivalent to '%b %d, %Y' (or a locale-dependent equivalent).
html_last_updated_fmt = r'%Y-%m-%d'
