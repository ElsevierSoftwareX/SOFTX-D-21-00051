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
import os
import sys
sys.path.insert(0, os.path.abspath('../..'))
sys.setrecursionlimit(1500)
version = open("../../VERSION", "rt").read()
# import pkg_resources
# version = pkg_resources.get_distribution('sila2_manager').version

# -- Project information -----------------------------------------------------

project = 'SiLA2 Manager'
copyright = '2021, Lukas Bromig, David Leiter, Alexandru-Virgil Mardale'
author = 'Lukas Bromig, David Leiter, Alexandru-Virgil Mardale'

# The full version, including alpha/beta/rc tags
release = f'01.02.2021, {version}'

# -- General configuration ---------------------------------------------------

# Add any Sphinx extension module names here, as strings. They can be
# extensions coming with Sphinx (named 'sphinx.ext.*') or your custom
# ones.
extensions = [
    'sphinx_material',
    'sphinx.ext.autodoc'
]
#    'sphinx.ext.napoleon'

autodoc_default_flags = ['members', 'inherited-members', 'show-inheritance']
autodoc_default_options = {
    "members": True,
    "inherited-members": False,
    "show-inheritance": True,
}

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
html_theme = 'sphinx_material'

# Material theme options (see theme.conf for more information)
html_theme_options = {

    # Set the name of the project to appear in the navigation.
    'nav_title': 'SiLA2 Manager',

    # Set you GA account ID to enable tracking
    'google_analytics_account': 'G-DV1NFGEFK4',

    # Specify a base_url used to generate sitemap.xml. If not
    # specified, then no sitemap will be built.
    'base_url': 'https://gitlab.com/lukas.bromig/sila2_manager',

    # Set the color and the accent color
    'color_primary': 'blue',
    'color_accent': 'light-blue',

    # Set the repo location to get a badge with stats
    'repo_url': 'https://gitlab.com/lukas.bromig/sila2_manager',
    'repo_name': 'SiLA2_Manager',

    # Visible levels of the global TOC; -1 means unlimited
    'globaltoc_depth': 3,
    # If False, expand all TOC entries
    'globaltoc_collapse': False,
    # If True, show hidden TOC entries
    'globaltoc_includehidden': False,
}

# Add any paths that contain custom static files (such as style sheets) here,
# relative to this directory. They are copied after the builtin static files,
# so a file named "default.css" will overwrite the builtin "default.css".
html_static_path = ['_static', 'images']


# Add the generate_config.py script here so it can be executed in the build env of readthedocs

#!/usr/bin/env python3
import secrets
import base64
import configparser
import os
from source.device_manager.data_directories import DATA_DIRECTORY

DIRECTORY = DATA_DIRECTORY
CONFIG_FILE = f'{DIRECTORY}/device-manager.conf'

config = configparser.ConfigParser()
config['Security'] = {
    'SecretKey': base64.b64encode(secrets.token_bytes(64)).decode()
}
config['Database'] = {
    'host': 'localhost',
    'port': 5432,
    'user': 'postgres',
    'password': '1234'
}

os.makedirs(DIRECTORY, exist_ok=True)
with open(CONFIG_FILE, 'w') as configfile:
    config.write(configfile)
