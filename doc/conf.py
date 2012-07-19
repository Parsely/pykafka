# -*- coding: utf-8 -*-
import sys, os

project = u'samsa'
copyright = u'2012, DISQUS'
version = release = '0.0'

extensions = ['sphinx.ext.autodoc']

templates_path = ['_templates']
exclude_patterns = ['_build']
html_static_path = ['_static']

source_suffix = '.rst'
master_doc = 'index'

html_theme = 'nature'
pygments_style = 'sphinx'
htmlhelp_basename = 'samsadoc'

autodoc_default_flags = ['special-members', 'show-inheritance']
