__license__ = """
Copyright 2015 Parse.ly, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""

# -*- coding: utf-8 -*-
import re


def get_version():
    with open("../pykafka/__init__.py") as version_file:
        return re.search(
            r"""__version__\s+=\s+(['"])(?P<version>.+?)\1""", version_file.read()
        ).group("version")


project = u"pykafka"
copyright = u"2019, Parse.ly"
version = release = get_version()

extensions = ["sphinx.ext.autodoc"]

templates_path = ["_templates"]
exclude_patterns = ["_build"]
html_static_path = ["_static"]

source_suffix = ".rst"
master_doc = "index"

html_theme = "sphinx_rtd_theme"
pygments_style = "sphinx"
htmlhelp_basename = "pykafkadoc"

autodoc_default_flags = ["special-members", "private-members", "show-inheritance"]
