#!/usr/bin/env python3
#
#   Copyright 2022  SenX S.A.S.
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.
#

"""
Package setup.py
"""
from pathlib import Path
import setuptools


# reading long description from file
with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

# Load packages from requirements.txt
with open(Path('./', "requirements.txt"), "r",encoding='utf-8') as file:
    REQUIREMENTS = [ln.strip() for ln in file.readlines()]

# # specify requirements of your package here
# REQUIREMENTS = ["fiona", "geopandas", "pyproj","docopt"]
# REQUIREMENTS = []

# calling the setup function
setuptools.setup(
    name="pywarp",
    version="0.6.0",
    description="A set of functions to ease working with Warp 10",
    long_description=long_description,
    long_description_content_type="text/markdown",
    # url='',
    author="SenX",
    author_email="contact@senx.io",
    license="Apache 2.0",
    package_dir={"": "src"},
    packages=setuptools.find_packages(
        where="src", include=["pywarp"]
    ),
    classifiers=[
        "Development Status :: 4 - Alpha",
        "Intended Audience :: Developers",
        "Topic :: Scientific/Engineering :: Time Series ",
        "License :: Apache 2.0",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3.8",
    ],
    install_requires=REQUIREMENTS,
    keywords="time series Warp 10",
    python_requires=">=3.8",
)
