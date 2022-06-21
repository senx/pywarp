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
    version="0.0.0",
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
