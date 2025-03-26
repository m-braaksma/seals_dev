import numpy
from setuptools import setup

# As of 2025-03-26 there doesn't appear to be a way to do the numpy build dir
# inclusion in pyproject.toml.
setup(
    include_dirs=[numpy.get_include()],
)
