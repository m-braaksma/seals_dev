import numpy
from setuptools import setup

# # As of 2025-03-26 there doesn't appear to be a way to do the numpy build dir
# # # inclusion in pyproject.toml.
from distutils.core import setup
from distutils.extension import Extension

# from setuptools import setup, Extension
from Cython.Distutils import build_ext
import numpy

# Print cwd
import os
print("Current working directory:", os.getcwd())

ext_modules = [Extension('seals.seals_cython_functions',
                         ['seals/seals_cython_functions.pyx'],
                         )]

returned = setup(
    name='seals_cython_functions',
    include_dirs=[numpy.get_include()],
    cmdclass={'build_ext': build_ext},
    ext_modules=ext_modules
)
