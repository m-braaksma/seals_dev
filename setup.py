# setup.py — only needed to register custom commands like build_ext
from setuptools import setup
import numpy
from Cython.Distutils import build_ext
from setuptools.extension import Extension

ext_modules = [Extension('seals.seals_cython_functions',
                         ['seals/seals_cython_functions.pyx'],
                         include_dirs=[numpy.get_include()])]

setup(
    cmdclass={'build_ext': build_ext},
    ext_modules=ext_modules,
)