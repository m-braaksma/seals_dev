import os
from distutils.core import setup
from distutils.extension import Extension

import numpy
from Cython.Build import cythonize
from Cython.Distutils import build_ext
from setuptools import find_packages
from setuptools import setup

packages=find_packages()
include_package_data=True

extensions = [
        Extension(
        "sealsmodel.seals_cython_functions",  # This corresponds to the Python import path
        ["sealsmodel/seals_cython_functions.pyx"],  # Path to the .pyx file
    )
]

setup(
    name = 'sealsmodel',
    packages = packages,
    version = '1.5.4',
    # download_url = 'https://github.com/jandrewjohnson/hazelbean/releases/hazelbean_x64_py3.6.3/dist/hazelbean-0.3.0_x64_py3.6.3.tar.gz',
    ext_modules=cythonize(extensions),
    # ext_modules=[Extension("seals_cython_functions", ["seals/seals_cython_functions.c"]),],
    include_dirs=[numpy.get_include()],
    cmdclass={'build_ext': build_ext},
)
