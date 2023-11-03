"""
Setuptools based setup module
"""
from setuptools import setup, find_packages
import versioneer

setup(
    name='pyiron_workflow',
    version=versioneer.get_version(),
    description='Graph-and-node based workflow tools.',
    long_description='http://pyiron.org',

    url='https://github.com/pyiron/pyiron_workflow',
    author='Max-Planck-Institut für Eisenforschung GmbH - Computational Materials Design (CM) Department',
    author_email='liamhuber@greyhavensolutions.com',
    license='BSD',

    classifiers=[
        'Development Status :: 3 - Alpha',
        'Topic :: Scientific/Engineering :: Physics',
        'License :: OSI Approved :: BSD License',
        'Intended Audience :: Science/Research',
        'Operating System :: OS Independent',
        'Programming Language :: Python :: 3.10',
        'Programming Language :: Python :: 3.11',
    ],

    keywords='pyiron',
    packages=find_packages(exclude=["*tests*", "*docs*", "*binder*", "*conda*", "*notebooks*", "*.ci_support*"]),
    install_requires=[
        'bidict==0.22.1',
        'cloudpickle==3.0.0',
        'graphviz==0.20.1',
        'matplotlib==3.8.0',
        'pyiron_base==0.6.8',
        'toposort==1.10',
        'typeguard==4.1.5',
    ],
    extras_require={
        "node_library": [
            'numpy==1.26.1',
            'pyiron_atomistics==0.3.5',
        ],
    },
    cmdclass=versioneer.get_cmdclass(),

    )
