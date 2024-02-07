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
        'h5io==0.2.2',
        'matplotlib==3.8.2',
        'pympipool==0.7.9',
        'toposort==1.10',
        'typeguard==4.1.5',
    ],
    extras_require={
        "node_library": [
            'ase==3.22.1',
            'atomistics==0.1.21',
            'numpy==1.26.3',
            'phonopy==2.21.0',
            'pyiron_atomistics==0.4.9',
        ],
        "tinybase": [
            'boto3',  # Just because pyiron_contrib is not making sure it's there
            'h5io_browser==0.0.6',
            'pyiron_contrib==0.1.14',
        ]
    },
    cmdclass=versioneer.get_cmdclass(),

    )
