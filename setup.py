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
        'bidict==0.23.1',
        'cloudpickle==3.0.0',
        'graphviz==0.20.3',
        'h5io==0.2.2',
        'h5io_browser==0.0.12',
        'matplotlib==3.8.4',
        'pandas==2.2.0',
        'pyiron_base==0.8.3',
        'pyiron_contrib==0.1.16',
        'pympipool==0.8.0',
        'toposort==1.10',
        'typeguard==4.2.1',
    ],
    extras_require={
        "node_library": [
            'ase==3.22.1',
            'atomistics==0.1.27',
            'numpy==1.26.4',
            'phonopy==2.22.1',
            'pyiron_atomistics==0.5.4',
        ],
    },
    cmdclass=versioneer.get_cmdclass(),

    )
