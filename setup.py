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
        'graphviz==8.1.0',
        'matplotlib==3.7.2',
        'numpy==1.24.3',
        'pyiron_base==0.6.7',
        'pyiron_atomistics==0.3.4',
        'toposort==1.1.0',
        'typeguard==4.1.0',
    ],
    cmdclass=versioneer.get_cmdclass(),

    )
