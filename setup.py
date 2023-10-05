"""
Setuptools based setup module
"""
from setuptools import setup, find_packages
import versioneer

setup(
    name='pyiron_module_template',
    version=versioneer.get_version(),
    description='pyiron_module_template - module extension to pyiron.',
    long_description='http://pyiron.org',

    url='https://github.com/pyiron/pyiron_module_template',
    author='Max-Planck-Institut für Eisenforschung GmbH - Computational Materials Design (CM) Department',
    author_email='@mpie.de',
    license='BSD',

    classifiers=[
        'Development Status :: 3 - Alpha',
        'Topic :: Scientific/Engineering :: Physics',
        'License :: OSI Approved :: BSD License',
        'Intended Audience :: Science/Research',
        'Operating System :: OS Independent',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
        'Programming Language :: Python :: 3.11',
    ],

    keywords='pyiron',
    packages=find_packages(exclude=["*tests*", "*docs*", "*binder*", "*conda*", "*notebooks*", "*.ci_support*"]),
    install_requires=[
        'pyiron_base'
    ],
    cmdclass=versioneer.get_cmdclass(),

    )
