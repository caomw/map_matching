try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup


with open('README.md', 'r') as f:
    readme = f.read()


setup(
    name='map_matching',
    version='0.1',
    description='A Map Matching Library in Python',
    long_description=readme,
    author='Mapillary',
    url='https://github.com/mapillary/map_matching',
    packages=['map_matching'],
    install_requires=['geopy'],
    include_package_data=True,
    license='BSD',
    zip_safe=False,
    classifiers=(
        'Development Status :: 3 - Alpha',
        'License :: OSI Approved :: BSD License',
        'Intended Audience :: Developers',
        'Operating System :: OS Independent',
        'Intended Audience :: Developers',
        'Topic :: Software Development :: Libraries :: Python Modules',
        'Topic :: Scientific/Engineering :: GIS',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2.7'
    )
)
