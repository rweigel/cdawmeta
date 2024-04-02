from setuptools import setup, find_packages

install_requires = ["requests_cache", "xmltodict"]

setup(
    name='cdawmeta',
    version='0.0.1',
    author='Bob Weigel',
    author_email='rweigel@gmu.edu',
    packages=find_packages(),
    license='LICENSE.txt',
    description='Transform CDAWeb metadata to HAPI and SPASE; build tables for inspection in browser.',
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown',
    install_requires=install_requires
)
