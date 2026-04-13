import sys

from setuptools import setup, find_packages

install_requires = [
    "cdasws",
    "cdflib",
    "GitPython",
    "hapiclient",
    "pymongo",
    "timedelta_isoformat",
    "fastapi",
    "uvicorn",
    "dicttoxml"
]

if sys.version_info < (3, 9):
  sys.exit('\n\n  Python < 3.9 is not supported\n\n')

install_requires.append("utilrsw[net] @ git+https://github.com/rweigel/utilrsw@main")
install_requires.append("tableui @ git+https://github.com/rweigel/table-ui@main")

setup(
  name='cdawmeta',
  version='0.0.3',
  author='Bob Weigel',
  license='LICENSE.txt',
  packages=find_packages(),
  author_email='rweigel@gmu.edu',
  install_requires=install_requires,
  long_description=open('README.md').read(),
  long_description_content_type='text/markdown',
  description='Transform CDAWeb metadata to HAPI and SPASE; build tables for inspection in browser.'
)
