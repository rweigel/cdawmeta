from setuptools import setup, find_packages

install_requires = [
    "cdasws",
    "cdflib",
    "GitPython",
    "hapiclient",
    "pymongo",
    "timedelta_isoformat",
    "fastapi",
    "uvicorn"
]

try:
  # Will work if utilrsw was already installed, for example via pip install -e .
  import utilrsw
except:
  install_requires.append("utilrsw @ git+https://github.com/rweigel/utilrsw")

setup(
  name='cdawmeta',
  version='0.0.2',
  author='Bob Weigel',
  license='LICENSE.txt',
  packages=find_packages(),
  author_email='rweigel@gmu.edu',
  install_requires=install_requires,
  long_description=open('README.md').read(),
  long_description_content_type='text/markdown',
  description='Transform CDAWeb metadata to HAPI, SPASE, and SOSO; build tables for inspection in browser.'
)
