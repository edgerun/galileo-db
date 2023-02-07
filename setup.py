import os

import setuptools

try:
    with open("README.md", "r") as fh:
        long_description = fh.read()
except FileNotFoundError:
    long_description = ''

try:
    with open("requirements-dev.txt", "r") as fh:
        tests_require = [line for line in fh.read().split(os.linesep) if line]
except FileNotFoundError:
    tests_require = []

try:
    with open("requirements.txt", "r") as fh:
        install_requires = [line for line in fh.read().split(os.linesep) if line and not line.startswith('git')]
except FileNotFoundError:
    install_requires = []

setuptools.setup(
    name="galileo-db",
    version="0.10.5.dev3",
    author="Thomas Rausch",
    author_email="t.rausch@dsg.tuwien.ac.at",
    description="Galileo DB: Gateway and client tools for the Galileo Experiment DB",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/edgerun/galileo-db",
    packages=setuptools.find_packages(),
    package_data={'galileodb.sql': ['schema.sql']},
    test_suite="tests",
    tests_require=tests_require,
    install_requires=install_requires,
    classifiers=[
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
    ],
    entry_points={
        "console_scripts": [
            'galileodb-recorder = galileodb.cli.recorder:main',
            'galileodb-ctl = galileodb.cli.manager:main'
        ]
    },
)
