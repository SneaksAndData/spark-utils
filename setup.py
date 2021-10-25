import subprocess
import setuptools


def get_version():
    base_version = subprocess.check_output(["git", "describe", "--tags", "--abbrev=7"]).strip().decode("utf-8")
    # have to follow PEP440 religious laws here
    parts = base_version.split('-')
    if len(parts) == 1:
        return parts[0]
    else:
        (semantic, commit_number, commit_id) = parts
        return f"{semantic}+{commit_number}.{commit_id}"


setuptools.setup(
    name='spark-utils',
    version=get_version(),
    description='Spark utilities for ESD Spark Runtime',
    author='ESD',
    author_email='esdsupport@ecco.com',
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    install_requires=[
        'hadoop-fs-wrapper==0.2.2',
        'pyspark~=3.1.2',
        'pandas~=1.3.0',
        'delta-spark==1.0.*',
        'opencensus-ext-azure==1.1.*',
    ],
    package_dir={"": "src"},
    packages=setuptools.find_packages(where="src"),
    python_requires=">=3.6",
)
