import subprocess
import setuptools

from pathlib import Path

this_directory = Path(__file__).parent
long_description = (this_directory / "README.md").read_text()


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
    description='Utility classes for convenient coding of Spark jobs',
    long_description=long_description,
    long_description_content_type='text/markdown',
    author='ECCO Sneaks & Data',
    author_email='esdsupport@ecco.com',
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    install_requires=[
        'hadoop-fs-wrapper==0.4.*',
        'pyspark~=3.2.0',
        'pandas~=1.3.0',
        'delta-spark==1.1.*',
        'opencensus-ext-azure==1.1.*',
    ],
    package_dir={"": "src"},
    packages=setuptools.find_packages(where="src"),
    python_requires=">=3.8",
)
