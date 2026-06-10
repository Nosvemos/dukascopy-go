import os
from setuptools import setup, find_packages

# Load the long description from the root README (if available)
try:
    with open(os.path.join(os.path.dirname(__file__), '..', '..', 'README.md'), encoding='utf-8') as f:
        long_description = f.read()
except FileNotFoundError:
    long_description = "Dukascopy market data downloader SDK"

setup(
    name="dukascopy-go",
    version="1.0.0",
    description="Blazing fast Dukascopy market data downloader SDK built on Go",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/Nosvemos/dukascopy-go",
    author="Nosvemos",
    packages=find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Topic :: Office/Business :: Financial :: Investment",
    ],
    python_requires=">=3.9",
    package_data={
        "": ["libdukascopy.so", "libdukascopy.dll", "libdukascopy.dylib"],
    },
    include_package_data=True,
    extras_require={
        "pandas": ["pandas>=1.0.0", "pyarrow>=1.0.0"],
    },
)
