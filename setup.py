from setuptools import setup, find_packages

setup(
    name="zynex",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[],
    extras_require={
        "local": ["pyspark>=3.1"],
        "dev": ["pytest>=6.0"],
    },
)
