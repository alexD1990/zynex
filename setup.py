from setuptools import setup, find_packages

setup(
    name="DCheck",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        "pyspark>=3.1",  
    ],
    tests_require=[
        "pytest>=6.0",  
    ],
    entry_points={
        "console_scripts": [
            "dcheck = DCheck.api:validate_spark",  
        ],
    },
)
