from setuptools import setup, find_packages

setup(
    name="pyspark_udemy",
    version="1.0",
    packages=find_packages(),
    python_requires="==3.7.*",
    install_requires=["pyspark==2.4.*", "numpy"]
)
