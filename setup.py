from setuptools import setup, find_packages

setup(
    name='Aduplicate',
    version='1.0.0',
    packages=find_packages(),
    install_requires=[
        'kafka-python-ng>2.0.0'
    ],
)