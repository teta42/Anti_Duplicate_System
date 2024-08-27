from setuptools import setup, find_packages

setup(
    name='Anti-Duplicate_System_for_Kafka',
    version='0.1',
    packages=find_packages(),
    install_requires=[
        'kafka-python==2.0.2'
    ],
)
