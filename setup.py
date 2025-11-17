"""
Setup configuration for Custom Dataflow Pipeline Template
"""

from setuptools import setup, find_packages

# Read requirements from requirements.txt
with open('requirements.txt', 'r') as f:
    requirements = [line.strip() for line in f if line.strip() and not line.startswith('#')]

setup(
    name='custom-dataflow-template',
    version='1.0.0',
    description='Custom Dataflow Pipeline Template for Cross-Platform Data Synchronization',
    author='Data Engineering Team',
    author_email='data-engineering@company.com',
    packages=find_packages(),
    install_requires=requirements,
    python_requires='>=3.9',
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
        'Programming Language :: Python :: 3.11',
        'Programming Language :: Python :: 3.12',
    ],
    keywords='dataflow apache-beam bigquery postgresql data-pipeline',
    project_urls={
        'Documentation': 'https://github.com/your-org/custom-dataflow-template',
        'Source': 'https://github.com/your-org/custom-dataflow-template',
        'Tracker': 'https://github.com/your-org/custom-dataflow-template/issues',
    },
) 