from setuptools import find_packages, setup
import os

# Get the absolute path to the directory containing this file
here = os.path.abspath(os.path.dirname(__file__))

with open(os.path.join(here, "README.md"), encoding="utf-8") as f:
    long_description = f.read()

# Hardcode requirements instead of reading from file
requirements = [
    "cachetools",
    "certifi",
    "charset-normalizer",
    "google-api-core",
    "google-api-python-client",
    "google-auth-httplib2",
    "google-cloud-bigquery",
    "google-cloud-core",
    "google-cloud-storage",
    "google-crc32c",
    "google-resumable-media",
    "googleapis-common-protos",
    "grpcio",
    "httplib2",
    "hurry.filesize",
    "idna",
    "packaging",
    "proto-plus",
    "protobuf",
    "pyasn1",
    "pyasn1-modules",
    "pyparsing",
    "requests",
    "rsa",
    "six",
    "tqdm",
    "uritemplate",
    "urllib3",
    "google-auth",
    "retry",
    "numpy",
    "pandas",
    "python-dotenv",
    "sqlalchemy",
    "psycopg2-binary",  # PostgreSQL driver instead of pymysql
    "cryptography",
    "rich",
]

setup(
    name="med-minds",  # Changed from minds to med-minds
    version="0.0.7",  # PostgreSQL migration version
    description="A package for downloading and processing multimodal oncology data from the MINDS database",
    package_dir={"": "app"},
    packages=find_packages(where="app"),
    install_requires=requirements,
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/lab-rasool/MINDS",
    author="Aakash Tripathi",
    author_email="aakash.tripathi@moffitt.org",
    license="MIT",
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Science/Research",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
    ],
    python_requires=">=3.7",
)
