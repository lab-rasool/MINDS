from setuptools import find_packages, setup

with open("README.md") as f:
    long_description = f.read()

with open("requirements.txt") as f:
    requirements = f.read().splitlines()

setup(
    name="minds",
    version="0.0.5",
    description="A package for downloading and processing data from the MINDS database",
    package_dir={"": "app"},
    packages=find_packages(where="app"),
    install_requires=requirements,
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/lab-rasool/MINDS",
    author="Aakash Tripathi",
    author_email="aakash.tripathi@moffitt.org",
    license="MIT",
)
