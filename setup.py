from setuptools import setup, find_packages

with open("requirements.txt") as f:
    requirements = f.read().splitlines()

setup(
    name="MINDS",
    version="0.1",
    packages=find_packages(),
    install_requires=requirements,
    author="Aakash Tripathi",
    author_email="aakash.tripathi@moffitt.org",
    description="A package for downloading and processing data from the MINDS database",
    license="MIT",
)
