import pathlib

import setuptools

HOME_DIR = pathlib.Path(__file__).parent

# The text of the README file
README = (HOME_DIR / "README.md").read_text()

setuptools.setup(
    name="asgarde",
    version="0.16.0",
    description="Allows simplifying error handling with Apache Beam",
    long_description=README,
    long_description_content_type="text/markdown",
    url="https://github.com/tosun-si/pasgarde",
    author="Mazlum TOSUN",
    author_email="mazlum.tosun@gmail.com",
    packages=setuptools.find_packages(),
    classifiers=[
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent"
    ],
    python_requires='>=3.6.10',
)
