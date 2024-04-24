import os
from setuptools import setup, find_packages

here = os.path.abspath(os.path.dirname(__file__))

# Read the long description from the relevant file
with open(os.path.join(here, "LONG_DESCRIPTION.rst"), "r", encoding="utf-8") as fp:
    long_description = fp.read()

version_contents = {}
with open(
    os.path.join(here, "parcllabs", "__version__.py"), "r", encoding="utf-8"
) as fp:
    exec(fp.read(), version_contents)

setup(
    name="parcllabs",
    version=version_contents["VERSION"],
    description="Python SDK for ParclLabs API",
    long_description=long_description,
    long_description_content_type="text/x-rst",
    author="ParclLabs",
    author_email="team@parcllabs.com",
    keywords="parcllabs api real estate analytics",
    packages=find_packages(exclude=["tests", "tests.*"]),
    url="https://github.com/ParclLabs/parcllabs-python",
    install_requires=["requests", "pandas"],
    extras_require={"test": ["pytest", "responses"]},
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
    ],
    setup_requires=["wheel"],
)
