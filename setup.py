from pathlib import Path

from setuptools import find_packages, setup

here = Path(__file__).parent

# Read the long description from the relevant file
with (here / "LONG_DESCRIPTION.rst").open(encoding="utf-8") as fp:
    long_description = fp.read()

version_contents = {}
with (here / "parcllabs" / "__version__.py").open(encoding="utf-8") as fp:
    exec(fp.read(), version_contents)  # noqa: S102

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
    install_requires=[
        "requests",
        "pandas",
        "numpy",
    ],
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
