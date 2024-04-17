from setuptools import setup, find_packages

setup(
    name='parcllabs',
    version='0.1.0',
    packages=find_packages(exclude=["tests", "tests.*"]),
    # package_dir={"": "src"},
    install_requires=[
        'requests',
        'pandas'
    ],
    extras_require={
        "test": ["pytest", "responses"]
    },
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.7',
    ],
)
