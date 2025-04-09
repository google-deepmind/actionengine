from setuptools import find_packages
from setuptools import setup

setup(
    name="eglt",
    ext_modules=[],
    version="0.1.0",
    # description=(
    #     "An implementation of the inference pipeline of AlphaFold v2.0. This is"
    #     " a completely new model that was entered as AlphaFold2 in CASP14 and"
    #     " published in Nature."
    # ),
    author="Google LLC",
    author_email="helenapankov@google.com",
    license="Apache License, Version 2.0",
    url="https://github.com/google-deepmind/actionengine-cpp",
    packages=find_packages(),
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Science/Research",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: POSIX :: Linux",
        "Programming Language :: Python :: 3.12",
        "Topic :: Scientific/Engineering :: Artificial Intelligence",
    ],
)
