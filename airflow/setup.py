from setuptools import find_packages, setup

Name = "airflow-dags"
Author = "Napo Limited"
AuthorEmail = ""
Maintainer = "Napo Limited"
Summary = "The data reporting and other pipelines."
License = "All rights reserved"
Version = "0.1.0"
Description = Summary
ShortDescription = Summary

setup(
    name=Name,
    zip_safe=False,
    version=Version,
    author=Author,
    author_email=AuthorEmail,
    description=Summary,
    long_description=Description,
    classifiers=[
        "Programming Language :: Python",
    ],
    keywords="",
    license=License,
    include_package_data=True,
    packages=find_packages(),
)
