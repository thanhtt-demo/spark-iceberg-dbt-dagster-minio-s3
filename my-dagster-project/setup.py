from setuptools import find_packages, setup

setup(
    name="my_dagster_project",
    packages=find_packages(exclude=["my_dagster_project_tests"]),
    install_requires=[
        # dagster + dbt
        "dagster==1.8.10",
        "dagster-dbt==0.24.10",
        "dbt-spark[PyHive]==1.8.0",
        "dagster-aws==0.24.10",
        "pyarrow==17.0.0",
        "minio==7.2.9",
        "pandas==2.2.3",
        "pyiceberg==0.7.1"
    ],
    extras_require={"dev": ["dagit", "pytest"]},
)
