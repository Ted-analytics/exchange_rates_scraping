from setuptools import find_packages, setup

setup(
    name="exchange_rate_pipeline",
    packages=find_packages(exclude=["exchange_rate_pipeline_tests"]),
    install_requires=[
        "dagster",
        "dagster-k8s",
        "dagster-postgres",
        "clickhouse-driver",
        "pandas",
        "python-dateutil",
        "selenium",
        "requests"
    ],
    extras_require={"dev": ["dagit", "pytest"]},
)