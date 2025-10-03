from setuptools import setup, find_packages

setup(
    name="donor-analytics-enterprise",
    version="1.0.0",
    packages=find_packages(),
    install_requires=[
        # Core dependencies
        "pandas>=1.5.0",
        "numpy>=1.21.0",
        "scikit-learn>=1.0.2",
        "xgboost>=1.7.0",
        "plotly>=5.13.0",
        "streamlit>=1.24.0",
        "click>=8.0.0",
        
        # Visualization
        "tableau-api-lib>=0.1.27",
        
        # ML & Analytics
        "shap>=0.41.0",
        "category_encoders>=2.5.0",
        "feature_engine>=1.5.0",
    ],
    extras_require={
        "aws": [
            "boto3>=1.26.0",
            "dbt-snowflake>=1.5.0",
            "apache-airflow>=2.6.0",
            "pyspark>=3.3.0",
        ],
        "azure": [
            "azure-storage-blob>=12.14.0",
            "azure-synapse-spark>=0.7.0",
            "azure-synapse-analytics>=0.7.0",
            "dbt-synapse>=1.5.0",
        ],
        "gcp": [
            "google-cloud-storage>=2.7.0",
            "dbt-bigquery>=1.5.0",
        ],
        "dev": [
            "pytest>=7.3.0",
            "pytest-cov>=4.0.0",
            "black>=23.3.0",
            "flake8>=6.0.0",
            "isort>=5.12.0",
            "mypy>=1.3.0",
            "pre-commit>=3.3.0",
        ],
    },
    entry_points={
        "console_scripts": [
            "donor-analytics=donor_analytics_enterprise.cli:main",
        ],
    },
    author="UJA Federation",
    author_email="tech@ujafedny.org",
    description="Enterprise-grade donor analytics and campaign management platform",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    url="https://github.com/ujafedny/donor-analytics-enterprise",
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Intended Audience :: Financial and Insurance Industry",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Topic :: Office/Business :: Financial",
    ],
    python_requires=">=3.9",
)