"""Setup script for Product Recommender System."""

from setuptools import setup, find_packages
from pathlib import Path

# Read the README file
this_directory = Path(__file__).parent
long_description = (this_directory / "README.md").read_text()

setup(
    name="product-recommender",
    version="2.0.0",
    author="TanSin18",
    description="A machine learning-based product recommendation system for e-commerce",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/TanSin18/Product-Recommender",
    packages=find_packages(where="Inactive_Customers"),
    package_dir={"": "Inactive_Customers"},
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "Topic :: Scientific/Engineering :: Artificial Intelligence",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
    ],
    python_requires=">=3.8",
    install_requires=[
        # Core Google Cloud dependencies
        "google-api-core>=2.19.1",
        "google-auth>=2.30.0",
        "google-auth-oauthlib>=1.2.0",
        "google-cloud-bigquery>=3.25.0",
        "google-cloud-bigquery-storage>=2.25.0",
        "google-cloud-core>=2.4.1",
        "google-cloud-storage>=2.17.0",
        "db-dtypes>=1.2.1",
        # Machine Learning
        "xgboost>=2.1.0",
        "scikit-learn>=1.5.1",
        "imbalanced-learn>=0.12.3",
        "scipy>=1.13.1",
        # Data Processing
        "pandas>=2.2.2",
        "numpy>=1.26.4",
        "pyarrow>=16.1.0",
        "pandas-gbq>=0.23.1",
        # Visualization
        "matplotlib>=3.9.0",
        "seaborn>=0.13.2",
        # Utilities
        "python-dateutil>=2.9.0",
        "pytz>=2024.1",
        "tqdm>=4.66.4",
        "joblib>=1.4.2",
    ],
    extras_require={
        "dev": [
            "ipython>=8.26.0",
            "ipykernel>=6.29.4",
            "jupyter>=1.0.0",
            "notebook>=7.2.1",
            "pytest>=8.0.0",
            "pytest-cov>=4.1.0",
            "black>=24.0.0",
            "flake8>=7.0.0",
            "mypy>=1.8.0",
        ],
    },
    entry_points={
        "console_scripts": [
            "train-recommender=Train.training:main",
            "score-recommender=Score.score:main",
        ],
    },
)
