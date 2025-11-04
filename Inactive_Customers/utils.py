"""Utility functions for the Inactive Customers recommendation system."""

import logging
import pickle
from pathlib import Path
from typing import Optional, Tuple, Any
import pandas as pd
import numpy as np
from google.oauth2 import service_account
from google.cloud import bigquery
from google.cloud import bigquery_storage


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class BigQueryClient:
    """Wrapper for BigQuery client with convenience methods."""

    def __init__(self, credentials_path: str, project_id: Optional[str] = None):
        """
        Initialize BigQuery client.

        Args:
            credentials_path: Path to GCP service account JSON key file
            project_id: GCP project ID (if None, uses credentials default)
        """
        self.credentials = service_account.Credentials.from_service_account_file(
            credentials_path,
            scopes=["https://www.googleapis.com/auth/cloud-platform"],
        )

        self.project_id = project_id or self.credentials.project_id

        self.bq_client = bigquery.Client(
            credentials=self.credentials,
            project=self.project_id
        )

        self.bq_storage_client = bigquery_storage.BigQueryReadClient(
            credentials=self.credentials
        )

        logger.info(f"BigQuery client initialized for project: {self.project_id}")

    def execute_query(
        self,
        query: str,
        use_storage_api: bool = True
    ) -> pd.DataFrame:
        """
        Execute a BigQuery query and return results as DataFrame.

        Args:
            query: SQL query string
            use_storage_api: Whether to use BigQuery Storage API for faster reads

        Returns:
            Query results as pandas DataFrame

        Raises:
            Exception: If query execution fails
        """
        try:
            logger.info("Executing BigQuery query...")
            logger.debug(f"Query: {query[:200]}...")

            query_job = self.bq_client.query(query)

            if use_storage_api:
                df = query_job.result().to_dataframe(
                    bqstorage_client=self.bq_storage_client
                )
            else:
                df = query_job.result().to_dataframe()

            logger.info(f"Query returned {len(df)} rows")
            return df

        except Exception as e:
            logger.error(f"Query execution failed: {str(e)}")
            raise

    def load_table(
        self,
        dataset: str,
        table_name: str,
        limit: Optional[int] = None
    ) -> pd.DataFrame:
        """
        Load a BigQuery table.

        Args:
            dataset: Dataset name
            table_name: Table name
            limit: Optional row limit

        Returns:
            Table data as pandas DataFrame
        """
        query = f"SELECT * FROM `{self.project_id}.{dataset}.{table_name}`"
        if limit:
            query += f" LIMIT {limit}"

        return self.execute_query(query)

    def upload_dataframe(
        self,
        df: pd.DataFrame,
        dataset: str,
        table_name: str,
        if_exists: str = 'replace'
    ) -> None:
        """
        Upload a DataFrame to BigQuery.

        Args:
            df: DataFrame to upload
            dataset: Dataset name
            table_name: Table name
            if_exists: What to do if table exists ('fail', 'replace', 'append')
        """
        try:
            logger.info(f"Uploading {len(df)} rows to {dataset}.{table_name}")

            df.to_gbq(
                destination_table=f'{dataset}.{table_name}',
                project_id=self.project_id,
                if_exists=if_exists,
                credentials=self.credentials
            )

            logger.info("Upload completed successfully")

        except Exception as e:
            logger.error(f"Upload failed: {str(e)}")
            raise


class DataProcessor:
    """Data processing utilities."""

    @staticmethod
    def apply_feature_imputation(
        df: pd.DataFrame,
        features: list,
        imputation_rules: dict
    ) -> pd.DataFrame:
        """
        Apply feature imputation rules to a DataFrame.

        Args:
            df: Input DataFrame
            features: List of feature column names
            imputation_rules: Dictionary mapping feature patterns to fill values

        Returns:
            DataFrame with imputed values
        """
        logger.info("Applying feature imputation...")

        # Convert to numeric, coercing errors to NaN
        df_imputed = pd.to_numeric(df[features].stack(), errors='coerce').unstack()

        # Apply imputation rules
        for col in df_imputed.columns:
            filled = False
            for pattern, fill_value in imputation_rules.items():
                if pattern == 'default':
                    continue
                if pattern in col.upper():
                    df_imputed[col].fillna(fill_value, inplace=True)
                    filled = True
                    break

            if not filled:
                df_imputed[col].fillna(imputation_rules['default'], inplace=True)

        logger.info(f"Imputation complete for {len(features)} features")
        return df_imputed

    @staticmethod
    def create_binary_target(
        df: pd.DataFrame,
        target_column: str,
        target_value: Any
    ) -> pd.Series:
        """
        Create binary target variable.

        Args:
            df: Input DataFrame
            target_column: Column name containing target values
            target_value: Value to encode as 1 (all others as 0)

        Returns:
            Binary target series
        """
        return np.where(df[target_column] == target_value, 1, 0)

    @staticmethod
    def pivot_predictions(
        predictions: pd.DataFrame,
        index_col: str = 'customer_id',
        columns_col: str = 'pdm_prod_type_id',
        values_col: str = 'p'
    ) -> pd.DataFrame:
        """
        Pivot predictions from long to wide format.

        Args:
            predictions: Long-format predictions DataFrame
            index_col: Column to use as index
            columns_col: Column to use as columns
            values_col: Column containing values

        Returns:
            Wide-format DataFrame with pivoted predictions
        """
        logger.info("Pivoting predictions to wide format...")

        predictions_wide = pd.pivot(
            predictions,
            index=[index_col],
            columns=columns_col,
            values=values_col
        )

        # Rename columns with prefix
        column_names = predictions_wide.columns.values.tolist()
        column_names_prep = ["p" + str(sub) for sub in column_names]
        predictions_wide.columns = column_names_prep

        # Reset index
        predictions_wide['customer_id'] = predictions_wide.index
        predictions_wide = predictions_wide.reset_index(drop=True)

        logger.info(f"Pivoted to {len(predictions_wide)} rows x {len(predictions_wide.columns)} columns")
        return predictions_wide


class ModelPersistence:
    """Model persistence utilities."""

    @staticmethod
    def save_model(model: Any, filepath: str) -> None:
        """
        Save a model to disk using pickle.

        Args:
            model: Model object to save
            filepath: Path to save the model
        """
        try:
            # Ensure directory exists
            Path(filepath).parent.mkdir(parents=True, exist_ok=True)

            with open(filepath, 'wb') as f:
                pickle.dump(model, f)

            logger.info(f"Model saved to {filepath}")

        except Exception as e:
            logger.error(f"Failed to save model: {str(e)}")
            raise

    @staticmethod
    def load_model(filepath: str) -> Any:
        """
        Load a model from disk.

        Args:
            filepath: Path to the model file

        Returns:
            Loaded model object

        Raises:
            FileNotFoundError: If model file doesn't exist
        """
        try:
            if not Path(filepath).exists():
                raise FileNotFoundError(f"Model file not found: {filepath}")

            with open(filepath, 'rb') as f:
                model = pickle.load(f)

            logger.info(f"Model loaded from {filepath}")
            return model

        except Exception as e:
            logger.error(f"Failed to load model: {str(e)}")
            raise

    @staticmethod
    def save_dataframe(df: pd.DataFrame, filepath: str) -> None:
        """
        Save a DataFrame to pickle.

        Args:
            df: DataFrame to save
            filepath: Path to save the DataFrame
        """
        try:
            Path(filepath).parent.mkdir(parents=True, exist_ok=True)

            df.to_pickle(filepath)
            logger.info(f"DataFrame saved to {filepath} ({len(df)} rows)")

        except Exception as e:
            logger.error(f"Failed to save DataFrame: {str(e)}")
            raise

    @staticmethod
    def load_dataframe(filepath: str) -> pd.DataFrame:
        """
        Load a DataFrame from pickle.

        Args:
            filepath: Path to the pickle file

        Returns:
            Loaded DataFrame
        """
        try:
            if not Path(filepath).exists():
                raise FileNotFoundError(f"File not found: {filepath}")

            df = pd.read_pickle(filepath)
            logger.info(f"DataFrame loaded from {filepath} ({len(df)} rows)")
            return df

        except Exception as e:
            logger.error(f"Failed to load DataFrame: {str(e)}")
            raise


def setup_logging(log_level: str = 'INFO', log_file: Optional[str] = None) -> None:
    """
    Setup logging configuration.

    Args:
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        log_file: Optional log file path
    """
    handlers = [logging.StreamHandler()]

    if log_file:
        handlers.append(logging.FileHandler(log_file))

    logging.basicConfig(
        level=getattr(logging, log_level.upper()),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=handlers
    )


if __name__ == "__main__":
    # Test utilities
    setup_logging('DEBUG')
    logger.info("Utilities module loaded successfully!")
