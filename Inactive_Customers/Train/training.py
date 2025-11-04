"""
Training pipeline for product recommendation using XGBoost with calibration.

This module trains separate XGBoost classifiers for each product type to predict
customer purchase probability, applies isotonic calibration, and generates clustering data.
"""

import logging
import sys
from pathlib import Path
from typing import List, Tuple
import warnings

import pandas as pd
import numpy as np
from sklearn.calibration import CalibratedClassifierCV
from xgboost import XGBClassifier
from concurrent.futures import ProcessPoolExecutor, as_completed

# Add parent directory to path for imports
sys.path.append(str(Path(__file__).parent.parent))

from config import get_config, Config
from utils import (
    BigQueryClient,
    DataProcessor,
    ModelPersistence,
    setup_logging,
    logger
)

warnings.filterwarnings('ignore')


class ProductRecommendationTrainer:
    """
    Trainer for product recommendation models.

    Trains XGBoost classifiers with isotonic calibration for each product type
    to predict purchase probability for customer re-engagement.
    """

    def __init__(self, config: Config):
        """
        Initialize the trainer.

        Args:
            config: Application configuration object
        """
        self.config = config
        self.bq_client = BigQueryClient(
            config.bigquery.credentials_path,
            config.bigquery.project_id
        )
        self.data_processor = DataProcessor()
        self.model_persistence = ModelPersistence()

        logger.info("ProductRecommendationTrainer initialized")

    def load_training_data(self) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        """
        Load all required training data from BigQuery.

        Returns:
            Tuple of (training_data, product_types, train_to_predict) DataFrames

        Raises:
            Exception: If data loading fails
        """
        logger.info("Loading training data from BigQuery...")

        try:
            # Load training data
            training_query = f"""
                SELECT * FROM `{self.config.bigquery.dataset}.{self.config.training.training_data_table}`
                LIMIT {self.config.training.training_limit}
            """
            training_data = self.bq_client.execute_query(training_query)
            logger.info(f"Loaded {len(training_data)} training records")

            # Load product types
            prod_types_query = f"""
                SELECT * FROM `{self.config.bigquery.dataset}.{self.config.training.prod_types_table}`
                LIMIT {self.config.training.prod_types_limit}
            """
            prod_types = self.bq_client.execute_query(prod_types_query)
            # Skip first row as per original logic
            prod_types = prod_types[1:]
            logger.info(f"Loaded {len(prod_types)} product types")

            # Load prediction dataset for clustering
            train_to_predict_query = f"""
                SELECT customer_id, {', '.join(self.config.features.features)}
                FROM `{self.config.bigquery.dataset}.{self.config.training.training_data_table}`
                GROUP BY customer_id, {', '.join(self.config.features.features)}
                LIMIT {self.config.training.train_to_predict_limit}
            """
            train_to_predict = self.bq_client.execute_query(train_to_predict_query)
            logger.info(f"Loaded {len(train_to_predict)} records for clustering")

            return training_data, prod_types, train_to_predict

        except Exception as e:
            logger.error(f"Failed to load training data: {str(e)}")
            raise

    def prepare_features(
        self,
        df: pd.DataFrame,
        is_training: bool = True
    ) -> pd.DataFrame:
        """
        Prepare features for model training or prediction.

        Args:
            df: Input DataFrame
            is_training: Whether this is for training (vs prediction)

        Returns:
            DataFrame with prepared features
        """
        logger.info("Preparing features...")

        # Select and impute features
        X = self.data_processor.apply_feature_imputation(
            df,
            self.config.features.features,
            self.config.features.imputation_rules
        )

        if self.config.debug:
            logger.debug(f"Features shape: {X.shape}")
            logger.debug(f"Features: {list(X.columns)}")

        return X

    def train_model_for_product(
        self,
        product_id: int,
        X: pd.DataFrame,
        training_data: pd.DataFrame
    ) -> None:
        """
        Train and calibrate a model for a specific product type.

        Args:
            product_id: Product type ID to train for
            X: Feature matrix
            training_data: Full training dataset with target column

        Raises:
            Exception: If training fails
        """
        try:
            logger.info(f"Training model for product type {product_id}")

            # Create binary target
            y = self.data_processor.create_binary_target(
                training_data,
                'pdm_prod_type_id',
                product_id
            )

            # Train base XGBoost model
            model = XGBClassifier(**self.config.model.to_xgb_params())
            model.fit(X, y)

            logger.info(f"Base model trained for product {product_id}")

            # Apply calibration
            calibrated_model = CalibratedClassifierCV(
                estimator=model,
                method=self.config.model.calibration_method,
                cv=self.config.model.calibration_cv,
                n_jobs=self.config.model.calibration_n_jobs
            )
            calibrated_model.fit(X, y)

            logger.info(f"Model calibrated for product {product_id}")

            # Save model
            model_path = self.config.training.get_model_path(product_id)
            self.model_persistence.save_model(model, model_path)

            logger.info(f"✓ Model saved for product {product_id}")

        except Exception as e:
            logger.error(f"Failed to train model for product {product_id}: {str(e)}")
            raise

    def train_all_models(
        self,
        training_data: pd.DataFrame,
        product_types: pd.DataFrame
    ) -> None:
        """
        Train models for all product types.

        Args:
            training_data: Training dataset
            product_types: DataFrame with product type IDs

        Raises:
            Exception: If any model training fails
        """
        logger.info("=" * 60)
        logger.info("STARTING MODEL TRAINING")
        logger.info("=" * 60)

        # Sample training data if configured
        if self.config.training.training_sample_fraction < 1.0:
            training_data = training_data.sample(
                frac=self.config.training.training_sample_fraction,
                random_state=self.config.model.random_state
            )
            logger.info(f"Sampled {len(training_data)} records for training")

        # Prepare features once for all models
        X = self.prepare_features(training_data, is_training=True)

        # Train models for each product type
        for prod_id in product_types['pdm_prod_type_id']:
            self.train_model_for_product(int(prod_id), X, training_data)

        logger.info("=" * 60)
        logger.info("MODEL TRAINING COMPLETED")
        logger.info("=" * 60)

    def generate_predictions(
        self,
        data: pd.DataFrame,
        product_types: pd.DataFrame
    ) -> pd.DataFrame:
        """
        Generate predictions for clustering data.

        Args:
            data: Input data to score
            product_types: DataFrame with product type IDs

        Returns:
            DataFrame with predictions for all product types
        """
        logger.info("=" * 60)
        logger.info("GENERATING PREDICTIONS FOR CLUSTERING")
        logger.info("=" * 60)

        data = data.reset_index(drop=True)
        customers = data["customer_id"]

        # Prepare features
        X = self.prepare_features(data, is_training=False)

        all_predictions = pd.DataFrame()

        # Generate predictions for each product type
        for prod_id in product_types['pdm_prod_type_id']:
            prod_id = int(prod_id)

            try:
                # Load model
                model_path = self.config.training.get_model_path(prod_id)
                model = self.model_persistence.load_model(model_path)

                # Predict probabilities
                prob = model.predict_proba(X)
                prob_df = pd.DataFrame(
                    data=prob[:, 1],
                    columns=['p'],
                    index=X.index.copy()
                )
                prob_df["pdm_prod_type_id"] = prod_id

                # Merge with customer IDs
                predictions = pd.merge(
                    customers,
                    prob_df,
                    how='left',
                    left_index=True,
                    right_index=True
                )

                all_predictions = pd.concat([all_predictions, predictions])

                logger.info(f"✓ Generated predictions for product {prod_id}")

            except Exception as e:
                logger.error(f"Failed to predict for product {prod_id}: {str(e)}")
                raise

        logger.info(f"Total predictions: {len(all_predictions)}")
        return all_predictions

    def create_clustering_data(
        self,
        predictions: pd.DataFrame
    ) -> pd.DataFrame:
        """
        Transform predictions to clustering format.

        Args:
            predictions: Long-format predictions DataFrame

        Returns:
            Wide-format DataFrame ready for clustering
        """
        logger.info("Creating clustering data format...")

        clustering_data = self.data_processor.pivot_predictions(predictions)

        logger.info(f"Clustering data shape: {clustering_data.shape}")
        return clustering_data

    def upload_results(self, clustering_data: pd.DataFrame) -> None:
        """
        Upload clustering data to BigQuery.

        Args:
            clustering_data: DataFrame to upload

        Raises:
            Exception: If upload fails
        """
        logger.info("Uploading clustering data to BigQuery...")

        try:
            self.bq_client.upload_dataframe(
                clustering_data,
                self.config.bigquery.dataset,
                self.config.training.output_table,
                if_exists='replace'
            )

            logger.info("✓ Clustering data uploaded successfully")

        except Exception as e:
            logger.error(f"Failed to upload clustering data: {str(e)}")
            raise

    def run(self) -> None:
        """
        Execute the complete training pipeline.

        This method orchestrates the entire training workflow:
        1. Load training data
        2. Train models for all product types
        3. Generate predictions
        4. Create clustering dataset
        5. Upload results to BigQuery
        """
        try:
            logger.info("\n" + "=" * 60)
            logger.info("PRODUCT RECOMMENDATION TRAINING PIPELINE")
            logger.info("=" * 60 + "\n")

            # Load data
            training_data, product_types, train_to_predict = self.load_training_data()

            # Train models
            self.train_all_models(training_data, product_types)

            # Generate predictions
            predictions = self.generate_predictions(train_to_predict, product_types)

            # Create clustering data
            clustering_data = self.create_clustering_data(predictions)

            # Upload results
            self.upload_results(clustering_data)

            logger.info("\n" + "=" * 60)
            logger.info("TRAINING PIPELINE COMPLETED SUCCESSFULLY")
            logger.info("=" * 60 + "\n")

        except Exception as e:
            logger.error(f"\n{'=' * 60}")
            logger.error("TRAINING PIPELINE FAILED")
            logger.error(f"{'=' * 60}")
            logger.error(f"Error: {str(e)}", exc_info=True)
            raise


def main():
    """Main entry point for the training pipeline."""
    # Setup logging
    setup_logging(
        log_level='INFO',
        log_file='training.log'
    )

    try:
        # Load configuration
        config = get_config()

        # Create and run trainer
        trainer = ProductRecommendationTrainer(config)
        trainer.run()

    except Exception as e:
        logger.error("Training pipeline failed with error:", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
