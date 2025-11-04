"""
Scoring pipeline for product recommendations using trained XGBoost models.

This module loads trained models and scores at-risk/lapsed customers to generate
personalized product recommendations for re-engagement campaigns.
"""

import logging
import sys
from pathlib import Path
from typing import Tuple
import warnings

import pandas as pd
import numpy as np

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


class ProductRecommendationScorer:
    """
    Scorer for product recommendations.

    Loads trained XGBoost models and generates product recommendations
    for at-risk and lapsed customers.
    """

    def __init__(self, config: Config):
        """
        Initialize the scorer.

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

        logger.info("ProductRecommendationScorer initialized")

    def load_scoring_data(self) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Load scoring data from BigQuery.

        Returns:
            Tuple of (scoring_data, product_types) DataFrames

        Raises:
            Exception: If data loading fails
        """
        logger.info("Loading scoring data from BigQuery...")

        try:
            # Load scoring data
            scoring_query = f"""
                SELECT * FROM `{self.config.bigquery.dataset}.{self.config.scoring.scoring_data_table}`
            """
            scoring_data = self.bq_client.execute_query(scoring_query)
            logger.info(f"Loaded {len(scoring_data)} customers to score")

            # Load product types
            prod_types_query = f"""
                SELECT * FROM `{self.config.bigquery.dataset}.{self.config.scoring.prod_types_table}`
            """
            prod_types = self.bq_client.execute_query(prod_types_query)
            # Skip first row as per original logic
            prod_types = prod_types[1:]
            logger.info(f"Loaded {len(prod_types)} product types")

            return scoring_data, prod_types

        except Exception as e:
            logger.error(f"Failed to load scoring data: {str(e)}")
            raise

    def prepare_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Prepare features for model scoring.

        Args:
            df: Input DataFrame

        Returns:
            DataFrame with prepared features
        """
        logger.info("Preparing features for scoring...")

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

    def generate_predictions(
        self,
        scoring_data: pd.DataFrame,
        product_types: pd.DataFrame
    ) -> pd.DataFrame:
        """
        Generate predictions for all product types.

        Args:
            scoring_data: Customer data to score
            product_types: DataFrame with product type IDs

        Returns:
            DataFrame with predictions for all customers and product types

        Raises:
            Exception: If prediction generation fails
        """
        logger.info("=" * 60)
        logger.info("GENERATING PRODUCT RECOMMENDATIONS")
        logger.info("=" * 60)

        scoring_data = scoring_data.reset_index(drop=True)
        customers = scoring_data["customer_id"]

        # Prepare features
        X = self.prepare_features(scoring_data)

        all_predictions = pd.DataFrame()

        # Generate predictions for each product type
        for prod_id in product_types['pdm_prod_type_id']:
            prod_id = int(prod_id)

            try:
                # Load model
                model_path = self.config.scoring.get_model_path(prod_id)
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

                # Save intermediate results
                predictions_dir = Path(self.config.scoring.predictions_file).parent
                predictions_dir.mkdir(parents=True, exist_ok=True)
                self.model_persistence.save_dataframe(
                    all_predictions,
                    self.config.scoring.predictions_file
                )

                logger.info(f"✓ Generated predictions for product {prod_id}")

            except FileNotFoundError:
                logger.warning(
                    f"Model not found for product {prod_id}. "
                    f"Skipping... (path: {model_path})"
                )
                continue

            except Exception as e:
                logger.error(f"Failed to predict for product {prod_id}: {str(e)}")
                raise

        if all_predictions.empty:
            raise ValueError("No predictions generated. Check if models exist.")

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
        Upload scored clustering data to BigQuery.

        Args:
            clustering_data: DataFrame to upload

        Raises:
            Exception: If upload fails
        """
        logger.info("Uploading scored data to BigQuery...")

        try:
            self.bq_client.upload_dataframe(
                clustering_data,
                self.config.bigquery.dataset,
                self.config.scoring.output_table,
                if_exists='replace'
            )

            logger.info("✓ Scored data uploaded successfully")

        except Exception as e:
            logger.error(f"Failed to upload scored data: {str(e)}")
            raise

    def run(self) -> pd.DataFrame:
        """
        Execute the complete scoring pipeline.

        This method orchestrates the entire scoring workflow:
        1. Load scoring data
        2. Generate predictions using trained models
        3. Create clustering dataset
        4. Upload results to BigQuery

        Returns:
            Final clustering dataset

        Raises:
            Exception: If scoring pipeline fails
        """
        try:
            logger.info("\n" + "=" * 60)
            logger.info("PRODUCT RECOMMENDATION SCORING PIPELINE")
            logger.info("=" * 60 + "\n")

            # Load data
            scoring_data, product_types = self.load_scoring_data()

            # Generate predictions
            predictions = self.generate_predictions(scoring_data, product_types)

            # Create clustering data
            clustering_data = self.create_clustering_data(predictions)

            # Upload results
            self.upload_results(clustering_data)

            logger.info("\n" + "=" * 60)
            logger.info("SCORING PIPELINE COMPLETED SUCCESSFULLY")
            logger.info("=" * 60 + "\n")

            return clustering_data

        except Exception as e:
            logger.error(f"\n{'=' * 60}")
            logger.error("SCORING PIPELINE FAILED")
            logger.error(f"{'=' * 60}")
            logger.error(f"Error: {str(e)}", exc_info=True)
            raise


def main():
    """Main entry point for the scoring pipeline."""
    # Setup logging
    setup_logging(
        log_level='INFO',
        log_file='scoring.log'
    )

    try:
        # Load configuration
        config = get_config()

        # Create and run scorer
        scorer = ProductRecommendationScorer(config)
        results = scorer.run()

        logger.info(f"Scoring completed. Final dataset shape: {results.shape}")

    except Exception as e:
        logger.error("Scoring pipeline failed with error:", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
