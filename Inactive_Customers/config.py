"""Configuration management for the Inactive Customers recommendation system."""

import os
from pathlib import Path
from typing import Dict, Any, Optional
from dataclasses import dataclass, field


@dataclass
class BigQueryConfig:
    """BigQuery configuration settings."""

    project_id: Optional[str] = None
    dataset: str = "SANDBOX_ANALYTICS"
    credentials_path: Optional[str] = None

    def __post_init__(self):
        """Initialize credentials path from environment if not provided."""
        if self.credentials_path is None:
            self.credentials_path = os.getenv(
                'GOOGLE_APPLICATION_CREDENTIALS',
                '/home/jupyter/d00_key.json'
            )

        if not os.path.exists(self.credentials_path):
            raise FileNotFoundError(
                f"Credentials file not found: {self.credentials_path}"
            )


@dataclass
class ModelConfig:
    """XGBoost model configuration."""

    learning_rate: float = 0.01
    n_estimators: int = 1000
    max_depth: int = 5
    gamma: float = 0
    subsample: float = 0.9
    colsample_bytree: float = 0.6
    objective: str = 'binary:logistic'
    n_jobs: int = -1
    scale_pos_weight: float = 1.0
    random_state: int = 42

    # Calibration settings
    calibration_method: str = 'isotonic'
    calibration_cv: str = 'prefit'
    calibration_n_jobs: int = -1

    def to_xgb_params(self) -> Dict[str, Any]:
        """Convert to XGBoost parameters dict."""
        return {
            'learning_rate': self.learning_rate,
            'n_estimators': self.n_estimators,
            'max_depth': self.max_depth,
            'gamma': self.gamma,
            'subsample': self.subsample,
            'colsample_bytree': self.colsample_bytree,
            'objective': self.objective,
            'n_jobs': self.n_jobs,
            'scale_pos_weight': self.scale_pos_weight,
            'random_state': self.random_state,
        }


@dataclass
class FeatureConfig:
    """Feature engineering configuration."""

    features: list = field(default_factory=lambda: [
        'BBB_INSTORE_M_DECILE_2Y',
        'A_AAP000447N_ASET_PRPN_DIS_INC',
        'time_interval',
        'PH_DM_RECENCY',
        'AVG_NET_SALES_PER_TXN',
        'COUPON_SALES_Q_08',
        'A_A3101N_RACE_WHITE',
        'BBB_R_2Y',
        'MOVER',
        'NUM_MERCH_DIVISIONS',
        'AVG_TOTAL_ITEMS_PER_TXN',
        'A_A8588N_HM_SQR_FT',
        'PH_MREDEEM730D_PERC'
    ])

    # Feature imputation strategies
    imputation_rules: Dict[str, float] = field(default_factory=lambda: {
        'DECILE': 11,
        '_R_2Y': 720,
        'RECENCY': 365,
        'default': 0
    })

    def get_imputation_value(self, column_name: str) -> float:
        """Get imputation value for a specific column."""
        for key, value in self.imputation_rules.items():
            if key in column_name.upper():
                return value
        return self.imputation_rules['default']


@dataclass
class TrainingConfig:
    """Training pipeline configuration."""

    # Data sampling
    training_sample_fraction: float = 0.60
    training_limit: int = 1000
    prod_types_limit: int = 2
    train_to_predict_limit: int = 100

    # Queries
    training_data_table: str = "reengagement_product_recommendation_training"
    prod_types_table: str = "shopping_prod"
    output_table: str = "prod_type_cluster_data"

    # Model persistence
    model_dir: str = "./models"
    model_prefix: str = "cali_model_"
    model_extension: str = ".pkl"

    def get_model_path(self, prod_type_id: int) -> str:
        """Get the model file path for a specific product type."""
        os.makedirs(self.model_dir, exist_ok=True)
        return os.path.join(
            self.model_dir,
            f"{self.model_prefix}{prod_type_id}{self.model_extension}"
        )


@dataclass
class ScoringConfig:
    """Scoring pipeline configuration."""

    # Queries
    scoring_data_table: str = "reengagement_product_recommendation_scoring"
    prod_types_table: str = "shopping_prod"
    output_table: str = "scored_cluster_data"

    # Model persistence
    model_dir: str = "./models"
    model_prefix: str = "cali_model_"
    model_extension: str = ".pkl"
    predictions_file: str = "./predictions/prod_type_p.pkl"

    def get_model_path(self, prod_type_id: int) -> str:
        """Get the model file path for a specific product type."""
        return os.path.join(
            self.model_dir,
            f"{self.model_prefix}{prod_type_id}{self.model_extension}"
        )


@dataclass
class ProcessingConfig:
    """Parallel processing configuration."""

    max_workers: int = 72
    use_multiprocessing: bool = True

    def __post_init__(self):
        """Validate and adjust worker count."""
        cpu_count = os.cpu_count() or 1
        if self.max_workers > cpu_count * 2:
            print(f"Warning: max_workers ({self.max_workers}) exceeds "
                  f"2x CPU count ({cpu_count}). Adjusting to {cpu_count * 2}")
            self.max_workers = cpu_count * 2


@dataclass
class Config:
    """Main configuration container."""

    bigquery: BigQueryConfig = field(default_factory=BigQueryConfig)
    model: ModelConfig = field(default_factory=ModelConfig)
    features: FeatureConfig = field(default_factory=FeatureConfig)
    training: TrainingConfig = field(default_factory=TrainingConfig)
    scoring: ScoringConfig = field(default_factory=ScoringConfig)
    processing: ProcessingConfig = field(default_factory=ProcessingConfig)

    # Environment
    environment: str = field(default_factory=lambda: os.getenv('ENV', 'development'))
    debug: bool = field(default_factory=lambda: os.getenv('DEBUG', 'False').lower() == 'true')

    @classmethod
    def from_env(cls) -> 'Config':
        """Create configuration from environment variables."""
        config = cls()

        # Override from environment variables
        if project_id := os.getenv('GCP_PROJECT_ID'):
            config.bigquery.project_id = project_id

        if dataset := os.getenv('BQ_DATASET'):
            config.bigquery.dataset = dataset

        if model_dir := os.getenv('MODEL_DIR'):
            config.training.model_dir = model_dir
            config.scoring.model_dir = model_dir

        if max_workers := os.getenv('MAX_WORKERS'):
            config.processing.max_workers = int(max_workers)

        return config

    def validate(self) -> bool:
        """Validate configuration."""
        errors = []

        # Validate BigQuery config
        if not os.path.exists(self.bigquery.credentials_path):
            errors.append(f"Credentials file not found: {self.bigquery.credentials_path}")

        # Validate model config
        if self.model.learning_rate <= 0 or self.model.learning_rate >= 1:
            errors.append(f"Invalid learning rate: {self.model.learning_rate}")

        if self.model.n_estimators <= 0:
            errors.append(f"Invalid n_estimators: {self.model.n_estimators}")

        # Validate processing config
        if self.processing.max_workers <= 0:
            errors.append(f"Invalid max_workers: {self.processing.max_workers}")

        if errors:
            raise ValueError(f"Configuration validation failed:\n" + "\n".join(errors))

        return True


def get_config() -> Config:
    """Get the application configuration."""
    config = Config.from_env()
    config.validate()
    return config


if __name__ == "__main__":
    # Test configuration
    config = get_config()
    print("Configuration loaded successfully!")
    print(f"Environment: {config.environment}")
    print(f"Project ID: {config.bigquery.project_id}")
    print(f"Dataset: {config.bigquery.dataset}")
    print(f"Model parameters: {config.model.to_xgb_params()}")
