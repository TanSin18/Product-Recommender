# Inactive Customers Recommendation Module

This module provides product recommendations for inactive, at-risk, and lapsed customers using XGBoost classification with isotonic calibration.

## Overview

The system identifies customers who have stopped purchasing and provides personalized product recommendations to drive re-engagement campaigns.

## Architecture

### Training Pipeline (`Train/training.py`)
1. **Data Loading**: Extracts features from reactivated customers
2. **Model Training**: Trains XGBoost classifiers for each product type
3. **Calibration**: Applies isotonic calibration for accurate probability estimates
4. **Clustering Preparation**: Generates probability matrix for customer segmentation

### Scoring Pipeline (`Score/score.py`)
1. **Customer Identification**: Loads at-risk/lapsed customer list
2. **Feature Engineering**: Extracts same features as training
3. **Prediction**: Generates product purchase probabilities
4. **Clustering**: Assigns customers to behavioral segments

## Module Structure

```
Inactive_Customers/
├── config.py              # Configuration management
├── utils.py               # Shared utilities and helpers
├── requirements.txt       # Python dependencies
├── Train/
│   ├── training.py        # Training pipeline
│   ├── cluster_training.sql    # K-means clustering (BigQuery ML)
│   ├── training_query.sql      # Training data preparation
│   └── README.txt
└── Score/
    ├── score.py           # Scoring pipeline
    ├── cluster_predict.sql     # Cluster assignment
    ├── query.sql               # Scoring data preparation
    └── README.txt
```

## Features

The system uses 13 engineered features across multiple categories:

### Customer Demographics
- `A_AAP000447N_ASET_PRPN_DIS_INC`: Household income
- `A_A3101N_RACE_WHITE`: Race demographic
- `A_A8588N_HM_SQR_FT`: Home square footage
- `MOVER`: New mover status

### RFM (Recency, Frequency, Monetary)
- `BBB_R_2Y`: Recency (2-year lookback)
- `BBB_INSTORE_M_DECILE_2Y`: Monetary decile (2-year)
- `time_interval`: Days since last purchase

### Shopping Behavior
- `AVG_NET_SALES_PER_TXN`: Average transaction value
- `AVG_TOTAL_ITEMS_PER_TXN`: Average items per basket
- `NUM_MERCH_DIVISIONS`: Product category diversity

### Promotional Engagement
- `COUPON_SALES_Q_08`: Coupon usage
- `PH_DM_RECENCY`: Direct mail recency
- `PH_MREDEEM730D_PERC`: Redemption rate

## Configuration

### Environment Variables

```bash
# Required
export GOOGLE_APPLICATION_CREDENTIALS="/path/to/key.json"

# Optional
export GCP_PROJECT_ID="your-project-id"
export BQ_DATASET="SANDBOX_ANALYTICS"
export MODEL_DIR="./models"
export MAX_WORKERS="72"
export ENV="production"  # or "development"
export DEBUG="False"  # Set to "True" for verbose logging
```

### Configuration File

Edit `config.py` to customize:
- Model hyperparameters
- Feature definitions
- Data sampling rates
- Processing settings
- BigQuery table names

## Usage

### Training

```bash
# Using Python directly
cd Inactive_Customers
python Train/training.py

# Using the installed package
train-recommender
```

### Scoring

```bash
# Using Python directly
cd Inactive_Customers
python Score/score.py

# Using the installed package
score-recommender
```

### BigQuery ML Clustering

After training or scoring, run the clustering SQL:

```bash
# Training clustering
bq query --use_legacy_sql=false < Train/cluster_training.sql

# Scoring clustering
bq query --use_legacy_sql=false < Score/cluster_predict.sql
```

## Model Details

### XGBoost Hyperparameters

- **Learning Rate**: 0.01 (slower learning for better generalization)
- **Estimators**: 1000 (boosting rounds)
- **Max Depth**: 5 (tree complexity)
- **Subsample**: 0.9 (row sampling)
- **Colsample by Tree**: 0.6 (feature sampling)
- **Objective**: binary:logistic

### Calibration

- **Method**: Isotonic regression
- **Purpose**: Converts model scores to calibrated probabilities
- **Benefit**: More accurate probability estimates for ranking

## Output Tables

### Training
- `prod_type_cluster_data`: Wide-format probability matrix
- `cluster_training_prod_type`: K-means model

### Scoring
- `scored_cluster_data`: Customer scores for clustering
- `lapsed_atrisk_clusters`: Final cluster assignments

## Performance

### Typical Runtime
- **Training**: ~30-60 minutes (depends on data size)
- **Scoring**: ~15-30 minutes (depends on customer count)

### Resource Requirements
- **Memory**: 16GB+ recommended
- **CPU**: Multi-core (parallel processing)
- **Disk**: 5GB for models and intermediate files

## Logging

Logs are written to:
- Console (stdout)
- `training.log` (training pipeline)
- `scoring.log` (scoring pipeline)

Log levels: DEBUG, INFO, WARNING, ERROR, CRITICAL

## Troubleshooting

### Common Issues

**Issue**: `FileNotFoundError: Credentials file not found`
- **Solution**: Set `GOOGLE_APPLICATION_CREDENTIALS` environment variable

**Issue**: `ValueError: No predictions generated`
- **Solution**: Run training pipeline first to create models

**Issue**: `MemoryError during training`
- **Solution**: Reduce `training_sample_fraction` in config.py

**Issue**: `Permission denied (BigQuery)`
- **Solution**: Ensure service account has BigQuery read/write permissions

### Debug Mode

Enable debug logging:

```bash
export DEBUG="True"
python Train/training.py
```

## Best Practices

1. **Data Freshness**: Retrain models quarterly
2. **Scoring Frequency**: Run scoring monthly
3. **Validation**: Monitor prediction distributions
4. **Model Governance**: Version control model files
5. **Performance Monitoring**: Track prediction accuracy over time

## Development

### Running Tests

```bash
pytest tests/
```

### Code Formatting

```bash
black Inactive_Customers/
```

### Type Checking

```bash
mypy Inactive_Customers/
```

## License

MIT License - see LICENSE file in project root.

## Support

For issues or questions:
- Open an issue on GitHub
- Check the main README.md for general information
