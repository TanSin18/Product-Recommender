# Product Recommender System

A comprehensive machine learning-based product recommendation system for e-commerce, utilizing collaborative filtering and XGBoost classification to provide personalized product recommendations.

## Overview

This system provides three distinct recommendation approaches:

1. **Product-Pairs**: SKU-level collaborative filtering using BigQuery ML Matrix Factorization
2. **Inactive Customers**: XGBoost-based product type recommendations for re-engagement campaigns
3. **Active Customers**: (Planned) Real-time recommendations for active customers

## Features

- Multi-region collaborative filtering (Midwest, Northeast, South, West)
- XGBoost classification with isotonic calibration
- K-means clustering for customer segmentation
- BigQuery integration for scalable data processing
- Parallel processing for efficient model training and scoring

## Project Structure

```
Product-Recommender/
├── Product-Pairs/          # Collaborative filtering for product pairs
│   ├── query.sql          # BigQuery ML collaborative filtering pipeline
│   ├── sku_pair.ipynb     # Product pair analysis notebook
│   └── requirements.txt   # Python dependencies
├── Inactive_Customers/    # Re-engagement recommendations
│   ├── Train/            # Model training pipeline
│   │   ├── training.py          # XGBoost model training
│   │   ├── cluster_training.sql # K-means clustering
│   │   └── training_query.sql   # Training data preparation
│   ├── Score/            # Model scoring pipeline
│   │   ├── score.py            # Customer scoring
│   │   ├── cluster_predict.sql # Cluster assignment
│   │   └── query.sql           # Scoring data preparation
│   └── requirements.txt
└── Active_Customers/      # (Planned) Active customer recommendations

```

## System Architecture

### Product-Pairs Module
- Uses transaction history to identify frequently co-purchased products
- Implements Matrix Factorization via BigQuery ML
- Segments customers by geographic region for localized recommendations
- Generates SKU-level product pair recommendations

### Inactive Customers Module

#### Training Pipeline
1. Extracts features from reactivated customers' purchase history
2. Trains separate XGBoost classifiers for top product types
3. Applies isotonic calibration for probability estimates
4. Creates clustering dataset from predictions
5. Trains K-means model for customer segmentation

#### Scoring Pipeline
1. Identifies at-risk/lapsed customers
2. Generates product type probabilities using trained models
3. Assigns customers to clusters
4. Outputs personalized recommendations

## Prerequisites

- Python 3.8+
- Google Cloud Platform account with BigQuery access
- Service account credentials (JSON key file)
- Access to required BigQuery datasets

## Installation

1. Clone the repository:
```bash
git clone https://github.com/TanSin18/Product-Recommender.git
cd Product-Recommender
```

2. Install dependencies for each module:
```bash
# For Inactive Customers module
cd Inactive_Customers
pip install -r requirements.txt

# For Product-Pairs module
cd ../Product-Pairs
pip install -r requirements.txt
```

3. Configure GCP credentials:
```bash
export GOOGLE_APPLICATION_CREDENTIALS="/path/to/your/key.json"
```

## Usage

### Product-Pairs Recommendations

1. Run the BigQuery SQL pipeline:
```bash
bq query --use_legacy_sql=false < Product-Pairs/query.sql
```

2. Analyze results using the Jupyter notebook:
```bash
jupyter notebook Product-Pairs/sku_pair.ipynb
```

### Inactive Customer Recommendations

#### Training Phase
```bash
cd Inactive_Customers/Train
python training.py
```

#### Scoring Phase
```bash
cd ../Score
python score.py
```

## Model Details

### XGBoost Hyperparameters
- Learning rate: 0.01
- Number of estimators: 1000
- Max depth: 5
- Subsample: 0.9
- Colsample by tree: 0.6
- Objective: binary:logistic

### Feature Engineering
- **Customer Demographics**: Income, race, home square footage, mover status
- **RFM Features**: Recency, frequency, monetary metrics (2-year lookback)
- **Shopping Behavior**: Average transaction value, items per transaction, merchandise divisions
- **Promotional Engagement**: Coupon usage, direct mail recency, redemption rates
- **Temporal Features**: Time since last purchase, seasonal patterns
- **Life Stage Events**: Wedding, baby, retirement, new mover

## Data Pipeline

### Training Data
- Source: Reactivated customers (past 1 year)
- Filter: Customers with 365-720 day purchase intervals
- Features: 13 engineered features from multiple data sources
- Target: Product type purchased at reactivation

### Scoring Data
- Source: At-risk/lapsed customer segments
- Real-time feature extraction from latest customer state
- Same 13 features as training for consistency

## Performance Considerations

- Parallel processing using ProcessPoolExecutor (72 workers)
- BigQuery Storage API for efficient data transfer
- Pickle serialization for model persistence
- Incremental processing for large customer bases

## Output Tables

### Product-Pairs
- `layer2_recommendations_[Region]`: Regional product recommendations
- `prod_type_ref`: Product type reference mappings

### Inactive Customers
- `prod_type_cluster_data`: Clustering input data
- `cluster_training_prod_type`: K-means model
- `lapsed_atrisk_clusters`: Customer cluster assignments
- `scored_cluster_data`: Final scored recommendations

## Best Practices

1. **Data Freshness**: Run training quarterly, scoring monthly
2. **Model Monitoring**: Track prediction distributions and cluster sizes
3. **A/B Testing**: Validate recommendations against control groups
4. **Privacy**: Ensure compliance with data retention and privacy policies
5. **Scalability**: Adjust parallel workers based on available compute resources

## Troubleshooting

### Common Issues

**Connection Timeout**
- Increase BigQuery client timeout
- Check network connectivity
- Verify service account permissions

**Memory Errors**
- Reduce batch size in data loading
- Process product types sequentially instead of in parallel
- Use sampling for initial testing

**Model Performance**
- Review feature importance plots
- Check for data drift in feature distributions
- Validate calibration curves

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests for new functionality
5. Submit a pull request

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Acknowledgments

- Built with Google Cloud BigQuery ML
- XGBoost library for gradient boosting
- scikit-learn for model calibration and preprocessing

## Contact

For questions or issues, please open an issue on GitHub or contact the maintainers.

## Roadmap

- [ ] Implement Active Customers module
- [ ] Add model explainability (SHAP values)
- [ ] Create REST API for real-time scoring
- [ ] Add automated model retraining pipeline
- [ ] Implement MLflow for experiment tracking
- [ ] Add comprehensive unit tests
- [ ] Create Docker containers for deployment
- [ ] Add CI/CD pipeline
