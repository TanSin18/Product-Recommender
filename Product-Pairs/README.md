# Product Pairs Recommendation Module

This module implements collaborative filtering using BigQuery ML to generate product pair recommendations based on customer purchase patterns.

## Overview

The Product-Pairs module uses Matrix Factorization to identify which products are frequently purchased together, enabling cross-sell and upsell opportunities.

## Features

- **Regional Segmentation**: Separate models for Midwest, Northeast, South, West, and Unknown regions
- **BigQuery ML Integration**: Leverages Google Cloud's native ML capabilities
- **SKU-Level Recommendations**: Granular product pair associations
- **Scalable Processing**: Handles millions of transactions

## Architecture

### Data Pipeline (`query.sql`)

1. **Transaction Aggregation**: Extract purchase history
2. **Customer Segmentation**: Group by geographic region
3. **Matrix Factorization**: Train collaborative filtering models
4. **Recommendation Generation**: Produce top-N product pairs

### Analysis Notebook (`sku_pair.ipynb`)

- Exploratory data analysis
- Product pair visualization
- Performance metrics
- Result validation

## Module Structure

```
Product-Pairs/
├── query.sql              # BigQuery ML pipeline
├── sku_pair.ipynb         # Analysis notebook
├── requirements.txt       # Python dependencies
└── README.md             # This file
```

## Data Model

### Input Tables

- `ITEM_SKU`: Product catalog
- `PDM_PROD_TYPE`: Product type mappings
- `sales_datamart_sales_transaction_line_sum`: Transaction details
- `CUSTOMER_ADDRESS`: Customer geography

### Output Tables

- `sales_transaction_distinct_skus`: Deduplicated SKU catalog
- `base_table`: Cleaned transaction history
- `customer_base_table`: Customer-product purchase counts
- `multi_trans`: Multi-product purchasers with regions
- `layer2_[Region]`: Regional training datasets
- `layer2_recommender_[Region]`: Trained matrix factorization models
- `l2recommendation_model_[Region]`: Model predictions
- `layer2_recommendations_[Region]`: Final recommendations

## Regional Models

### Regions

1. **Midwest**: IA, IL, IN, KS, MI, MN, MO, ND, NE, OH, SD, WI
2. **Northeast**: ME, NH, NJ, NY, PA, RI, VT
3. **South**: AL, AR, DC, DE, FL, GA, KY, LA, MD, MS, NC, OK, SC, TN, TX, VA, WV
4. **West**: AK, AZ, CA, CO, HI, ID, MT, NM, NV, OR, UT, WA, WY
5. **Unknown**: Missing or unmatched states

### Model Type

**Matrix Factorization** with:
- **User Column**: customer_id
- **Item Column**: pdm_prod_type_id
- **Rating Column**: trans_count (implicit feedback)
- **Feedback Type**: IMPLICIT

## Usage

### Running the Pipeline

```bash
# Execute the full BigQuery pipeline
bq query --use_legacy_sql=false < Product-Pairs/query.sql
```

### Querying Recommendations

```sql
-- Get top recommendations for a specific product
SELECT
  FOCUS_pdm_prod_type_id,
  focus_type,
  RECOMM_pdm_prod_type_id,
  recomm_type,
  predicted_trans_count_confidence,
  rn as rank
FROM `your-project.SANDBOX_ANALYTICS.layer2_recommendations_Midwest`
WHERE FOCUS_pdm_prod_type_id = 123
  AND rn <= 10
ORDER BY rn;
```

### Jupyter Notebook Analysis

```bash
cd Product-Pairs
jupyter notebook sku_pair.ipynb
```

## Configuration

### BigQuery Settings

Update project and dataset names in `query.sql`:

```sql
-- Change these references
`dw-bq-data-d00.SANDBOX_ANALYTICS.*`
`dw-bq-data-p00.ANALYTICAL.*`
```

### Time Windows

Default: 6 months of transaction history

```sql
-- Modify the interval in query.sql
transaction_booked_date between date_sub(current_date(), interval 6 MONTH)
and current_date()
```

## Output Format

### Recommendation Table Schema

| Column | Type | Description |
|--------|------|-------------|
| FOCUS_pdm_prod_type_id | INTEGER | Source product ID |
| focus_type | STRING | Source product description |
| RECOMM_pdm_prod_type_id | INTEGER | Recommended product ID |
| recomm_type | STRING | Recommended product description |
| predicted_trans_count_confidence | FLOAT | Confidence score |
| rn | INTEGER | Rank (1 = highest confidence) |

## Performance Metrics

### Typical Runtime
- **Data Preparation**: 10-15 minutes
- **Model Training**: 20-30 minutes per region
- **Prediction Generation**: 5-10 minutes per region
- **Total**: ~2-3 hours for all regions

### Data Volume
- Processes millions of transactions
- Generates thousands of recommendations per product
- Top 50 recommendations kept per product type

## Best Practices

1. **Refresh Frequency**: Weekly or bi-weekly
2. **Historical Window**: 6-12 months (balance recency vs. volume)
3. **Minimum Support**: Filter products with low transaction counts
4. **Validation**: Cross-reference with business rules
5. **Monitoring**: Track recommendation diversity and coverage

## Customization

### Adjusting Number of Recommendations

```sql
-- In query.sql, modify the where clause
where rn<=50  -- Change from 50 to desired number
```

### Adding New Regions

1. Create region definition in `multi_trans` table
2. Add training dataset (e.g., `layer2_NewRegion`)
3. Create model (`layer2_recommender_NewRegion`)
4. Generate predictions
5. Create recommendations table

### Feature Engineering

Add product attributes to enhance recommendations:

```sql
-- Join additional product features
INNER JOIN `product_features_table` pf
ON A.pdm_prod_type_id = pf.product_id
```

## Troubleshooting

### Common Issues

**Issue**: Empty recommendation tables
- **Cause**: Insufficient transaction history
- **Solution**: Increase time window or reduce minimum support

**Issue**: Poor recommendation quality
- **Cause**: Data sparsity or wrong region segmentation
- **Solution**: Aggregate to higher product levels or combine regions

**Issue**: Long query runtime
- **Cause**: Large dataset or complex joins
- **Solution**: Partition tables, optimize joins, use table sampling

### Query Optimization

```sql
-- Add table partitioning
CREATE TABLE dataset.table_name
PARTITION BY DATE(transaction_date)
AS SELECT ...

-- Use clustering
CLUSTER BY customer_id, product_id
```

## Notebook Usage

The `sku_pair.ipynb` notebook provides:

1. **Data Exploration**: Transaction patterns and distributions
2. **Product Analysis**: Co-purchase frequencies
3. **Recommendation Quality**: Precision, recall, coverage metrics
4. **Visualization**: Network graphs of product relationships

### Running the Notebook

```python
# Load credentials
key_path = '/path/to/credentials.json'
credentials = service_account.Credentials.from_service_account_file(
    key_path, scopes=["https://www.googleapis.com/auth/cloud-platform"]
)

# Query recommendations
query = "SELECT * FROM `dataset.layer2_recommendations_Midwest`"
df = bq_client.query(query).result().to_dataframe()

# Analyze
df.head()
```

## Integration

### Using Recommendations

```python
def get_recommendations(product_id: int, region: str, top_n: int = 5):
    """Get top-N product recommendations."""
    query = f"""
    SELECT
      RECOMM_pdm_prod_type_id,
      recomm_type,
      predicted_trans_count_confidence
    FROM `dataset.layer2_recommendations_{region}`
    WHERE FOCUS_pdm_prod_type_id = {product_id}
      AND rn <= {top_n}
    ORDER BY rn
    """
    return bq_client.query(query).result().to_dataframe()
```

## License

MIT License - see LICENSE file in project root.

## Support

For issues or questions:
- Check the main project README
- Open an issue on GitHub
- Review BigQuery ML documentation
