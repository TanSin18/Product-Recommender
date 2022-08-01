# Lapsed-Atrisk-Product-Recommender

## Background

- An ML driven product recommendation solution designed to help reactivate the lapsed and at-risk customers by offering an ensemble of personalized product recommendations based on the probabilities of the customers to convert on the top selling product types which reactivated the lapsed customers in the past.

- Lapsed and Reactivated customer definition:
1. Lapsed Customers:  Inactive since 12+ Months.
2. At-Risk Customers: Inactive between 6-12 Months.

Due to the long absence of activity in any form from such customers we cannot apply legacy solutions like Matrix Factorization as they need focus products on which the other products are recommended. To tackle this situation, 
we used the following 
## Approach:
### For Email Marketing:
- Identify the top 99 Product Types which reactivates the lapsed & At-Risk customers in the past.
- Train 99 models (XG-Boost) for each product types purchased by the reactivated customers to predict their likelihood to be purchased by the lapsed and At-Risk customers.
- Recommend top 4 high probability product type.
### For DM Marketing:
- Identify the top 99 Product Types which reactivates the lapsed & At-Risk customers in the past.
- Train 99 models (XG-Boost) for each product types purchased by the reactivated customers to predict their likelihood to be purchased by the lapsed and At-Risk customers.
- Using the predictions of the reactivated customers for 99 Product types as attributes, use unsupervised learning algorithm (K-Means) to create clusters of the customers inclined to buy similar group of product types.


