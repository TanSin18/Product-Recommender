from google.oauth2 import service_account
from google.cloud import bigquery
from google.cloud import bigquery_storage
from sklearn.calibration import CalibratedClassifierCV
import pandas as pd
import numpy as np
import pickle
from functools import partial
import warnings
from concurrent.futures import ProcessPoolExecutor
warnings.filterwarnings('ignore')
import grpc
import time
import pandas_gbq
from xgboost import XGBClassifier
from xgboost import plot_importance
from matplotlib import pyplot

key_path = '/home/jupyter/d00_key.json'
credentials = service_account.Credentials.from_service_account_file(
    key_path,
    scopes=["https://www.googleapis.com/auth/cloud-platform"],
)


def get_client(credentials):
    bq_client = bigquery.Client(credentials=credentials,
                                project=credentials.project_id)
    bqstorageclient = bigquery_storage.BigQueryReadClient(
        credentials=credentials)
    return bq_client, bqstorageclient
  
    

def data_upload(QUERY):
    key_path = '/home/jupyter/d00_key.json'
    credentials = service_account.Credentials.from_service_account_file(
        key_path,
        scopes=["https://www.googleapis.com/auth/cloud-platform"],
    )

    bq_client = bigquery.Client(credentials=credentials,
                                project=credentials.project_id)
    bqstorageclient = bigquery_storage.BigQueryReadClient(
        credentials=credentials)

    data = bq_client.query(QUERY).result().to_dataframe(
        bqstorage_client=bqstorageclient)

    return data


def model_predict(score,prod_type):
    print("Initiliazing Predictions for clustering Data")
    score = score.reset_index(drop=True)
    out = pd.DataFrame()
    df_out = pd.DataFrame()
    prob1 = pd.DataFrame()
    customers=score["customer_id"]
    x_data = score[[
        'BBB_INSTORE_M_DECILE_2Y', 'A_AAP000447N_ASET_PRPN_DIS_INC',
        'time_interval', 'PH_DM_RECENCY', 'AVG_NET_SALES_PER_TXN',
        'COUPON_SALES_Q_08', 'A_A3101N_RACE_WHITE', 'BBB_R_2Y', 'MOVER',
        'NUM_MERCH_DIVISIONS', 'AVG_TOTAL_ITEMS_PER_TXN', 'A_A8588N_HM_SQR_FT',
        'PH_MREDEEM730D_PERC'
    ]] 
    x_data = pd.to_numeric(x_data.stack(), errors='coerce').unstack()
    for col in x_data.columns:
        if 'DECILE' in col:
            x_data[col].fillna(11, inplace=True)
        elif '_R_2Y' in col and 'DECILE' not in col:
            x_data[col].fillna(720, inplace=True)
        elif 'RECENCY' in col and 'DECILE' not in col:
            x_data[col].fillna(365, inplace=True)
        else:
            x_data[col].fillna(0, inplace=True)
    
    
    for i in prod_type['pdm_prod_type_id']:
        z = pickle.load(open("cali_model_" + str(int(i)) + ".pkl", 'rb'))
        probn = z.predict_proba(x_data) 
        prob1=pd.DataFrame(data = probn[:, 1], columns = ['p'], index = x_data.index.copy())
        prob1["pdm_prod_type_id"]=int(i)
        df_out = pd.merge(customers,
                          pd.DataFrame(prob1),
                          how='left',
                          left_index=True,
                          right_index=True)
        out = pd.concat([out, df_out])
        pickle.dump(out, open("./prod_type_p.pkl", 'wb'))
    return out        

def cluster_data_format(predictions):
    predictions_wide=pd.pivot(predictions, index=['customer_id'], columns ='pdm_prod_type_id',values= 'p') 
    column_names=predictions_wide.columns.values.tolist()
    column_names_prep = ["p" + str(sub) for sub in column_names]
    predictions_wide.columns=column_names_prep
    predictions_wide['customer_id'] = predictions_wide.index
    predictions_wide = predictions_wide.reset_index(drop=True)
    return predictions_wide
    
    
    
def main():
    scoring_data_query = """SELECT * FROM `dw-bq-data-d00.SANDBOX_ANALYTICS.reengagement_product_recommendation_scoring`"""
    top99_prod_types_query = """SELECT * FROM `dw-bq-data-d00.SANDBOX_ANALYTICS.shopping_prod`"""
    
    scoring_data = data_upload(scoring_data_query)
    top99_prod_types = data_upload(top99_prod_types_query)
    top99_prod_types=top99_prod_types[1:]
    prediction_output = model_predict(scoring_data, top99_prod_types)
    clustering_data = cluster_data_format(prediction_output)
    output_name = "prod_type_cluster_data"
    clustering_data.to_gbq(destination_table=f'SANDBOX_ANALYTICS.scored_cluster_data',
                project_id=credentials.project_id,
                if_exists='replace',
                credentials=credentials)



if __name__ == "__main__":
    main()
