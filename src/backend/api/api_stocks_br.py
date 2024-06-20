import os
from fastapi import FastAPI, HTTPException
from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd
from typing import List, Dict

app = FastAPI()

# Configuração da autenticação do GCP
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "credentials/credentials_private_key_gbq/GBQ.json"
credentials_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")

def load_from_bigquery(query: str, credentials_path: str) -> pd.DataFrame:
    """Carrega dados do BigQuery usando uma consulta SQL."""
    credentials = service_account.Credentials.from_service_account_file(credentials_path)
    client = bigquery.Client(credentials=credentials, project=credentials.project_id)
    return client.query(query).to_dataframe()

@app.get("/")
def read_root():
    return {"message": "Welcome to the Neoway Capital Market Analytics API"}

@app.get("/silver_wallet_br", response_model=List[Dict])
def get_silver_wallet_br():
    try:
        query = """
        SELECT * FROM `fluent-outpost-424800-h1.2_silver_Neoway_Capital_Market_Analytics.silver_wallet_br`
        """
        df = load_from_bigquery(query, credentials_path)
        return df.to_dict(orient="records")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/silver_historical_stock_price_br", response_model=List[Dict])
def get_silver_historical_stock_price_br():
    try:
        query = """
        SELECT * FROM `fluent-outpost-424800-h1.2_silver_Neoway_Capital_Market_Analytics.silver_historical_stock_price_br`
        """
        df = load_from_bigquery(query, credentials_path)
        return df.to_dict(orient="records")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/silver_address_company_br", response_model=List[Dict])
def get_silver_address_company_br():
    try:
        query = """
        SELECT * FROM `fluent-outpost-424800-h1.2_silver_Neoway_Capital_Market_Analytics.silver_address_company_br`
        """
        df = load_from_bigquery(query, credentials_path)
        return df.to_dict(orient="records")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
