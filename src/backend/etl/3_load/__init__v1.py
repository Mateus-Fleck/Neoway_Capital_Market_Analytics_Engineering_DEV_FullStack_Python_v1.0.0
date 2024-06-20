import os
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
import time

# Configuração da autenticação do GCP
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "credentials/credentials_private_key_gbq/GBQ.json"
credentials_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")

def load_from_bigquery(query, credentials_path):
    """Carrega dados do BigQuery usando uma consulta SQL."""
    credentials = service_account.Credentials.from_service_account_file(credentials_path)
    client = bigquery.Client(credentials=credentials, project=credentials.project_id)
    start_time = time.time()
    dataframe = client.query(query).to_dataframe()
    end_time = time.time()
    elapsed_time = end_time - start_time
    print(f"Tempo de carregamento dos dados: {elapsed_time:.2f} segundos")
    return dataframe

def persist_to_bigquery(df, table_id, credentials_path):
    """Persiste o DataFrame no BigQuery."""
    credentials = service_account.Credentials.from_service_account_file(credentials_path)
    client = bigquery.Client(credentials=credentials, project=credentials.project_id)
    start_time = time.time()
    job = client.load_table_from_dataframe(df, table_id)
    job.result()  # Aguarde o término do job
    end_time = time.time()
    elapsed_time = end_time - start_time
    print(f"Dados persistidos na tabela {table_id}")
    print(f"Tempo de persistência dos dados: {elapsed_time:.2f} segundos")

def transform_to_gold_wallet(df):
    """Aplica transformações finais para a tabela de dimensões (dim_wallet_br)."""
    # Exemplo de transformação adicional, se necessário
    df['pais'] = df['pais'].str.upper()  # Converte o nome do país para maiúsculas
    return df

def transform_to_gold_historical(df):
    """Aplica transformações finais para a tabela de fatos (fact_historical_stock_price_br)."""
    # Exemplo de transformação adicional, se necessário
    df['data'] = pd.to_datetime(df['data'])  # Garante que a coluna data esteja no formato datetime
    return df

def main():
    # Queries para carregar dados da camada silver
    query_silver_wallet_br = """
    SELECT * FROM `fluent-outpost-424800-h1.2_silver_Neoway_Capital_Market_Analytics.silver_wallet_br`
    """
    query_silver_historical_stock_price_br = """
    SELECT * FROM `fluent-outpost-424800-h1.2_silver_Neoway_Capital_Market_Analytics.silver_historical_stock_price_br`
    """

    # Carrega dados da camada silver
    print("Carregando dados da camada silver...")
    silver_wallet_br = load_from_bigquery(query_silver_wallet_br, credentials_path)
    silver_historical_stock_price_br = load_from_bigquery(query_silver_historical_stock_price_br, credentials_path)

    # Exibe as colunas dos DataFrames carregados para verificação
    print(f"Colunas de silver_wallet_br: {silver_wallet_br.columns.tolist()}")
    print(f"Colunas de silver_historical_stock_price_br: {silver_historical_stock_price_br.columns.tolist()}")

    # Aplica transformações finais nos dados para a camada gold
    print("Transformando dados para a camada gold...")
    gold_wallet_br = transform_to_gold_wallet(silver_wallet_br)
    gold_historical_stock_price_br = transform_to_gold_historical(silver_historical_stock_price_br)

    # Salva dados transformados na camada gold com novos nomes de tabelas
    print("Persistindo dados na camada gold...")
    persist_to_bigquery(gold_wallet_br, 'fluent-outpost-424800-h1.3_gold_Neoway_Capital_Market_Analytics.gold_dim_wallet_br', credentials_path)
    persist_to_bigquery(gold_historical_stock_price_br, 'fluent-outpost-424800-h1.3_gold_Neoway_Capital_Market_Analytics.gold_fact_historical_stock_price_br', credentials_path)

    # Salvando na camada Gold local
    gold_wallet_br.to_csv("src/backend/data/3_gold/gold_dim_wallet_br.csv", index=False)
    gold_historical_stock_price_br.to_csv("src/backend/data/3_gold/gold_fact_historical_stock_price_br.csv", index=False)

    print("Processo de transformação para a camada gold concluído!")

if __name__ == "__main__":
    main()
