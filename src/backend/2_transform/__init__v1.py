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
    try:
        return client.query(query).to_dataframe()
    except Exception as e:
        print(f"Erro ao carregar dados do BigQuery: {e}")
        return pd.DataFrame()

def persist_to_bigquery(df, table_id, credentials_path):
    """Persiste o DataFrame no BigQuery."""
    credentials = service_account.Credentials.from_service_account_file(credentials_path)
    client = bigquery.Client(credentials=credentials, project=credentials.project_id)
    try:
        job = client.load_table_from_dataframe(df, table_id)
        job.result()  # Aguarde o término do job
        print(f"Dados persistidos na tabela {table_id}")
    except Exception as e:
        print(f"Erro ao persistir dados no BigQuery: {e}")

def translate_column_names(df, translation_dict):
    """Traduz os nomes das colunas de acordo com o dicionário fornecido."""
    df.rename(columns=translation_dict, inplace=True)
    return df

def transform_wallet_br(df):
    """Aplica transformações no DataFrame wallet_br."""
    try:
        # Remover linhas duplicadas e limpar colunas
        df.drop_duplicates(subset=['ticker_br'], inplace=True)
        df.fillna('Desconhecido', inplace=True)
        # Traduzir nomes das colunas para português
        translation_dict = {
            'country': 'pais',
            'name': 'nome',
            'full_name': 'nome_completo',
            'symbol': 'simbolo',
            'ticker_br': 'ticker_br',
            'snome': 'snome',
            'sector': 'setor',
            'industry': 'industria',
            'research_cnpj': 'pesquisa_cnpj',
            'class_exchange': 'classe_listagem'
        }
        df = translate_column_names(df, translation_dict)
        return df
    except Exception as e:
        print(f"Erro ao transformar wallet_br: {e}")
        return pd.DataFrame()

def transform_historical_stock_price_br(df):
    """Aplica transformações no DataFrame historical_stock_price_br."""
    try:
        # Verifica se as colunas esperadas estão presentes
        required_columns = ['Date', 'ticker', 'Open', 'High', 'Low', 'Close', 'Volume']
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            raise KeyError(f"Colunas ausentes no DataFrame: {', '.join(missing_columns)}")

        # Remover linhas com dados ausentes e converter tipos de dados
        df.dropna(inplace=True)
        df['Date'] = pd.to_datetime(df['Date'])
        df['Volume'] = df['Volume'].astype(int)
        # Traduzir nomes das colunas para português
        translation_dict = {
            'Date': 'data',
            'ticker': 'ticker',
            'Open': 'abertura',
            'High': 'máxima',
            'Low': 'mínima',
            'Close': 'fechamento',
            'Volume': 'volume'
        }
        df = translate_column_names(df, translation_dict)
        return df
    except KeyError as e:
        print(f"Erro de chave: {e}")
        return pd.DataFrame()
    except Exception as e:
        print(f"Erro ao transformar historical_stock_price_br: {e}")
        return pd.DataFrame()

def save_to_local(dataframe, file_path):
    """Salva o DataFrame em um arquivo CSV local."""
    dataframe.to_csv(file_path, index=False)
    print(f"Dados salvos em {file_path}")

def main():
    start_time = time.time()
    
    # Queries para carregar dados da camada raw
    query_wallet_br = """
    SELECT * FROM `fluent-outpost-424800-h1.1_raw_Neoway_Capital_Market_Analytics.raw_wallet_br`
    """
    query_historical_stock_price_br = """
    SELECT * FROM `fluent-outpost-424800-h1.1_raw_Neoway_Capital_Market_Analytics.raw_historical_stock_price_br`
    """

    # Carrega dados da camada raw
    print("Carregando dados da camada raw...")
    wallet_br = load_from_bigquery(query_wallet_br, credentials_path)
    historical_stock_price_br = load_from_bigquery(query_historical_stock_price_br, credentials_path)

    # Exibe as colunas dos DataFrames carregados para verificação
    print(f"Colunas de wallet_br: {wallet_br.columns.tolist()}")
    print(f"Colunas de historical_stock_price_br: {historical_stock_price_br.columns.tolist()}")

    # Aplica transformações nos dados
    print("Transformando dados...")
    silver_wallet_br = transform_wallet_br(wallet_br)
    silver_historical_stock_price_br = transform_historical_stock_price_br(historical_stock_price_br)

    # Verifica se as transformações retornaram dados válidos
    if not silver_wallet_br.empty:
        persist_to_bigquery(silver_wallet_br, 'fluent-outpost-424800-h1.2_silver_Neoway_Capital_Market_Analytics.silver_wallet_br', credentials_path)
        save_to_local(silver_wallet_br, "src/backend/data/2_silver/silver_wallet_br.csv")
    else:
        print("Dados de silver_wallet_br estão vazios. Não foram persistidos.")

    if not silver_historical_stock_price_br.empty:
        persist_to_bigquery(silver_historical_stock_price_br, 'fluent-outpost-424800-h1.2_silver_Neoway_Capital_Market_Analytics.silver_historical_stock_price_br', credentials_path)
        save_to_local(silver_historical_stock_price_br, "src/backend/data/2_silver/silver_historical_stock_price_br.csv")
    else:
        print("Dados de silver_historical_stock_price_br estão vazios. Não foram persistidos.")

    end_time = time.time()
    elapsed_time = end_time - start_time
    print(f"Processo de transformação concluído em {elapsed_time:.2f} segundos!")

if __name__ == "__main__":
    main()
