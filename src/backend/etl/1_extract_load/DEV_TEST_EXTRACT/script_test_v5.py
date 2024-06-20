import os
import time
import pandas as pd
import investpy as inv
import yfinance as yf
from google.cloud import bigquery
from google.oauth2 import service_account
from google.api_core.exceptions import NotFound, BadRequest, RefreshError

# Configuração da autenticação do GCP
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "credentials/credentials_private_key_gbq/fluent-outpost-424800-h1-81323e8da89e.json"
credentials_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")

# Define os caminhos para salvar os arquivos CSV
output_directory = os.path.join(os.getcwd(), 'src', 'backend', 'data', '1_raw')
raw_wallet_br_path = os.path.join(output_directory, 'raw_wallet_br.csv')
raw_historical_stock_price_br_path = os.path.join(output_directory, 'raw_historical_stock_price_br.csv')

def persist_to_bigquery_and_local(df, table_id, local_path, credentials_path):
    """Persiste o DataFrame no BigQuery e salva localmente."""
    try:
        credentials = service_account.Credentials.from_service_account_file(credentials_path)
        client = bigquery.Client(credentials=credentials, project=credentials.project_id)
        job = client.load_table_from_dataframe(df, table_id)
        job.result()  # Aguarde o término do job
        print(f"Dados persistidos na tabela {table_id}")
    except (NotFound, BadRequest, RefreshError) as e:
        print(f"Erro ao persistir dados no BigQuery: {e}")
    except Exception as e:
        print(f"Erro inesperado ao persistir dados: {e}")
    
    # Salvar localmente
    df.to_csv(local_path, index=False)
    print(f"Dados salvos localmente em {local_path}")

def get_brazil_stocks():
    """Obtém a lista de ações do Brasil e seleciona as colunas desejadas."""
    print("Obtendo lista de ações do Brasil...")
    try:
        start_time = time.time()
        br_stocks = inv.get_stocks(country='brazil')
        selected_columns_br = br_stocks[['country', 'name', 'full_name', 'symbol']]
        end_time = time.time()
        print(f"Tempo de execução: {end_time - start_time:.2f} segundos")
        return selected_columns_br
    except Exception as e:
        print(f"Erro ao obter lista de ações do Brasil: {e}")

def format_tickers(df):
    """Formata os tickers das ações para o padrão utilizado pelo Yahoo Finance."""
    return [ticker + '.SA' for ticker in df['symbol'].tolist()]

def create_wallet_df(df, tickers):
    """Cria o DataFrame wallet_br com colunas adicionais."""
    wallet_br = df.copy()
    wallet_br['ticker_br'] = tickers
    wallet_br['snome'] = wallet_br['symbol'] + '-' + wallet_br['name']
    return wallet_br

def get_stock_info(tickers):
    """Obtém informações de setor e indústria com tratamento de exceções."""
    print("Obtendo informações de setor e indústria das ações...")
    try:
        data = []
        start_time = time.time()
        for i, ticker in enumerate(tickers, 1):
            try:
                stock = yf.Ticker(ticker)
                info = stock.info
                data.append({
                    'ticker': ticker,
                    'sector': info.get('sector', 'N/A'),
                    'industry': info.get('industry', 'N/A')
                })
            except Exception as e:
                print(f"Erro ao obter informações para {ticker}: {e}")
                data.append({
                    'ticker': ticker,
                    'sector': 'N/A',
                    'industry': 'N/A'
                })
            print(f"Processo {i}/{len(tickers)} concluído. Tempo estimado restante: {(len(tickers)-i)*0.5:.2f} segundos")
            if i % 5 == 0:  # Reduz a frequência de espera para ser mais rápido
                time.sleep(0.5)
        end_time = time.time()
        print(f"Tempo total de execução: {end_time - start_time:.2f} segundos")
        return pd.DataFrame(data)
    except Exception as e:
        print(f"Erro ao obter informações das ações: {e}")

def merge_stock_info(wallet_df, stock_info_df):
    """Junta as informações de setor e indústria ao DataFrame original."""
    print("Juntando informações de setor e indústria ao DataFrame...")
    try:
        start_time = time.time()
        merged_df = wallet_df.merge(stock_info_df, left_on='ticker_br', right_on='ticker', how='left')
        merged_df.drop(columns=['ticker'], inplace=True)
        merged_df['research_cnpj'] = merged_df['full_name'] + ' - CNPJ'
        final_columns = ['country', 'name', 'full_name', 'symbol', 'ticker_br', 'snome', 'sector', 'industry', 'class_exchange', 'research_cnpj']
        merged_df = merged_df[final_columns]
        end_time = time.time()
        print(f"Tempo de execução: {end_time - start_time:.2f} segundos")
        return merged_df
    except Exception as e:
        print(f"Erro ao juntar informações: {e}")

def get_class_exchange(symbol):
    """
    Obtém a classe de câmbio com base no número no final do símbolo e nas siglas fornecidas na tabela de classes.
    Utiliza o número final do símbolo e as siglas para classificar.
    """
    try:
        class_exchange_map = {
            '3': 'Ações Ordinárias',
            '4': 'Ações Preferenciais',
            # Adicione aqui o restante das classes...
        }

        # Extrai o número final do símbolo
        num_final = symbol[-1]
        # Verifica se a sigla está no mapa de class_exchange
        if num_final in class_exchange_map:
            return class_exchange_map[num_final]
        else:
            return 'Unknown'
    except Exception as e:
        print(f"Erro ao obter classe de câmbio: {e}")
        return 'Unknown'

def main():
    try:
        # Obtém a lista de ações do Brasil
        selected_columns_br = get_brazil_stocks()

        # Formata os tickers das ações
        tickers = format_tickers(selected_columns_br)

        # Cria o DataFrame wallet_br com colunas adicionais
        wallet_br = create_wallet_df(selected_columns_br, tickers)

        # Persiste o DataFrame wallet_br no BigQuery e salva localmente
        persist_to_bigquery_and_local(wallet_br, 'atg_datta_solutions.raw_wallet_br', raw_wallet_br_path, credentials_path)

        # Obtém informações de setor e indústria
        stock_info_df = get_stock_info(tickers)

                # Junta as informações de setor e indústria ao DataFrame original
        merged_df = merge_stock_info(wallet_br, stock_info_df)

        # Cria a coluna class_exchange
        merged_df['class_exchange'] = merged_df['symbol'].apply(get_class_exchange)

        # Reordena a coluna research_cnpj por último
        columns_order = ['country', 'name', 'full_name', 'symbol', 'ticker_br', 'snome', 'sector', 'industry', 'class_exchange', 'research_cnpj']
        merged_df = merged_df.reindex(columns=columns_order)

        # Persiste o DataFrame no BigQuery e salva localmente
        persist_to_bigquery_and_local(merged_df, 'atg_datta_solutions.raw_historical_stock_price_br', raw_historical_stock_price_br_path, credentials_path)
    except Exception as e:
        print(f"Erro inesperado durante a execução do script: {e}")

if __name__ == "__main__":
    main()
