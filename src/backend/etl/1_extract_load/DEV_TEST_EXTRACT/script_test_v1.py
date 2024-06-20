import os
import time
import pandas as pd
import investpy as inv
import yfinance as yf
from google.cloud import bigquery
from google.oauth2 import service_account
from google.api_core.exceptions import NotFound

# Configuração da autenticação do GCP
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "docs/credentials_private_key_gbq/fluent-outpost-424800-h1-81323e8da89e.json"
credentials_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")

# Define os caminhos para salvar os arquivos CSV
output_directory = os.path.join(os.getcwd(), 'src', 'backend', 'data', '1_raw')
wallet_br_path = os.path.join(output_directory, 'wallet_br.csv')
historical_stock_price_br_path = os.path.join(output_directory, 'historical_stock_price_br.csv')

def persist_to_bigquery(df, table_id, credentials_path):
    """Persiste o DataFrame no BigQuery."""
    credentials = service_account.Credentials.from_service_account_file(credentials_path)
    client = bigquery.Client(credentials=credentials, project=credentials.project_id)
    try:
        job = client.load_table_from_dataframe(df, table_id)
        job.result()  # Aguarde o término do job
        print(f"Dados persistidos na tabela {table_id}")
    except NotFound as e:
        print(f"Tabela não encontrada: {e}")
    except Exception as e:
        print(f"Erro ao persistir dados no BigQuery: {e}")

def get_brazil_stocks():
    """Obtém a lista de ações do Brasil e seleciona as colunas desejadas."""
    print("Obtendo lista de ações do Brasil...")
    start_time = time.time()
    br_stocks = inv.get_stocks(country='brazil')
    selected_columns_br = br_stocks[['country', 'name', 'full_name', 'symbol']]
    end_time = time.time()
    print(f"Tempo de execução: {end_time - start_time:.2f} segundos")
    return selected_columns_br

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

def merge_stock_info(wallet_df, stock_info_df):
    """Junta as informações de setor e indústria ao DataFrame original."""
    print("Juntando informações de setor e indústria ao DataFrame...")
    start_time = time.time()
    merged_df = wallet_df.merge(stock_info_df, left_on='ticker_br', right_on='ticker', how='left')
    merged_df.drop(columns=['ticker'], inplace=True)
    merged_df['research_cnpj'] = merged_df['full_name'] + ' - CNPJ'
    final_columns = ['country', 'name', 'full_name', 'symbol', 'ticker_br', 'snome', 'sector', 'industry', 'research_cnpj']
    end_time = time.time()
    print(f"Tempo de execução: {end_time - start_time:.2f} segundos")
    return merged_df[final_columns]

def get_historical_data(ticker):
    """Obtém as cotações históricas dos últimos 6 meses."""
    print(f"Obtendo cotações históricas para {ticker}...")
    try:
        stock = yf.Ticker(ticker)
        hist = stock.history(period='6mo')
        if hist.empty:
            print(f"{ticker}: No data found, symbol may be delisted")
            return pd.DataFrame()
        hist['ticker'] = ticker
        return hist
    except Exception as e:
        print(f"Erro ao obter cotações para {ticker}: {e}")
        return pd.DataFrame()

def get_all_historical_data(tickers):
    """Obtém as cotações históricas para todos os tickers e as combina em um único DataFrame."""
    print("Obtendo cotações históricas para todas as ações...")
    start_time = time.time()
    all_stocks_hist = pd.concat([get_historical_data(ticker) for ticker in tickers], axis=0)
    all_stocks_hist.reset_index(inplace=True)
    end_time = time.time()
    print(f"Tempo total de execução: {end_time - start_time:.2f} segundos")
    return all_stocks_hist[['Date', 'ticker', 'Open', 'High', 'Low', 'Close', 'Volume']]

def main():
    # Verifica se o diretório de output existe e, se não, cria
    if not os.path.exists(output_directory):
        os.makedirs(output_directory)

    if os.path.exists(wallet_br_path) and os.path.exists(historical_stock_price_br_path):
        print("Arquivos CSV já existem. Carregando os dados...")
        wallet_br = pd.read_csv(wallet_br_path)
        historical_stock_price_br = pd.read_csv(historical_stock_price_br_path)
    else:
        # Passo 1: Extração
        start_time = time.time()
        br_stocks = get_brazil_stocks()
        ticker_br = format_tickers(br_stocks)
        wallet_br = create_wallet_df(br_stocks, ticker_br)
        stock_info = get_stock_info(wallet_br['ticker_br'])
        wallet_br = merge_stock_info(wallet_br, stock_info)
        historical_stock_price_br = get_all_historical_data(wallet_br['ticker_br'])
        end_time = time.time()
        print(f"Tempo total de extração: {end_time - start_time:.2f} segundos")

        # Passo 3: Salvamento dos arquivos CSV
        start_time = time.time()
        wallet_br.to_csv(wallet_br_path, index=False)
        historical_stock_price_br.to_csv(historical_stock_price_br_path, index=False)
        end_time = time.time()
        print(f"Tempo total de salvamento: {end_time - start_time:.2f} segundos")

    # Passo 2: Persistência na camada raw (bronze)
    start_time = time.time()
    persist_to_bigquery(wallet_br, 'fluent-outpost-424800-h1.1_raw_Neoway_Capital_Market_Analytics.raw_wallet_br', credentials_path)
    persist_to_bigquery(historical_stock_price_br, 'fluent-outpost-424800-h1.1_raw_Neoway_Capital_Market_Analytics.raw_historical_stock_price_br', credentials_path)
    end_time = time.time()
    print(f"Tempo total de persistência: {end_time - start_time:.2f} segundos")

    # Mensagens de confirmação
    print(f"wallet_br saved to {wallet_br_path}")
    print(f"historical_stock_price_br saved to {historical_stock_price_br_path}")

    print("Processo concluído!")

if __name__ == "__main__":
    main()
