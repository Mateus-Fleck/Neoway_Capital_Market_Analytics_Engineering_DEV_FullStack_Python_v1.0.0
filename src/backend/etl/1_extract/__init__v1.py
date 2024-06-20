import os
import time
import pandas as pd
import investpy as inv
import yfinance as yf
from google.cloud import bigquery
from google.oauth2 import service_account
from google.api_core.exceptions import NotFound
from multiprocessing import Pool
from tqdm import tqdm

# Configuração da autenticação do GCP
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "credentials/credentials_private_key_gbq/GBQ.json"
credentials_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")

# Define os caminhos para salvar os arquivos CSV
output_directory = os.path.join(os.getcwd(), 'src', 'backend', 'data', '1_raw')
wallet_br_path = os.path.join(output_directory, 'raw_wallet_br.csv')
address_path = os.path.join(output_directory, 'raw_address_company_br.csv')
historical_stock_price_br_path = os.path.join(output_directory, 'raw_historical_stock_price_br.csv')


def persist_to_bigquery(df, table_id, credentials_path):
    """Persiste o DataFrame no BigQuery."""
    credentials = service_account.Credentials.from_service_account_file(credentials_path)
    client = bigquery.Client(credentials=credentials, project=credentials.project_id)
    
    # Remover ou renomear a coluna 'date' se ela existir no DataFrame
    if 'date' in df.columns:
        df = df.rename(columns={'date': 'record_date'})
    
    try:
        job = client.load_table_from_dataframe(df, table_id)
        job.result()  # Aguarde o término do job
        print(f"Dados persistidos na tabela {table_id}")
    except NotFound as e:
        print(f"Tabela não encontrada: {e}")
    except Exception as e:
        print(f"Erro ao persistir dados no BigQuery: {e}")

def save_to_local(df, local_path):
    """Salva o DataFrame localmente."""
    df.to_csv(local_path, index=False)
    print(f"Dados salvos localmente em {local_path}")

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

def get_stock_info_parallel(ticker):
    """Obtém informações de setor e indústria com tratamento de exceções para um ticker."""
    try:
        stock = yf.Ticker(ticker)
        info = stock.info
        return {
            'ticker': ticker,
            'sector': info.get('sector', 'N/A'),
            'industry': info.get('industry', 'N/A'),
            'longBusinessSummary': info.get('longBusinessSummary', 'N/A'),
            'address': info.get('address1', '') + ' ' + info.get('address2', ''),
            'city': info.get('city', 'N/A'),
            'state': info.get('state', 'N/A'),
            'zip': info.get('zip', 'N/A'),
            'country': info.get('country', 'N/A'),
            'website': info.get('website', 'N/A')
        }
    except Exception as e:
        print(f"Erro ao obter informações para {ticker}: {e}")
        return {
            'ticker': ticker,
            'sector': 'N/A',
            'industry': 'N/A',
            'longBusinessSummary': 'N/A',
            'address': 'N/A',
            'city': 'N/A',
            'state': 'N/A',
            'zip': 'N/A',
            'country': 'N/A',
            'website': 'N/A'
        }

def get_stock_info_parallel_wrapper(ticker):
    """Wrapper para a função get_stock_info_parallel para lidar com exceções."""
    try:
        return get_stock_info_parallel(ticker)
    except Exception as e:
        print(f"Erro ao processar {ticker}: {e}")

def get_stock_info_parallelized(tickers):
    """Obtém informações de setor e indústria para todos os tickers em paralelo."""
    print("Obtendo informações de setor e indústria das ações em paralelo...")
    start_time = time.time()
    total_processes = len(tickers)
    with Pool() as pool:
        data = []
        for i, result in enumerate(pool.imap_unordered(get_stock_info_parallel_wrapper, tickers), 1):
            data.append(result)
            remaining_time = (total_processes - i) * (time.time() - start_time) / i
            print(f"Processo {i}/{total_processes} concluído. Tempo estimado restante: {remaining_time:.2f} segundos")
    end_time = time.time()
    print(f"Tempo total de execução: {end_time - start_time:.2f} segundos")
    return pd.DataFrame(data)

def merge_stock_info(wallet_df, stock_info_df):
    """Junta as informações de setor e indústria ao DataFrame original."""
    print("Juntando informações de setor e indústria ao DataFrame...")
    start_time = time.time()
    merged_df = wallet_df.merge(stock_info_df, left_on='ticker_br', right_on='ticker', how='left')
    merged_df.drop(columns=['ticker'], inplace=True)
    
    # Adiciona a coluna 'country' ao DataFrame original para evitar o erro
    merged_df['country'] = 'Brazil'
    
    merged_df['research_cnpj'] = merged_df['full_name'] + ' - CNPJ'
    
    # Limpar as colunas name e full_name
    merged_df['name'] = merged_df['name'].str.replace(r'\W+', '')
    merged_df['full_name'] = merged_df['full_name'].str.replace(r'\W+', '')
    
    merged_df['class_exchange'] = merged_df['symbol'].str.extract(r'(\d+)$').replace({
        '3': 'Ações Ordinárias',
        '4': 'Ações Preferenciais',
        '5': 'Ações Preferenciais Classe A',
        '6': 'Ações Preferenciais Classe B',
        '7': 'Ações Preferenciais Classe C',
        '8': 'Ações Preferenciais Classe D',
        '11': 'Units (Pacote de valores mobiliários)',
        '12': 'Ações Preferenciais Classe E',
        '13': 'Ações Preferenciais Classe F',
        '31': 'Ações Ordinárias Resgatáveis',
        '32': 'Ações Preferenciais Resgatáveis',
        '33': 'Ações Preferenciais Classe A Resgatáveis',
        '34': 'Ações Preferenciais Classe B Resgatáveis',
        '35': 'Ações Preferenciais Classe C Resgatáveis',
        '36': 'Ações Preferenciais Classe D Resgatáveis',
        '39': 'Ações Preferenciais de Dividendos Prioritários Resgatáveis',
        '41': 'Ações Ordinárias Não Conversíveis',
        '42': 'Ações Preferenciais Não Conversíveis',
        '43': 'Ações Preferenciais Classe A Não Conversíveis',
        '44': 'Ações Preferenciais Classe B Não Conversíveis',
        '45': 'Ações Preferenciais Classe C Não Conversíveis',
        '46': 'Ações Preferenciais Classe D Não Conversíveis',
        '49': 'Ações Preferenciais de Dividendos Prioritários Não Conversíveis',
        '50': 'Ações Ordinárias com Direitos Diferenciados',
        '51': 'Ações Preferenciais com Direitos Diferenciados',
        '52': 'Ações Preferenciais Classe A com Direitos Diferenciados',
        '53': 'Ações Preferenciais Classe B com Direitos Diferenciados',
        '54': 'Ações Preferenciais Classe C com Direitos Diferenciados',
        '55': 'Ações Preferenciais Classe D com Direitos Diferenciados',
        '56': 'Ações Preferenciais Diferenciadas de Dividendos Prioritários'
    })
    
    # Reorganizar as colunas
    final_columns = ['country', 'name', 'full_name', 'symbol', 'ticker_br', 'snome', 'sector', 'industry', 'class_exchange', 'research_cnpj']
    merged_df = merged_df[final_columns]
    
    end_time = time.time()
    print(f"Tempo de execução: {end_time - start_time:.2f} segundos")
    return merged_df

def get_historical_data_parallel(ticker):
    """Obtém as cotações históricas dos últimos 6 meses."""
    print(f"Obtendo cotações históricas para {ticker}...")
    try:
        end_date = pd.Timestamp.today()
        start_date = end_date - pd.DateOffset(months=6)
        df = yf.download(ticker, start=start_date, end=end_date)
        df['ticker'] = ticker
        df.reset_index(inplace=True)
        return df[['Date', 'Open', 'High', 'Low', 'Close', 'Volume', 'ticker']]
    except Exception as e:
        print(f"Erro ao obter cotações para {ticker}: {e}")
        return pd.DataFrame()

def get_historical_data_parallelized(tickers):
    """Obtém as cotações históricas para todos os tickers em paralelo."""
    print("Obtendo cotações históricas em paralelo...")
    start_time = time.time()
    with Pool() as pool:
        historical_data = list(tqdm(pool.imap_unordered(get_historical_data_parallel, tickers), total=len(tickers), desc="Progresso"))
    combined_data = pd.concat(historical_data, ignore_index=True)
    end_time = time.time()
    print(f"Tempo total de execução: {end_time - start_time:.2f} segundos")
    return combined_data

def process_data():
    """Executa todo o pipeline de dados."""
    selected_columns_br = get_brazil_stocks()
    save_to_local(selected_columns_br, wallet_br_path)
    
    tickers_br = format_tickers(selected_columns_br)
    wallet_br_df = create_wallet_df(selected_columns_br, tickers_br)
    
    stock_info_df = get_stock_info_parallelized(tickers_br)
    address_df = stock_info_df[['address', 'city', 'state', 'zip', 'country', 'website']]
    
    # Adicionando uma coluna 'ticker' no início do DataFrame address
    address_df.insert(0, 'ticker', tickers_br)
    
    save_to_local(address_df, address_path)
    
    final_df = merge_stock_info(wallet_br_df, stock_info_df)
    save_to_local(final_df, wallet_br_path)
    
    historical_data_df = get_historical_data_parallelized(tickers_br)
    save_to_local(historical_data_df, historical_stock_price_br_path)
    
    # Persistir os dados no BigQuery
    persist_to_bigquery(wallet_br_df, 'fluent-outpost-424800-h1.1_raw_Neoway_Capital_Market_Analytics.raw_wallet_br', credentials_path)
    persist_to_bigquery(address_df, 'fluent-outpost-424800-h1.1_raw_Neoway_Capital_Market_Analytics.raw_address_company_br', credentials_path)
    persist_to_bigquery(historical_data_df, 'fluent-outpost-424800-h1.1_raw_Neoway_Capital_Market_Analytics.raw_historical_stock_price_br', credentials_path)

if __name__ == "__main__":
    process_data()
