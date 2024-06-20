import os
import time
import pandas as pd
import investpy as inv
import yfinance as yf
from google.cloud import bigquery
from google.oauth2 import service_account
from google.api_core.exceptions import NotFound

# Configuração da autenticação do GCP
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "credentials/credentials_private_key_gbq/GBQ.json"
credentials_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")

# Define os caminhos para salvar os arquivos CSV
output_directory = os.path.join(os.getcwd(), 'src', 'backend', 'data', '1_raw')
raw_wallet_br_path = os.path.join(output_directory, 'raw_wallet_br.csv')
raw_historical_stock_price_br_path = os.path.join(output_directory, 'raw_historical_stock_price_br.csv')

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
    final_columns = ['country', 'name', 'full_name', 'symbol', 'ticker_br', 'snome', 'sector', 'industry', 'class_exchange', 'research_cnpj']
    merged_df = merged_df[final_columns]
    end_time = time.time()
    print(f"Tempo de execução: {end_time - start_time:.2f} segundos")
    return merged_df

def get_class_exchange(symbol):
    """
    Obtém a classe de câmbio com base no número no final do símbolo e nas siglas fornecidas na tabela de classes.
    Utiliza o número final do símbolo e as siglas para classificar.
    """
    class_exchange_map = {
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
    }

    # Extrai o número final do símbolo
    num_final = symbol[-1]
    # Verifica se a sigla está no mapa de class_exchange
    if num_final in class_exchange_map:
        return class_exchange_map[num_final]
    else:
        return 'Unknown'

def main():
    # Obtém a lista de ações do Brasil
    selected_columns_br = get_brazil_stocks()

    # Formata os tickers das ações
    tickers = format_tickers(selected_columns_br)

    # Cria o DataFrame wallet_br com colunas adicionais
    wallet_br = create_wallet_df(selected_columns_br, tickers)

    # Persiste o DataFrame wallet_br no BigQuery
    persist_to_bigquery(wallet_br, 'fluent-outpost-424800-h1.1_raw_Neoway_Capital_Market_Analytics.raw_wallet_br', credentials_path)

    # Obtém informações de setor e indústria
    stock_info_df = get_stock_info(tickers)

    # Junta as informações de setor e indústria ao DataFrame original
    merged_df = merge_stock_info(wallet_br, stock_info_df)

    # Cria a coluna class_exchange
    merged_df['class_exchange'] = merged_df['symbol'].apply(get_class_exchange)

    # Reordena a coluna research_cnpj por último
    columns_order = ['country', 'name', 'full_name', 'symbol', 'ticker_br', 'snome', 'sector', 'industry', 'class_exchange', 'research_cnpj']
    merged_df = merged_df.reindex(columns=columns_order)

    # Persiste o DataFrame no BigQuery
    persist_to_bigquery(merged_df, 'fluent-outpost-424800-h1.1_raw_Neoway_Capital_Market_Analytics.raw_historical_stock_price_br', credentials_path)

if __name__ == "__main__":
    main()
