import pandas as pd
import os
from googlesearch import search
from bs4 import BeautifulSoup
import requests
import re
from datetime import datetime
import time

def buscar_cnpj_google(nome_empresa):
    try:
        # Realiza a busca no Google pelo nome da empresa seguido de "CNPJ"
        query = f'{nome_empresa} CNPJ'
        search_results = search(query, num_results=1, lang='pt')  # Limita a 1 resultado
        
        # Extrai o link do primeiro resultado da busca
        first_result_link = next(search_results)
        
        # Envia uma solicitação GET para o link
        response = requests.get(first_result_link)
        
        # Verifica se a solicitação foi bem-sucedida
        if response.status_code == 200:
            # Parseia o conteúdo HTML
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Encontra o texto que contém o CNPJ
            cnpj_text = soup.get_text()
            
            # Utiliza expressão regular para encontrar o CNPJ no texto
            cnpj_matches = re.findall(r'\b\d{2}\.\d{3}\.\d{3}\/\d{4}-\d{2}\b', cnpj_text)
            
            # Retorna o primeiro CNPJ encontrado
            if cnpj_matches:
                return cnpj_matches[0], True
            else:
                print(f'CNPJ não encontrado para {nome_empresa}')
                return None, False
        elif response.status_code == 429:
            # Se receber erro 429, aguarde um tempo e tente novamente
            print(f'Erro 429: Muitas solicitações. Aguardando e tentando novamente...')
            time.sleep(10)  # Espera 10 segundos antes de tentar novamente
            return buscar_cnpj_google(nome_empresa)
        else:
            print(f'Erro ao buscar CNPJ para {nome_empresa}. Status code: {response.status_code}')
            return None, False
    except Exception as e:
        print(f'Erro ao buscar CNPJ para {nome_empresa}: {str(e)}')
        return None, False

# Caminhos dos arquivos de entrada e saída
input_csv_path = 'C:/Neoway/Neoway_Capital_Market_Analytics_Engineering_DEV_FullStack_Python_v1.0.0/src/backend/etl/1_extract/BOT_CNPJ/data/INPUT/research_cnpj.csv'
output_dir = 'C:/Neoway/Neoway_Capital_Market_Analytics_Engineering_DEV_FullStack_Python_v1.0.0/src/backend/etl/1_extract/BOT_CNPJ/data/OUTPUT'
data_atual = datetime.now().strftime("%Y%m%d")
output_csv_path = os.path.join(output_dir, f'Output_{data_atual}_Busca_CNPJ.csv')

# Certificar-se de que o diretório de saída existe
os.makedirs(output_dir, exist_ok=True)

# Verificar se o arquivo CSV de entrada existe
if not os.path.isfile(input_csv_path):
    raise FileNotFoundError(f'O arquivo {input_csv_path} não foi encontrado. Verifique o caminho.')

# Ler o arquivo CSV original
df = pd.read_csv(input_csv_path, header=None, names=["empresa_cnpj"])

# Lista para armazenar os resultados
resultados = []

# Contador de CNPJs encontrados e não encontrados
cnpjs_encontrados = 0
cnpjs_nao_encontrados = 0

# Iterar sobre cada empresa no CSV
for i, row in df.iterrows():
    nome_empresa = row['empresa_cnpj'].replace(' - CNPJ', '')  # Remove o sufixo " - CNPJ"
    cnpj, encontrado = buscar_cnpj_google(nome_empresa)
    if cnpj:
        print(f'{i + 1}/{len(df)}: CNPJ encontrado para {nome_empresa}: {cnpj}')
        resultados.append({"Empresa": nome_empresa, "CNPJ": cnpj, "Status": "Encontrado"})
        cnpjs_encontrados += 1
    else:
        print(f'{i + 1}/{len(df)}: Não foi possível encontrar o CNPJ para {nome_empresa}')
        resultados.append({"Empresa": nome_empresa, "CNPJ": "", "Status": "Não Encontrado"})
        cnpjs_nao_encontrados += 1

# Salvar os resultados em um novo arquivo CSV
df_resultados = pd.DataFrame(resultados)
df_resultados.to_csv(output_csv_path, index=False)

print(f'Arquivo de saída salvo em: {output_csv_path}')
print(f'Total de CNPJs encontrados: {cnpjs_encontrados}/{len(df)}')
print(f'Total de CNPJs não encontrados: {cnpjs_nao_encontrados}/{len(df)}')
