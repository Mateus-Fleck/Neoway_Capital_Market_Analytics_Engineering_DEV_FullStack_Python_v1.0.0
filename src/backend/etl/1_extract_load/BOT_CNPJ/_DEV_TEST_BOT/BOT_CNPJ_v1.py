import os
import csv
from bs4 import BeautifulSoup
import requests
from datetime import datetime
import pandas as pd
from googlesearch import search

def buscar_cnpj_google(nome_empresa):
    try:
        # Realiza a busca no Google pelo nome da empresa
        query = nome_empresa
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

# Caminho para o arquivo de entrada
input_csv_path = 'C:/Neoway/Neoway_Capital_Market_Analytics_Engineering_DEV_FullStack_Python_v1.0.0/src/backend/data/1_raw/raw_wallet_br.csv'

# Caminho para o diretório de saída
output_dir = 'C:/Neoway/Neoway_Capital_Market_Analytics_Engineering_DEV_FullStack_Python_v1.0.0/src/backend/etl/1_extract/BOT_CNPJ/data/OUTPUT'
output_csv_path = os.path.join(output_dir, 'Output_Busca_CNPJ_v5.csv')

# Certificar-se de que o diretório de saída existe
os.makedirs(output_dir, exist_ok=True)

# Lê o arquivo CSV de entrada e extrai a coluna 'research_cnpj'
df = pd.read_csv(input_csv_path)
nomes_empresas = df['research_cnpj'].dropna().unique()

# Lista para armazenar os resultados
resultados = []

# Contador de CNPJs encontrados e não encontrados
cnpjs_encontrados = 0
cnpjs_nao_encontrados = 0

try:
    # Cria e abre o arquivo de saída
    with open(output_csv_path, mode='w', encoding='utf-8', newline='') as output_file:
        csv_writer = csv.writer(output_file)
        csv_writer.writerow(["Empresa", "CNPJ", "Status"])  # Escreve cabeçalho no arquivo de saída
        
        total_empresas = len(nomes_empresas)
        
        for i, nome_empresa in enumerate(nomes_empresas, start=1):
            if nome_empresa:  # Verifica se o nome da empresa não está vazio
                cnpj, encontrado = buscar_cnpj_google(nome_empresa)
                if cnpj:
                    print(f'{i}/{total_empresas}: CNPJ encontrado para {nome_empresa}: {cnpj}')
                    resultados.append({"Empresa": nome_empresa, "CNPJ": cnpj, "Status": "Encontrado"})
                    cnpjs_encontrados += 1
                else:
                    print(f'{i}/{total_empresas}: Não foi possível encontrar o CNPJ para {nome_empresa}')
                    resultados.append({"Empresa": nome_empresa, "CNPJ": "", "Status": "Não Encontrado"})
                    cnpjs_nao_encontrados += 1
                
                # Escreve o resultado no arquivo de saída em tempo real
                csv_writer.writerow([nome_empresa, cnpj, "Encontrado" if encontrado else "Não Encontrado"])
    
    print(f'Arquivo de saída salvo em: {output_csv_path}')
    print(f'Total de CNPJs encontrados: {cnpjs_encontrados}/{total_empresas}')
    print(f'Total de CNPJs não encontrados: {cnpjs_nao_encontrados}/{total_empresas}')

except KeyboardInterrupt:
    print("\nPrograma interrompido pelo usuário. Processo até o momento será salvo.")
    print(f'Total de CNPJs encontrados: {cnpjs_encontrados}/{total_empresas}')
    print(f'Total de CNPJs não encontrados: {cnpjs_nao_encontrados}/{total_empresas}')

except Exception as e:
    print(f'Ocorreu um erro inesperado: {str(e)}')
    print(f'Total de CNPJs encontrados: {cnpjs_encontrados}/{total_empresas}')
    print(f'Total de CNPJs não encontrados: {cnpjs_nao_encontrados}/{total_empresas}')
