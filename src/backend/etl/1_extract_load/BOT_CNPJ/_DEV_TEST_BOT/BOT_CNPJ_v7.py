import os
import csv
from googlesearch import search
from bs4 import BeautifulSoup
import requests
import re
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
import time

def buscar_cnpj_google(nome_empresa):
    try:
        query = f'{nome_empresa} CNPJ'
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'}
        search_results = search(query, num_results=1, lang='pt', headers=headers)
        first_result_link = next(search_results)
        response = requests.get(first_result_link)
        
        if response.status_code == 200:
            soup = BeautifulSoup(response.content, 'html.parser')
            cnpj_text = soup.get_text()
            cnpj_matches = re.findall(r'\b\d{2}\.\d{3}\.\d{3}\/\d{4}-\d{2}\b', cnpj_text)
            
            if cnpj_matches:
                return cnpj_matches[0], True
            else:
                print(f'CNPJ não encontrado para {nome_empresa}')
                return None, False
        elif response.status_code == 429:
            print(f'Erro 429: Muitas solicitações. Aguardando e tentando novamente...')
            time.sleep(10)
            return buscar_cnpj_google(nome_empresa)
        else:
            print(f'Erro ao buscar CNPJ para {nome_empresa}. Status code: {response.status_code}')
            return None, False
    except Exception as e:
        print(f'Erro ao buscar CNPJ para {nome_empresa}: {str(e)}')
        return None, False

def salvar_resultados_parciais(resultados, arquivo_saida):
    with open(arquivo_saida, mode='a', encoding='utf-8', newline='') as output_file:
        csv_writer = csv.writer(output_file)
        for resultado in resultados:
            csv_writer.writerow([resultado["Empresa"], resultado["CNPJ"], resultado["Status"]])

# Diretórios de entrada e saída
diretorio_input = 'C:/Neoway/Neoway_Capital_Market_Analytics_Engineering_DEV_FullStack_Python_v1.0.0/src/backend/etl/1_extract/BOT_CNPJ/data/INPUT'
diretorio_output = 'C:/Neoway/Neoway_Capital_Market_Analytics_Engineering_DEV_FullStack_Python_v1.0.0/src/backend/etl/1_extract/BOT_CNPJ/data/OUTPUT'

# Verifica se o diretório de saída existe, senão o cria
if not os.path.exists(diretorio_output):
    os.makedirs(diretorio_output)

# Define o nome do arquivo de entrada e o nome do novo arquivo de entrada
arquivo_input_original = os.path.join(diretorio_input, "research_cnpj.csv")
arquivo_input_novo = os.path.join(diretorio_input, f"Input_{datetime.now().strftime('%Y%m%d')}_research_cnpj.csv")

# Renomeia o arquivo de entrada original
os.rename(arquivo_input_original, arquivo_input_novo)

# Define o nome do arquivo de saída
data_atual = datetime.now().strftime("%Y%m%d")
arquivo_output = f"Output_{data_atual}_Busca_CNPJ.csv"
arquivo_output_completo = os.path.join(diretorio_output, arquivo_output)

# Lista para armazenar os resultados
resultados = []

# Contador de CNPJs encontrados e não encontrados
cnpjs_encontrados = 0
cnpjs_nao_encontrados = 0

try:
    with open(arquivo_input_novo, mode='r', encoding='utf-8', newline='') as csv_file:
        
        csv_reader = csv.reader(csv_file)
        total_empresas = sum(1 for _ in csv_reader)
        csv_file.seek(0)  # Retorna o cursor para o início do arquivo
        
        with ThreadPoolExecutor() as executor:
            for i, row in enumerate(csv_reader, start=1):
                if row:  # Verifica se a linha não está vazia
                    nome_empresa = row[0].replace(' - CNPJ', '')  # Remove o sufixo " - CNPJ"
                    future = executor.submit(buscar_cnpj_google, nome_empresa)
                    resultado = future.result()
                    if resultado[0]:
                        print(f'{i}/{total_empresas}: CNPJ encontrado para {nome_empresa}: {resultado[0]}')
                        resultados.append({"Empresa": nome_empresa, "CNPJ": resultado[0], "Status": "Encontrado"})
                        cnpjs_encontrados += 1
                    else:
                        print(f'{i}/{total_empresas}: Não foi possível encontrar o CNPJ para {nome_empresa}')
                        resultados.append({"Empresa": nome_empresa, "CNPJ": "", "Status": "Não Encontrado"})
                        cnpjs_nao_encontrados += 1
                
                if i % 10 == 0:  # Salva os resultados a cada 10 iterações
                    salvar_resultados_parciais(resultados, arquivo_output_completo)
                    resultados = []  # Limpa a lista de resultados

    # Salva os resultados finais
    salvar_resultados_parciais(resultados, arquivo_output_completo)

    print(f'Arquivo de saída salvo em: {arquivo_output_completo}')
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

