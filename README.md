# Challenge Neoway Capital Market Analytics Engineering DEV FullStack Python - v1.0.0

# Challenge 
## 1 - BACKEND

1.1. **EXTRACT**
- [X] Pegar os dados das ações listadas na bolsa de valores (cnpj, nome, segmento, setor) (pode usar Web Scraping, APIs ou Bibliotecas disponíveis);

1.2. **EXTRACT** 
- [X] Pegar o histórico de cotações dos últimos 6 meses;

1.3. **EXTRACT**
- [X] Persistir estes dados em um banco de dados na camada raw (bronze) (preferência BigQuery);

1.4. **TRANSFORM**
- [X] Realizar a limpeza dos dados e possíveis tratamentos (camada silver) (aqui pode ser usado o DBT, pandas, ou DuckDB);

1.5. **LOAD**
- [X] Salvar o dado modelado no conceito de fatos e dimensões (BigQuery camada Gold);

1.6. **API**
- [X] Criar um endpoint simples que retorne os dados da entidade tratada na camada Transform (Pode usar a lib FasApi do Python para criar);

## 2 - FRONTEND (Streamlit ou Dash)

2.1. **PRESENTATION** 
- [X] Usando os dados das ações, apresentar informações gráficas (Gráfico em candlestick, pode usar a lib Plotly para isso)

2.2. **PRESENTATION**
- [X] Criar tabela com os dados extraídos;

2.3. **FILTERs**
- [X] Criar filtros de ticker, data, segmento e setor;

# Neoway - Uma empresa B3

# SOLUTION

VERSION app_v1.0.0

![Print Solution](https://github.com/Mateus-Fleck/Neoway_Capital_Market_Analytics_Engineering_DEV_FullStack_Python_v1.0.0/blob/main/src/frontend/assets/imgs/Print_Solution_app_v1/Print_Solution_v3.jpg)
