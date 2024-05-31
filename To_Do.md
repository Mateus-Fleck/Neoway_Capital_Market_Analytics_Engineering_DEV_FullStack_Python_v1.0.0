# Challenge Neoway Capital Market Analytics Engineering DEV FullStack Python - v1.0.0

## 1 - BACKEND

1.1. **EXTRACT**
- [X] Pegar os dados das ações listadas na bolsa de valores (cnpj, nome, segmento, setor) (pode usar Web Scraping, APIs ou Bibliotecas disponíveis);
    TASKS
    - [X] Using lib python investpy for extract data the all stocks in Brazil and create table wallet_br


1.2. **EXTRACT** 
- [X] Pegar o histórico de cotações dos últimos 6 meses;
    - [X] Using lib python yfinance and list ticker_br in colunm table wallet_br for research and create table historical_stock_price  

1.3. **EXTRACT**
- [X] Persistir estes dados em um banco de dados na camada raw (bronze) (preferência BigQuery);
    TASKS
    - [X] - Create new accout services
    - [X] - Create new project



1.4. **TRANSFORM**
- [X] Realizar a limpeza dos dados e possíveis tratamentos (camada silver) (aqui pode ser usado o DBT, pandas, ou DuckDB);

    TASKS

    - [X] Renomear todos nomes de colunas traduzindo de inglês para portugues.
    - [X] Renomear os nomes das tabelas:
        - [X] Table wallet_br -> dim_wallet_br
        - [X] Table historical_stock_price_br -> fact_historical_stock_price_br
    


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