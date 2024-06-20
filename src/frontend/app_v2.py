import os
import datetime
import pandas as pd
import streamlit as st
from plotly import graph_objs as go
from google.cloud import bigquery
from google.oauth2 import service_account

# Função para obter o caminho absoluto do arquivo
def get_absolute_path(relative_path):
    base_path = os.path.dirname(os.path.abspath(__file__))
    return os.path.join(base_path, relative_path)

# Configuração da autenticação do GCP
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "credentials/credentials_private_key_gbq/GBQ.json"
credentials_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")

def get_bigquery_client():
    return bigquery.Client()

def query_bigquery(client, query):
    query_job = client.query(query)
    results = query_job.result().to_dataframe()
    return results

# Função para pegar dados das ações do BigQuery
def pegar_dados_acoes(client):
    query = """
    SELECT DISTINCT snome FROM `fluent-outpost-424800-h1.gold_dim_wallet_br`
    """
    return query_bigquery(client, query)

# Função para pegar valores históricos das ações do BigQuery
def pegar_valores_historicos(client, sigla_acao, start_date, end_date):
    query = f"""
    SELECT Data, Abertura, Máximo, Mínimo, Fechamento
    FROM `fluent-outpost-424800-h1.gold_fact_historical_stock_price_br`
    WHERE ticker = '{sigla_acao}'
    AND Data BETWEEN '{start_date}' AND '{end_date}'
    ORDER BY Data
    """
    return query_bigquery(client, query)

# Função para calcular os principais resultados do último dia
def calcular_principais_resultados(df_valores):
    ultimo_dia = df_valores.iloc[-1]
    resultados = {
        'Preço de Abertura': ultimo_dia['Abertura'],
        'Preço de Fechamento': ultimo_dia['Fechamento'],
        'Preço Máximo': ultimo_dia['Máximo'],
        'Preço Mínimo': ultimo_dia['Mínimo']
    }
    return resultados

# Função para criar os cards personalizados
def criar_card(title, value):
    card_style = 'border: 2px solid {}; border-radius: 5px; padding: 15px; margin-right: 10px; flex: 1; background-color: {}; color: #ffffff; margin-bottom: 10px;'
    value_style = 'font-size: 24px; margin-bottom: 5px;'
    title_style = 'font-size: 18px; color: #ffffff;'

    if title == 'Preço de Abertura':
        card_style = card_style.format('#cccccc', '#333333')
        value_style += 'color: #FFFFFF;'
    elif title == 'Preço de Fechamento':
        card_style = card_style.format('#00b0e6', '#333333')
        value_style += 'color: #00b0e6;'
    elif title == 'Preço Máximo':
        card_style = card_style.format('#008000', '#333333')
        value_style += 'color: green;'
    elif title == 'Preço Mínimo':
        card_style = card_style.format('#ff0000', '#333333')
        value_style += 'color: red;'

    card_content = f"""
        <div style="{card_style}">
            <div style="{value_style}">{value:.2f}</div>
            <div style="{title_style}">{title}</div>
        </div>
    """
    return card_content

st.title('Análise de Ações')

# Inicializar cliente BigQuery
client = get_bigquery_client()

# Sidebar com opções de ações e tipo de gráfico
st.sidebar.header('Escolha a ação')
df_acoes = pegar_dados_acoes(client)
acao = df_acoes['snome']
nome_acao_escolhida = st.sidebar.selectbox('Escolha uma ação:', ['Escolher Todos'] + list(acao))
tipo_grafico = st.sidebar.radio("Selecione o tipo de gráfico:", ('Candlestick', 'Linha'))

# Filtrar data de início e fim da pesquisa
start_date = st.sidebar.date_input("Data de início:", datetime.date(2020, 1, 1))
end_date = st.sidebar.date_input("Data de fim:", datetime.date.today())

# Seleção da ação
if nome_acao_escolhida != 'Escolher Todos':
    df_valores = pegar_valores_historicos(client, nome_acao_escolhida, start_date, end_date)

    # Calcular e exibir principais resultados do último dia em cards personalizados
    st.subheader('Principais resultados do último dia')
    resultados = calcular_principais_resultados(df_valores)

    # Criar contêiner flexível para os cards
    st.markdown('<div style="display: flex;">', unsafe_allow_html=True)
    for chave, valor in resultados.items():
        # Adicionar cada card ao contêiner flexível
        st.markdown(criar_card(chave, valor), unsafe_allow_html=True)
    st.markdown('</div>', unsafe_allow_html=True)

    # Criar o gráfico
    st.subheader(f'Gráfico de preços da ação {nome_acao_escolhida}')
    if tipo_grafico == 'Candlestick':
        fig = go.Figure(data=[go.Candlestick(x=df_valores['Data'],
                                             open=df_valores['Abertura'],
                                             high=df_valores['Máximo'],
                                             low=df_valores['Mínimo'],
                                             close=df_valores['Fechamento'])])
    else:
        fig = go.Figure()
        fig.add_trace(go.Scatter(x=df_valores['Data'],
                                 y=df_valores['Fechamento'],
                                 name='Preço de Fechamento',
                                 line_color='blue'))
    st.plotly_chart(fig)

    # Tabela de valores
    st.subheader(f'Tabela de valores - {nome_acao_escolhida}')
    st.write(df_valores.tail(10))

else:
    st.write("Por favor, escolha uma ação na barra lateral.")
