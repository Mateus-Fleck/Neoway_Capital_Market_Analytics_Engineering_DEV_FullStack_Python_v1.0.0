import streamlit as st
import yfinance as yf
import pandas as pd
from plotly import graph_objs as go
from datetime import datetime, timedelta

# Carregar dados das ações
def pegar_dados_acoes():
    path = './src/backend/data/3_gold/gold_dim_wallet_br.csv'  # Caminho para o novo arquivo CSV
    return pd.read_csv(path)

# Função para baixar dados da ação online
def pegar_valores_online(symbol, start_date, end_date):
    try:
        df = yf.download(symbol, start=start_date, end=end_date)
        df.reset_index(inplace=True)
        return df
    except Exception as e:
        st.error(f"Erro ao baixar dados da ação: {e}")
        return pd.DataFrame()  # Retorna um DataFrame vazio em caso de erro

# Função para calcular os principais resultados do último dia
def calcular_principais_resultados(df_valores):
    if df_valores.empty or len(df_valores) < 1:
        return {'Preço de Abertura': 0, 'Preço de Fechamento': 0, 'Preço Máximo': 0, 'Preço Mínimo': 0}
    
    ultimo_dia = df_valores.iloc[-1]
    resultados = {
        'Abertura': ultimo_dia['Open'],
        'Fechamento': ultimo_dia['Close'],
        'Máxima': ultimo_dia['High'],
        'Mínima': ultimo_dia['Low']
    }
    return resultados

# Função para criar os cards personalizados
def criar_card(title, value, color):
    card_content = f"""
        <div style="border: 2px solid {color}; border-radius: 5px; padding: 10px; background-color: #333333; color: #ffffff; height: 120px; display: flex; flex-direction: column; justify-content: space-between;">
            <div style="font-size: 18px; color: #ffffff;">{title}</div>
            <div style="font-size: 24px; color: {color};">{value:.2f}</div>
        </div>
    """
    return card_content

def main():
    st.title('Análise de Ações')

    # Sidebar com opções de ações e tipo de gráfico
    st.sidebar.header('Menu de Filtros')
    df = pegar_dados_acoes()
    
    # Filtros adicionais
    setores = st.sidebar.multiselect('Escolha o Setor:', df['setor'].unique(), default=None)
    if setores:
        df = df[df['setor'].isin(setores)]
    segmentos = st.sidebar.multiselect('Escolha o Segmento:', df['industria'].unique(), default=None)
    if segmentos:
        df = df[df['industria'].isin(segmentos)]

    # Seleção da ação com base nos filtros aplicados
    acao = df['snome']
    nome_acao_escolhida = st.sidebar.selectbox('Escolha uma Ação:', ['Tickers'] + list(acao))

    # Definir a data de fim como a data atual
    max_end_date = datetime.today().date()

    # Definir a data de início como 6 meses atrás por padrão, mas permitir que o usuário edite
    default_start_date = datetime.today() - timedelta(days=6*30)
    start_date = st.sidebar.date_input("Data de Início da Análise:", default_start_date, format="DD/MM/YYYY")
    end_date = st.sidebar.date_input("Data de Final da Análise:", max_end_date, max_value=max_end_date, format="DD/MM/YYYY")

    # Aplicar filtros ao DataFrame
    if nome_acao_escolhida != 'Tickers':
        df_acao = df[df['snome'] == nome_acao_escolhida]
        
        if not df_acao.empty:
            acao_escolhida = df_acao.iloc[0]['ticker_br']
            
            # Baixar dados da ação
            df_valores = pegar_valores_online(acao_escolhida, start_date, end_date)

            if not df_valores.empty:
                # Calcular e exibir principais resultados do último dia em cards personalizados
                st.subheader('Principais Resultados do Último Dia')
                resultados = calcular_principais_resultados(df_valores)
                
                # Criar colunas para os cards
                col1, col2, col3, col4 = st.columns(4)
                cores = ['#cccccc', '#00b0e6', '#008000', '#ff0000']
                chaves = list(resultados.keys())
                
                with col1:
                    st.markdown(criar_card(chaves[0], resultados[chaves[0]], cores[0]), unsafe_allow_html=True)
                with col2:
                    st.markdown(criar_card(chaves[1], resultados[chaves[1]], cores[1]), unsafe_allow_html=True)
                with col3:
                    st.markdown(criar_card(chaves[2], resultados[chaves[2]], cores[2]), unsafe_allow_html=True)
                with col4:
                    st.markdown(criar_card(chaves[3], resultados[chaves[3]], cores[3]), unsafe_allow_html=True)

                # Adiciona um espaçamento entre os cards e o gráfico
                st.markdown("<div style='margin-top: 30px;'></div>", unsafe_allow_html=True)

                # Criar o gráfico
                st.subheader(f'Gráfico de Preços da Ação {acao_escolhida}')
                tipo_grafico = st.sidebar.radio("Selecione o Tipo de Gráfico:", ('Candlestick', 'Linha'))
                if tipo_grafico == 'Candlestick':
                    fig = go.Figure(data=[go.Candlestick(x=df_valores['Date'],
                                                         open=df_valores['Open'],
                                                         high=df_valores['High'],
                                                         low=df_valores['Low'],
                                                         close=df_valores['Close'])])
                else:
                    fig = go.Figure()
                    fig.add_trace(go.Scatter(x=df_valores['Date'], 
                                             y=df_valores['Close'], 
                                             name='Preço de Fechamento',
                                             line_color='blue'))
                
                # Ajustar o layout do gráfico
                fig.update_layout(
                    margin=dict(t=10, b=10),
                    height=400  # Definindo uma altura fixa para o gráfico
                )
                
                st.plotly_chart(fig)

                # Tabela de Valores
                st.subheader(f'Tabela de Valores - {acao_escolhida}')
                if not df_valores.empty:
                    df_valores_traduzido = df_valores.rename(columns={"Date": "Data", "Open": "Abertura", "High": "Alta", "Low": "Baixa", "Close": "Fechamento", "Volume": "Volume"})
                    st.write(df_valores_traduzido.drop(columns=['Adj Close']).tail(10))
                else:
                    st.write("Não há dados disponíveis para exibir.")
            else:
                st.error("Não foi possível obter dados para a ação selecionada. Por favor, verifique se o ticker está correto e tente novamente.")
        else:
            st.error("A ação selecionada não está disponível no dataset. Por favor, escolha outra ação.")
    else:
        st.write("Por favor, escolha uma ação na barra lateral.")

if __name__ == "__main__":
    main()
