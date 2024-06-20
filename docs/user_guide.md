PROJECT - Capital Market Analytics

Para usar o app_v1.0.0 Siga o Tutorial Abaixo

OBS: Para o app_v1.0.0 funcionar ele precisa conectar na tabela armasenada na máquina local chamada raw_wallet_br e apartir da coluna ticker_br busca os dados na web de cotacões históricas de um periodo de 6 meses das acoes listadas na b3. 
Usuário pode aplicar filtros de setor, seguimento e alterar a data de inicio e final da análise. 

### 1 - Clonar o repositório na sua máquina local.

    git clone https://github.com/Mateus-Fleck/Neoway_Capital_Market_Analytics_Engineering_DEV_FullStack_Python_v1.0.0

    cd https://github.com/Mateus-Fleck/Neoway_Capital_Market_Analytics_Engineering_DEV_FullStack_Python_v1.0.0

### 2 - Configurar o ambiente virtual
Crie e ative um ambiente virtual (recomendado usar Python 3. ou superior).

    python -m venv .venv

    .venv/Scripts/activate  # Para Windows

    source .venv/bin/activate  # Para Linux/MacOS

### 3 - Instalar as dependências
Instale as dependências do projeto.

    pip install -r docs/requirements.txt


### 4 - Abrir o terminal de comando e inserir o codigo abaixo para start no Frontend local

    streamlit run src/frontend/app_v1.py

