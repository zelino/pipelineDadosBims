
# Projeto de Integração de Dados com Python

Este projeto é uma ferramenta para integração de dados de uma API para um banco de dados SQL SERVER. O objetivo principal é buscar dados de vendas, produtos, e estoque de uma loja online e inseri-los de forma incremental em um banco de dados, facilitando análises e relatórios.

## Funcionalidades

- **Integração com API**: Coleta de dados de vendas, produtos, e estoque de uma API de ecommerce.
- **Paginação e Incrementalidade**: Implementa paginação para evitar sobrecarregar a API e busca de dados incrementais para otimizar o processo de atualização.
- **Armazenamento de Dados**: Insere dados em tabelas específicas no banco de dados SQL SERVER.
- **Configuração via `.env`**: Utiliza um arquivo `.env` para armazenar configurações sensíveis como credenciais de acesso ao banco de dados.


## Instruções de Uso

### 1. Pré-requisitos

- Python 3.8 ou superior
- ODBC SQL SERVER 18
- Acesso à API

### 2. Instalação

Clone o repositório para o seu ambiente local:

```bash
git clone https://github.com/zelino/pipelineDadosBims.git
cd pipelineDadosBims
```

Instale as dependências do projeto:

```bash
pip install -r requirements.txt
```

### 3. Configuração

Crie um arquivo `.env` na raiz do projeto e adicione as seguintes variáveis:

```
DRIVER=ODBC Driver 18 for SQL Server
HOST=seu_host
PORT=sua_porta
DATABASE=seu_banco_de_dados
USER_BD=seu_usuario
PASSWORD_BD=sua_senha
BASE_URL=https://api.seusite.com
```

### 4. Execução

Para executar o pipeline de dados, use o comando:

```bash
python main_dimensoes.py
python main_fatos.py
```

### 5. Contribuição

Contribuições são bem-vindas! Sinta-se à vontade para abrir issues ou enviar pull requests.

### 6. Licença

Este projeto está licenciado sob a [MIT License](LICENSE).