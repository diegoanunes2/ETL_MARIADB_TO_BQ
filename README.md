# 🦭ETL MARIADB TO BIGQUERY 
Este projeto realiza a extração de dados de um banco de dados MariaDB, ajusta os tipos de dados das colunas conforme especificado, e envia os dados tratados para o Google BigQuery.

## 📑Requisitos
-Python 3.x <br>
-Biblioteca pandas para manipulação de dados<br>
-Conexão com banco de dados MariaDB<br>
-Configuração de autenticação para o BigQuery<br>
## 📝Estrutura de Arquivos
## config.ini<br>
Arquivo de configuração com as credenciais de acesso ao banco de dados e informações do BigQuery.
## query_list.py<br>
Contém a query SQL a ser executada no banco de dados.
## functions.py<br>
Funções auxiliares para leitura de configurações, execução de queries e tratamento de dados.
## column_types.py<br>
Especificação dos tipos de dados para as colunas do DataFrame.
## ⚙️Configuração
Crie um arquivo config.ini no formato abaixo:
````sh
[credentials_maria_db]
user_db = seu_usuario
password_db = sua_senha
host_db = localhost
port_db = 3306
db = nome_do_banco

[credentials_google]
destino_bq = nome_do_projeto.nome_do_dataset.nome_da_tabela
cert = caminho_para_o_certificado.json
````
## 💻Execução

### Leitura de Configurações:<br>
O programa lê as configurações do arquivo config.ini para obter as credenciais do MariaDB e do BigQuery.<br>

### 🔌Conexão e Extração:<br>
O programa conecta ao MariaDB utilizando as credenciais fornecidas e executa a query definida no arquivo query_list.py.

### 🪛Ajuste de Tipos de Dados<br>
Os tipos de dados das colunas são ajustados conforme as seguintes categorias:<br>

String: Colunas convertidas para tipo string.<br>
Timestamp (YYYY/MM/DD): Colunas convertidas para timestamp com ano primeiro.<br>
Timestamp (DD/MM/YYYY): Colunas convertidas para timestamp com dia primeiro.<br>
Float: Colunas convertidas para tipo float.<br>
Integer: Colunas convertidas para tipo inteiro.<br>
Boolean: Colunas convertidas para tipo booleano.<br>
Esses tipos de dados devem ser definidos no arquivo column_types.py.<br>

### 🎲Envio para o BigQuery:<br>
Os dados tratados são enviados para a tabela especificada no BigQuery utilizando as credenciais fornecidas.

### ❌Tratamento de Erros<br>
Caso ocorra algum erro durante a execução, como falha na conexão ao banco de dados, ajuste de tipos de dados ou envio ao BigQuery, o programa exibirá uma mensagem de erro e será encerrado.<br>

### Exemplo de Uso
Execute o script principal (main.py) do projeto para iniciar o processo de extração e envio:
